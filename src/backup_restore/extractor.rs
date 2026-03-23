use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::cipher::{decrypt_file, is_encrypted};
use super::manifest::ManifestFile;
use super::{decompress, CompressType};

/// Describes where to find a file in the pgBackRest repository.
#[derive(Debug)]
pub struct FileLocation {
    pub manifest_entry: ManifestFile,
    /// The backup label directory that actually contains this file
    /// (may differ from the target backup when `reference` is set).
    pub backup_label: String,
}

/// Encryption context: optional cipher passphrase for the repo.
/// When set, all reads go through decrypt → then decompress.
/// pgBackRest encryption order: compress THEN encrypt (so decrypt first, then decompress).
#[derive(Default, Clone)]
pub struct EncryptionContext {
    /// Repo-level cipher pass (from flashback.pgbackrest_cipher_pass GUC)
    pub repo_cipher_pass: Option<String>,
    /// Per-backup cipher subpass (extracted from the decrypted manifest's [cipher] section)
    pub backup_cipher_subpass: Option<String>,
}

impl EncryptionContext {
    pub fn new(repo_cipher_pass: Option<String>, backup_cipher_subpass: Option<String>) -> Self {
        Self {
            repo_cipher_pass,
            backup_cipher_subpass,
        }
    }

    /// The effective passphrase for data files:
    /// use backup_cipher_subpass if available, otherwise fall back to repo_cipher_pass.
    fn data_passphrase(&self) -> Option<&str> {
        self.backup_cipher_subpass
            .as_deref()
            .or(self.repo_cipher_pass.as_deref())
    }
}

/// Extract a set of files from a pgBackRest repository into a destination directory.
///
/// Encryption order in pgBackRest: compress → encrypt.
/// Decryption order here:         decrypt → decompress.
///
/// For each file:
/// 1. If `reference` is set, the file lives in a different backup's directory.
/// 2. If `bundle_id` is set, extract from the bundle file using offset + size_repo.
/// 3. If not bundled, read the standalone file (with compression extension) from backup dir.
/// 4. Decrypt if encryption is configured and data has "Salted__" magic header.
/// 5. Decompress (lz4/zst/gz) — for bundle entries this happens on the extracted slice.
/// 6. Write to `dest_dir/{relative_path}` (stripping the `pg_data/` prefix).
pub fn extract_files(
    repo_path: &Path,
    stanza: &str,
    locations: &[FileLocation],
    compress_type: CompressType,
    dest_dir: &Path,
    enc: &EncryptionContext,
) -> Result<Vec<PathBuf>, String> {
    let mut extracted = Vec::new();
    let backup_base = repo_path.join("backup").join(stanza);

    for loc in locations {
        let entry = &loc.manifest_entry;

        if entry.block_incr_size.unwrap_or(0) > 0 {
            return Err(format!(
                "block-level incremental not supported for file '{}' (pgBackRest 2.46+ feature, Phase 5)",
                entry.path
            ));
        }

        let relative_path = entry
            .path
            .strip_prefix("pg_data/")
            .unwrap_or(&entry.path);
        let dest_file = dest_dir.join(relative_path);
        if let Some(parent) = dest_file.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
        }

        let final_data = if let (Some(bid), Some(bof)) = (entry.bundle_id, entry.bundle_offset) {
            // Bundled file: read slice from bundle, decrypt if needed, then decompress
            let raw = read_from_bundle(
                &backup_base.join(&loc.backup_label),
                bid,
                bof,
                entry.size_repo.unwrap_or(entry.size),
            )?;
            let decrypted = maybe_decrypt(&raw, enc)?;
            decompress(&decrypted, compress_type)?
        } else {
            // Non-bundled file: file on disk already has compress extension;
            // read_plain_file reads the raw bytes (still encrypted+compressed)
            let raw = read_raw_file(
                &backup_base.join(&loc.backup_label),
                &entry.path,
                compress_type,
                enc.data_passphrase().is_some(),
            )?;
            let decrypted = maybe_decrypt(&raw, enc)?;
            decompress(&decrypted, compress_type)?
        };

        std::fs::write(&dest_file, &final_data)
            .map_err(|e| format!("write {}: {}", dest_file.display(), e))?;
        extracted.push(dest_file);
    }

    Ok(extracted)
}

/// Decrypt data if a passphrase is available and the data has the "Salted__" header.
fn maybe_decrypt(data: &[u8], enc: &EncryptionContext) -> Result<Vec<u8>, String> {
    match enc.data_passphrase() {
        Some(pass) if is_encrypted(data) => decrypt_file(data, pass),
        Some(_) => {
            // Passphrase configured but data doesn't have the magic header — pass through.
            // This happens for unencrypted repos or files that pgBackRest didn't encrypt.
            Ok(data.to_vec())
        }
        None => {
            if is_encrypted(data) {
                return Err(
                    "data appears encrypted ('Salted__' header found) but \
                     flashback.pgbackrest_cipher_pass is not set"
                        .into(),
                );
            }
            Ok(data.to_vec())
        }
    }
}

/// Read raw (potentially encrypted+compressed) bytes from a non-bundled file.
/// When encrypted, the file on disk is the encrypted ciphertext (no extension beyond compress).
/// When not encrypted, the file has the compress extension.
fn read_raw_file(
    backup_dir: &Path,
    manifest_path: &str,
    compress_type: CompressType,
    encrypted: bool,
) -> Result<Vec<u8>, String> {
    let ext = compress_type.extension();

    // pgBackRest stores: plain → compress → encrypt
    // On disk: filename{compress_ext} (encrypted files have the same extension as compressed)
    let candidates: &[&str] = if encrypted {
        // With encryption: the encrypt step wraps the compressed blob,
        // file extension is the compress extension (or empty if no compression)
        &[manifest_path]
    } else if ext.is_empty() {
        &[manifest_path]
    } else {
        // Non-encrypted with compression: filename.lz4 / .zst / .gz
        &[manifest_path]
    };

    // Build the full path with compression extension
    let primary = if !ext.is_empty() {
        backup_dir.join(format!("{}{}", manifest_path, ext))
    } else {
        backup_dir.join(manifest_path)
    };

    if primary.exists() {
        return std::fs::read(&primary)
            .map_err(|e| format!("read {}: {}", primary.display(), e));
    }

    // Fallback: try without extension (uncompressed backup or extension mismatch)
    let plain = backup_dir.join(manifest_path);
    if plain.exists() {
        return std::fs::read(&plain)
            .map_err(|e| format!("read {}: {}", plain.display(), e));
    }

    // Try all known extensions as last resort
    for try_ext in &[".lz4", ".zst", ".gz", ""] {
        let p = if try_ext.is_empty() {
            backup_dir.join(manifest_path)
        } else {
            backup_dir.join(format!("{}{}", manifest_path, try_ext))
        };
        if p.exists() {
            return std::fs::read(&p)
                .map_err(|e| format!("read {}: {}", p.display(), e));
        }
    }

    let _ = candidates; // used above
    Err(format!(
        "file not found: {} in {}",
        manifest_path,
        backup_dir.display()
    ))
}

/// Read raw bytes from a bundle file at the given offset.
fn read_from_bundle(
    backup_dir: &Path,
    bundle_id: u64,
    offset: u64,
    size: u64,
) -> Result<Vec<u8>, String> {
    let bundle_path = backup_dir.join("bundle").join(bundle_id.to_string());
    let mut file = std::fs::File::open(&bundle_path)
        .map_err(|e| format!("open bundle {}: {}", bundle_path.display(), e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| format!("seek bundle {}: {}", bundle_path.display(), e))?;
    let mut buf = vec![0u8; size as usize];
    file.read_exact(&mut buf)
        .map_err(|e| format!("read bundle {}: {}", bundle_path.display(), e))?;
    Ok(buf)
}

/// Resolve file locations for a set of manifest entries against a backup chain.
/// For each file, determines which backup directory actually contains the data
/// by following the `reference` field.
pub fn resolve_file_locations(
    files: &[&ManifestFile],
    current_backup_label: &str,
) -> Vec<FileLocation> {
    files
        .iter()
        .map(|f| {
            let backup_label = f
                .reference
                .as_deref()
                .unwrap_or(current_backup_label)
                .to_string();
            FileLocation {
                manifest_entry: (*f).clone(),
                backup_label,
            }
        })
        .collect()
}
