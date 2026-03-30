pub mod backup_info;
pub mod cipher;
pub mod manifest;
pub mod extractor;
pub mod temp_instance;

use pgrx::prelude::*;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupType {
    Full,
    Diff,
    Incr,
}

impl BackupType {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "full" => Ok(Self::Full),
            "diff" => Ok(Self::Diff),
            "incr" => Ok(Self::Incr),
            other => Err(format!("unknown backup type: {}", other)),
        }
    }
}

impl std::fmt::Display for BackupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Diff => write!(f, "diff"),
            Self::Incr => write!(f, "incr"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressType {
    None,
    Lz4,
    Zst,
    Gz,
}

impl CompressType {
    pub fn parse(s: &str) -> Self {
        match s {
            "lz4" => Self::Lz4,
            "zst" => Self::Zst,
            "gz" => Self::Gz,
            "none" | "" => Self::None,
            _ => Self::None,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::Lz4 => ".lz4",
            Self::Zst => ".zst",
            Self::Gz => ".gz",
        }
    }
}

// pgBackRest uses an INI-like format: [section] headers with key=value entries.
// Values in some sections (backup:current, target:file, db) are JSON strings.
pub type IniSections = HashMap<String, HashMap<String, String>>;

pub fn parse_ini(content: &str) -> IniSections {
    let mut sections = HashMap::new();
    let mut current_section = String::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            current_section = line[1..line.len() - 1].to_string();
            sections
                .entry(current_section.clone())
                .or_insert_with(HashMap::new);
        } else if let Some(eq_pos) = line.find('=') {
            let key = line[..eq_pos].to_string();
            let value = line[eq_pos + 1..].to_string();
            sections
                .entry(current_section.clone())
                .or_insert_with(HashMap::new)
                .insert(key, value);
        }
    }

    sections
}

/// Strip surrounding double-quotes from an INI string value.
pub fn unquote(s: &str) -> &str {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

pub fn decompress(data: &[u8], compress_type: CompressType) -> Result<Vec<u8>, String> {
    match compress_type {
        CompressType::None => Ok(data.to_vec()),
        CompressType::Lz4 => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("LZ4 decompress error: {}", e))?;
            Ok(out)
        }
        CompressType::Zst => {
            zstd::decode_all(data).map_err(|e| format!("zstd decompress error: {}", e))
        }
        CompressType::Gz => {
            // pgBackRest uses two gz sub-formats:
            //   - Standalone files: standard gzip (magic 1F 8B) → GzDecoder
            //   - Bundle-internal files: zlib deflate (magic 78 xx) → ZlibDecoder
            // Detect by magic bytes and dispatch accordingly.
            if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
                let mut decoder = flate2::read::GzDecoder::new(data);
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .map_err(|e| format!("gzip decompress error: {}", e))?;
                Ok(out)
            } else {
                // zlib stream (0x78 0x9C / 0x78 0x01 / 0x78 0xDA)
                let mut decoder = flate2::read::ZlibDecoder::new(data);
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .map_err(|e| format!("zlib decompress error: {}", e))?;
                Ok(out)
            }
        }
    }
}

/// Read a pgBackRest info file (backup.info / archive.info).
/// Info files are never compressed; they may be encrypted.
/// Falls back to .copy if the primary file is missing.
pub fn read_info_file(path: &Path) -> Result<String, String> {
    read_info_file_bytes(path).and_then(|b| {
        String::from_utf8(b).map_err(|e| format!("info file is not valid UTF-8: {}", e))
    })
}

fn read_info_file_bytes(path: &Path) -> Result<Vec<u8>, String> {
    let primary = std::fs::read(path);
    match primary {
        Ok(b) => return Ok(b),
        Err(_) => {}
    }
    let copy_name = format!(
        "{}.copy",
        path.file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("backup.info")
    );
    let copy = path.with_file_name(copy_name);
    std::fs::read(&copy).map_err(|e| format!("cannot read info file {}: {}", path.display(), e))
}

/// Read a pgBackRest info file, decrypting it if a passphrase is provided.
pub fn read_info_file_decrypted(path: &Path, cipher_pass: Option<&str>) -> Result<String, String> {
    let raw = read_info_file_bytes(path)?;
    let decrypted = match cipher_pass {
        Some(pass) if cipher::is_encrypted(&raw) => cipher::decrypt_file(&raw, pass)?,
        _ => raw,
    };
    String::from_utf8(decrypted).map_err(|e| format!("info file is not valid UTF-8: {}", e))
}

/// Read, decrypt (if needed), and decompress a pgBackRest manifest file.
/// The manifest is stored with a compression extension matching the backup's compress type.
/// When encrypted, it has the same extension as a compressed file (pgBackRest: compress → encrypt).
pub fn read_manifest_file(
    backup_dir: &Path,
    compress_type: CompressType,
) -> Result<String, String> {
    read_manifest_file_with_cipher(backup_dir, compress_type, None)
}

pub fn read_manifest_file_with_cipher(
    backup_dir: &Path,
    compress_type: CompressType,
    cipher_pass: Option<&str>,
) -> Result<String, String> {
    let ext = compress_type.extension();
    let manifest_name = format!("backup.manifest{}", ext);
    let manifest_path = backup_dir.join(&manifest_name);

    // Try the expected path first; fall back to trying all known extensions
    let (data, actual_compress) = if manifest_path.exists() {
        let d =
            std::fs::read(&manifest_path).map_err(|e| format!("read {}: {}", manifest_path.display(), e))?;
        (d, compress_type)
    } else {
        let candidates = [
            ("backup.manifest", CompressType::None),
            ("backup.manifest.lz4", CompressType::Lz4),
            ("backup.manifest.zst", CompressType::Zst),
            ("backup.manifest.gz", CompressType::Gz),
        ];
        let mut found = None;
        for (name, ct) in &candidates {
            let p = backup_dir.join(name);
            if p.exists() {
                let d = std::fs::read(&p).map_err(|e| format!("read {}: {}", p.display(), e))?;
                found = Some((d, *ct));
                break;
            }
        }
        found.ok_or_else(|| {
            format!(
                "backup.manifest not found in {}",
                backup_dir.display()
            )
        })?
    };

    // pgBackRest encryption order: compress → encrypt.
    // So decrypt first, then decompress.
    let decrypted = match cipher_pass {
        Some(pass) if cipher::is_encrypted(&data) => cipher::decrypt_file(&data, pass)?,
        _ => data,
    };

    let decompressed = decompress(&decrypted, actual_compress)?;
    String::from_utf8(decompressed).map_err(|e| format!("manifest is not valid UTF-8: {}", e))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find a free TCP port starting from `start`, binding on 127.0.0.1.
fn find_free_port(start: u16) -> u16 {
    // Spread starting port by PID to avoid parallel restore collisions.
    // Each backend has a unique PID; taking (pid % 1000) * 2 gives a spread
    // of 0–1998, keeping us well within the ephemeral range.
    let pid_offset = (std::process::id() % 1000) as u16 * 2;
    let begin = start.saturating_add(pid_offset);
    for port in begin..=65000 {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    // Fallback: scan from start
    for port in start..begin {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    start
}

/// RAII guard that removes a directory on drop.
struct TempDirGuard(PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        if self.0.exists() {
            if let Err(e) = std::fs::remove_dir_all(&self.0) {
                pgrx::warning!(
                    "pg_recyclebin: failed to clean up temp dir {}: {}",
                    self.0.display(), e
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public orchestration: single-table restore from pgBackRest backup
// ---------------------------------------------------------------------------

/// Perform a full single-table restore from a pgBackRest backup.
///
/// Requires:
/// - `flashback.pgbackrest_repo_path` — path to the pgBackRest repo root
/// - `flashback.pgbackrest_stanza`    — stanza name
/// - `flashback.pgbackrest_pg_bin_dir`— directory with pg_ctl, pg_dump, psql
/// - `flashback.pgbackrest_temp_dir`  — temp dir for the ephemeral instance
/// - `flashback.pgbackrest_bin_path`  — path to pgbackrest binary (for restore_command)
///
/// The `metadata` JSON must contain `relfilenode`, `db_oid`, `wal_lsn` fields
/// (captured by `ddl_capture.rs` at DROP time).
///
/// Returns `Ok(())` on success or `Err(message)` on failure.
pub fn restore_table_from_backup(
    schema: &str,
    table: &str,
    metadata_json: &str,
    skip_wal_replay: bool,
) -> Result<(), String> {
    let meta: serde_json::Value = serde_json::from_str(metadata_json)
        .map_err(|e| format!("metadata parse error: {}", e))?;

    let relfilenode = meta
        .get("relfilenode")
        .and_then(|v| v.as_u64())
        .ok_or("metadata missing 'relfilenode'")?;
    let db_oid = meta
        .get("db_oid")
        .and_then(|v| v.as_u64())
        .ok_or("metadata missing 'db_oid'")? as u32;
    let wal_lsn = meta
        .get("wal_lsn")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let toast_relfilenode = meta
        .get("toast_relfilenode")
        .and_then(|v| v.as_u64())
        .filter(|&v| v > 0);

    // Read GUCs
    let repo_path_str = crate::guc::get_pgbackrest_repo_path()
        .ok_or("flashback.pgbackrest_repo_path is not set")?;
    let stanza = crate::guc::get_pgbackrest_stanza()
        .ok_or("flashback.pgbackrest_stanza is not set")?;
    let temp_base_dir = crate::guc::get_pgbackrest_temp_dir();
    let pgbackrest_bin = crate::guc::get_pgbackrest_bin_path();
    let pg_bin_dir_str = crate::guc::get_pgbackrest_pg_bin_dir()
        .ok_or("flashback.pgbackrest_pg_bin_dir is not set")?;
    let cipher_pass = crate::guc::get_pgbackrest_cipher_pass();

    // Early validation: catch empty required GUCs before doing any I/O
    if repo_path_str.trim().is_empty() {
        return Err("flashback.pgbackrest_repo_path is empty — set it to the pgBackRest repo root".to_string());
    }
    if stanza.trim().is_empty() {
        return Err("flashback.pgbackrest_stanza is empty — set it to your pgBackRest stanza name".to_string());
    }
    if pg_bin_dir_str.trim().is_empty() {
        return Err("flashback.pgbackrest_pg_bin_dir is empty — set it to the PostgreSQL bin directory (e.g. /usr/pgsql-17/bin)".to_string());
    }
    if pgbackrest_bin.trim().is_empty() {
        return Err("flashback.pgbackrest_bin_path is empty — set it to the pgbackrest binary path (needed for restore_command)".to_string());
    }

    let repo_path = PathBuf::from(&repo_path_str);
    let pg_bin_dir = PathBuf::from(&pg_bin_dir_str);

    // 1. Parse backup.info — select the most recent backup
    let info_path = repo_path.join("backup").join(&stanza).join("backup.info");
    let info_content = read_info_file_decrypted(&info_path, cipher_pass.as_deref())?;
    let backup_info = backup_info::BackupInfo::parse(&info_content)?;
    let selected = backup_info
        .select_latest()
        .ok_or("no backups found in backup.info")?
        .clone();

    pgrx::log!(
        "pg_recyclebin: restoring {}.{} from backup '{}' ({})",
        schema, table, selected.label, selected.backup_type
    );

    // 2. Parse backup.manifest for the selected backup
    let backup_dir = repo_path.join("backup").join(&stanza).join(&selected.label);
    let manifest_content = read_manifest_file_with_cipher(
        &backup_dir,
        selected.compress_type,
        cipher_pass.as_deref(),
    )?;

    // 3. Extract cipher subpass from manifest (for encrypted repos)
    let subpass = manifest::extract_cipher_subpass(&manifest_content);
    let enc = extractor::EncryptionContext::new(cipher_pass, subpass);

    let bmanifest = manifest::BackupManifest::parse(&manifest_content)?;

    // 4. Identify target files
    let table_files = bmanifest.find_table_files(db_oid, relfilenode, toast_relfilenode);
    if table_files.is_empty() {
        return Err(format!(
            "no files found for relfilenode {} in db_oid {} in backup '{}'",
            relfilenode, db_oid, selected.label
        ));
    }

    // 5. Create temp directory. Append PID to the path so concurrent restore
    //    calls (same backup label, different tables) don't collide.
    let pid = std::process::id();
    let temp_dir = PathBuf::from(format!(
        "{}/{}_{}",
        temp_base_dir, &selected.label, pid
    ));
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)
            .map_err(|e| format!("cleanup old temp dir: {}", e))?;
    }
    std::fs::create_dir_all(&temp_dir)
        .map_err(|e| format!("create temp dir {}: {}", temp_dir.display(), e))?;
    // Guard ensures temp_dir is removed on all return paths (success or error).
    let _temp_guard = TempDirGuard(temp_dir.clone());

    // 5b. Block-level incremental: check table_files AND all support files
    // (global catalog, db catalog) together.  A global file with bim > 0 would
    // fail the manual extractor just as much as a table file would.
    let global_files_preview = bmanifest.find_global_files();
    let catalog_files_preview = bmanifest.find_db_catalog_files(db_oid);
    let all_files_for_check: Vec<&manifest::ManifestFile> = table_files
        .iter()
        .copied()
        .chain(global_files_preview.iter().copied())
        .chain(catalog_files_preview.iter().copied())
        .collect();

    if manifest::BackupManifest::has_block_incremental(&all_files_for_check) {
        pgrx::log!(
            "pg_recyclebin: block-level incremental detected for '{}.{}' in backup '{}' — \
             delegating to pgbackrest restore subprocess",
            schema, table, selected.label
        );

        // Resolve production connection params now (needed inside the helper)
        let prod_port: u16 = Spi::get_one::<String>("SELECT current_setting('port')")
            .unwrap_or(None)
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(5432);
        let prod_db = Spi::get_one::<String>("SELECT current_database()::text")
            .unwrap_or(None)
            .unwrap_or_else(|| "postgres".to_string());
        let temp_db = bmanifest
            .databases
            .iter()
            .find(|d| d.oid == db_oid)
            .map(|d| d.name.clone())
            .unwrap_or_else(|| "postgres".to_string());

        return restore_via_pgbackrest(
            schema,
            table,
            &temp_db,
            &selected.label,
            &stanza,
            &pgbackrest_bin,
            &pg_bin_dir,
            &temp_dir,
            skip_wal_replay,
            &wal_lsn,
            "127.0.0.1",
            prod_port,
            &prod_db,
        );
    }

    // 6. Reuse the file lists already fetched for the block-incr check above.
    let global_files = global_files_preview;
    let catalog_files = catalog_files_preview;

    // 7. Resolve locations (handle reference chains for incr/diff)
    let all_files: Vec<&manifest::ManifestFile> = table_files
        .into_iter()
        .chain(global_files.into_iter())
        .chain(catalog_files.into_iter())
        .collect();
    let locations = extractor::resolve_file_locations(&all_files, &selected.label);

    // 8. Extract files into temp directory
    let extract_dest = temp_dir.join("pgdata");
    std::fs::create_dir_all(&extract_dest)
        .map_err(|e| format!("create extract dir: {}", e))?;

    extractor::extract_files(
        &repo_path,
        &stanza,
        &locations,
        selected.compress_type,
        &extract_dest,
        &enc,
    )?;

    // 9. Create temp PG instance using extracted files
    let temp_pg_dir = temp_dir.join("pginstance");
    let temp_port = find_free_port(15432);

    // initdb to get a proper cluster structure
    let initdb = pg_bin_dir.join("initdb");
    let output = std::process::Command::new(&initdb)
        .args(["--no-locale", "-E", "UTF8", "-D"])
        .arg(&temp_pg_dir)
        .output()
        .map_err(|e| format!("initdb: {}", e))?;
    if !output.status.success() {
        return Err(format!(
            "initdb failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    // Overwrite extracted directories into the initdb'd instance.
    // Copy order matters: global first (includes pg_control), then xact dirs, then base.
    for dir_name in &["global", "pg_xact", "pg_multixact"] {
        let src = extract_dest.join(dir_name);
        if src.exists() {
            let dst = temp_pg_dir.join(dir_name);
            std::fs::create_dir_all(&dst)
                .map_err(|e| format!("create {dir_name} dir: {}", e))?;
            copy_dir_overwrite(&src, &dst)?;
        }
    }
    let base_src = extract_dest.join("base").join(db_oid.to_string());
    let base_dst = temp_pg_dir.join("base").join(db_oid.to_string());
    std::fs::create_dir_all(&base_dst)
        .map_err(|e| format!("create base dir: {}", e))?;
    if base_src.exists() {
        copy_dir_overwrite(&base_src, &base_dst)?;
    }

    // 10. Configure and start the temp instance
    let mut instance = temp_instance::TempInstance::new(&temp_pg_dir, temp_port, &pg_bin_dir);

    if skip_wal_replay || wal_lsn.is_empty() {
        instance.configure_no_recovery()?;
    } else {
        instance.configure_recovery(&wal_lsn, &pgbackrest_bin, &stanza)?;
    }

    instance.start()?;

    // 11. Wait for recovery to complete (max 5 minutes)
    instance.wait_for_recovery(Duration::from_secs(300))?;

    // 12. Determine production connection params via SPI.
    // Use current_setting('port') — inet_server_port() returns NULL for Unix socket connections.
    let prod_port: u16 = Spi::get_one::<String>("SELECT current_setting('port')")
        .unwrap_or(None)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5432);
    let prod_host = "127.0.0.1";
    // Explicit ::text cast because current_database() returns type 'name' not 'text',
    // and pgrx Spi::get_one::<String> may fail to coerce 'name' → String.
    let prod_db = Spi::get_one::<String>("SELECT current_database()::text")
        .unwrap_or(None)
        .unwrap_or_else(|| "postgres".to_string());

    // Need the db name as pgBackRest sees it (match by db_oid in manifest)
    let temp_db = bmanifest
        .databases
        .iter()
        .find(|d| d.oid == db_oid)
        .map(|d| d.name.clone())
        .unwrap_or_else(|| "postgres".to_string());

    // 13. pg_dump from temp instance → pg_restore into production
    instance.dump_and_restore(
        &temp_db,
        schema,
        table,
        prod_host,
        prod_port,
        &prod_db,
    )?;

    // 14. Cleanup
    instance.stop()?;
    std::fs::remove_dir_all(&temp_dir)
        .map_err(|e| format!("cleanup temp dir: {}", e))?;

    pgrx::log!(
        "pg_recyclebin: successfully restored {}.{} from backup '{}'",
        schema, table, selected.label
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Block-level incremental fallback: use pgbackrest restore subprocess
// ---------------------------------------------------------------------------

/// Restore a single table from a block-level incremental backup by running
/// `pgbackrest restore` to reconstruct the full data directory, then using
/// the normal temp-instance + pg_dump/pg_restore path.
///
/// This avoids implementing the complex varint-128 block map format that
/// pgBackRest 2.46+ uses. pgBackRest handles all assembly; we just start
/// a temp instance on the result.
#[allow(clippy::too_many_arguments)]
fn restore_via_pgbackrest(
    schema: &str,
    table: &str,
    temp_db: &str,
    backup_label: &str,
    stanza: &str,
    pgbackrest_bin: &str,
    pg_bin_dir: &Path,
    temp_dir: &Path,
    skip_wal_replay: bool,
    wal_lsn: &str,
    prod_host: &str,
    prod_port: u16,
    prod_db: &str,
) -> Result<(), String> {
    let temp_pg_dir = temp_dir.join("pginstance");
    std::fs::create_dir_all(&temp_pg_dir)
        .map_err(|e| format!("create pginstance dir: {}", e))?;

    // Run pgbackrest restore into the temp directory.
    // --db-include: restore ONLY the target database's files — critical for large
    //   clusters: avoids copying every database in the cluster to the temp dir.
    //   pgBackRest still restores global/ (pg_control, pg_xact, etc.) which are
    //   required for the temp instance to start.
    // --set: pick the specific backup set (avoids accidentally using latest).
    // pgbackrest writes recovery.signal and sets up postgresql.auto.conf for recovery.
    // Pass --repo=N when the user has configured a non-default repo index (e.g. repo2 for
    // encrypted repos in a multi-repo pgBackRest setup).
    let repo_idx = crate::guc::get_pgbackrest_repo();
    let repo_arg = format!("--repo={}", repo_idx);

    let restore_log = temp_dir.join("pgbackrest_restore.log");
    let mut restore_args: Vec<String> = vec![
        format!("--stanza={}", stanza),
        format!("--pg1-path={}", temp_pg_dir.display()),
        format!("--set={}", backup_label),
        format!("--db-include={}", temp_db),
        "--log-level-console=warn".to_string(),
    ];
    if repo_idx != 1 {
        restore_args.push(repo_arg);
    }
    restore_args.push("restore".to_string());

    let restore_status = std::process::Command::new(pgbackrest_bin)
        .args(&restore_args)
        .stdout(std::fs::File::create(&restore_log).ok().map_or(
            std::process::Stdio::null(),
            std::process::Stdio::from,
        ))
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|e| format!("pgbackrest restore: {}", e))?;

    if !restore_status.status.success() {
        let stderr = String::from_utf8_lossy(&restore_status.stderr);
        return Err(format!(
            "pgbackrest restore failed for backup '{}': {}",
            backup_label, stderr.trim()
        ));
    }

    pgrx::log!(
        "pg_recyclebin: pgbackrest restore completed for backup '{}', starting temp instance",
        backup_label
    );

    let temp_port = find_free_port(15432);
    let mut instance = temp_instance::TempInstance::new(&temp_pg_dir, temp_port, pg_bin_dir);

    if skip_wal_replay || wal_lsn.is_empty() {
        // pgbackrest restore already wrote recovery.signal — pg_resetwal -f
        // removes the WAL dependency so we can start without the archive.
        instance.configure_no_recovery()?;
    } else {
        // Overwrite the recovery config written by pgbackrest with our LSN target.
        // configure_recovery handles the case where recovery.signal already exists.
        instance.configure_recovery(wal_lsn, pgbackrest_bin, stanza)?;
    }

    instance.start()?;
    instance.wait_for_recovery(Duration::from_secs(300))?;

    instance.dump_and_restore(temp_db, schema, table, prod_host, prod_port, prod_db)?;
    instance.stop()?;

    pgrx::log!(
        "pg_recyclebin: successfully restored {}.{} from block-incremental backup '{}'",
        schema, table, backup_label
    );
    Ok(())
}

/// Recursively copy directory contents, overwriting existing files.
fn copy_dir_overwrite(src: &Path, dst: &Path) -> Result<(), String> {
    for entry in std::fs::read_dir(src)
        .map_err(|e| format!("readdir {}: {}", src.display(), e))?
    {
        let entry = entry.map_err(|e| format!("readdir entry: {}", e))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            std::fs::create_dir_all(&dst_path)
                .map_err(|e| format!("mkdir {}: {}", dst_path.display(), e))?;
            copy_dir_overwrite(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)
                .map_err(|e| format!("copy {} → {}: {}", src_path.display(), dst_path.display(), e))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// pgrx SQL-exposed functions
// ---------------------------------------------------------------------------

/// Restore a single table from a pgBackRest backup into the production database.
///
/// ```sql
/// SELECT flashback_restore_from_backup('orders');
/// SELECT flashback_restore_from_backup('orders', target_schema => 'public');
/// SELECT flashback_restore_from_backup('orders', skip_wal_replay => true);
/// ```
#[pg_extern]
pub fn flashback_restore_from_backup(
    table_name: &str,
    target_schema: default!(Option<&str>, NULL),
    skip_wal_replay: default!(bool, false),
) -> bool {
    // Only superusers may invoke this — it executes external binaries and accesses the backup repo.
    let is_superuser = unsafe { pg_sys::superuser_arg(pg_sys::GetSessionUserId()) };
    if !is_superuser {
        pgrx::warning!(
            "pg_recyclebin: flashback_restore_from_backup() requires superuser privileges"
        );
        return false;
    }

    if table_name.contains('\'') || table_name.contains(';') {
        pgrx::warning!("pg_recyclebin: invalid table name: {}", table_name);
        return false;
    }

    // Look up metadata from the operations table (must have relfilenode captured at drop time)
    let schema_priority = match target_schema {
        Some(s) => format!("(schema_name = '{}') DESC, ", s.replace('\'', "''")),
        None => String::new(),
    };
    // Fetch ALL operation records for this table (oldest first).
    // We'll iterate them in the Rust layer and pick the one whose relfilenode
    // actually exists in the backup, matching by WAL LSN.
    let all_rows = Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT schema_name, COALESCE(metadata::text, '') \
                 FROM flashback.operations \
                 WHERE table_name = '{}' \
                 ORDER BY {}op_id ASC",
                table_name.replace('\'', "''"),
                schema_priority
            ),
            None,
            &[],
        )?;
        let mut found: Vec<(String, String)> = Vec::new();
        for row in rows {
            let schema = row.get::<String>(1)?.unwrap_or_default();
            let meta = row.get::<String>(2)?.unwrap_or_default();
            found.push((schema, meta));
        }
        Ok::<_, spi::Error>(found)
    });

    let candidates = match all_rows {
        Ok(v) if !v.is_empty() => v,
        _ => {
            pgrx::warning!(
                "pg_recyclebin: no operation record found for '{}'. \
                 Make sure the table was dropped after pg_recyclebin was installed.",
                table_name
            );
            return false;
        }
    };

    // Use the first record that has relfilenode metadata.
    // restore_table_from_backup will skip records whose relfilenode isn't in the backup.
    let (schema, _metadata_json) = {
        let with_relfnode: Vec<_> = candidates
            .iter()
            .filter(|(_, m)| {
                serde_json::from_str::<serde_json::Value>(m)
                    .ok()
                    .and_then(|v| v.get("relfilenode").cloned())
                    .is_some()
            })
            .collect();
        if with_relfnode.is_empty() {
            pgrx::warning!(
                "pg_recyclebin: metadata for '{}' does not contain relfilenode. \
                 Use flashback_backup_restore_hint('{}') for manual guidance.",
                table_name, table_name
            );
            return false;
        }
        // Clone the first candidate; the Rust restore logic will try each in order.
        with_relfnode[0].clone()
    };

    let all_candidates_json: Vec<String> = candidates
        .iter()
        .filter_map(|(_, m)| {
            serde_json::from_str::<serde_json::Value>(m)
                .ok()
                .and_then(|v| v.get("relfilenode").cloned())
                .map(|_| m.clone())
        })
        .collect();

    let restore_schema = target_schema.unwrap_or(&schema);

    // Try each candidate metadata record (oldest first) until one succeeds.
    // This handles cases where a table was dropped/recreated multiple times and
    // the backup contains an older relfilenode.
    let mut last_error = String::new();
    for candidate_meta in &all_candidates_json {
        match restore_table_from_backup(restore_schema, table_name, candidate_meta, skip_wal_replay) {
            Ok(()) => {
                if let Err(e) = Spi::run(&format!(
                    "UPDATE flashback.operations \
                     SET restored = true, restored_at = now() \
                     WHERE table_name = '{}' AND restored = false",
                    table_name.replace('\'', "''")
                )) {
                    pgrx::warning!(
                        "pg_recyclebin: restored '{}' but failed to update operations log: {}",
                        table_name, e
                    );
                }
                return true;
            }
            Err(e) => {
                // "no files found" means this relfilenode isn't in the backup — try next
                if e.contains("no files found") {
                    last_error = e;
                    continue;
                }
                // Other errors are real failures
                pgrx::warning!("pg_recyclebin: backup restore failed for '{}': {}", table_name, e);
                return false;
            }
        }
    }

    pgrx::warning!(
        "pg_recyclebin: backup restore failed for '{}': no matching relfilenode found in backup. {}",
        table_name, last_error
    );
    false
}

/// Return a hint message explaining how to manually restore a table from a pgBackRest backup.
///
/// ```sql
/// SELECT flashback_backup_restore_hint('orders');
/// ```
#[pg_extern]
pub fn flashback_backup_restore_hint(table_name: &str) -> String {
    if table_name.contains('\'') || table_name.contains(';') {
        return "ERROR: invalid table name".into();
    }

    // Get latest metadata
    let row = Spi::connect(|client| {
        let rows = client.select(
            &format!(
                "SELECT schema_name, COALESCE(metadata::text, '{{}}') \
                 FROM flashback.operations \
                 WHERE table_name = '{}' \
                 ORDER BY timestamp DESC, op_id DESC LIMIT 1",
                table_name.replace('\'', "''")
            ),
            None,
            &[],
        )?;
        let mut found: Option<(String, String)> = None;
        for row in rows {
            let schema = row.get::<String>(1)?.unwrap_or_default();
            let meta = row.get::<String>(2)?.unwrap_or_default();
            found = Some((schema, meta));
        }
        Ok::<_, spi::Error>(found)
    });

    let (schema, metadata_json) = match row {
        Ok(Some((s, m))) => (s, m),
        _ => {
            return format!(
                "No operation record found for '{}'. \
                 Table must have been dropped while pg_recyclebin was installed.",
                table_name
            );
        }
    };

    let meta: serde_json::Value = serde_json::from_str(&metadata_json).unwrap_or_default();
    let relfilenode = meta.get("relfilenode").and_then(|v| v.as_u64());
    let db_oid = meta.get("db_oid").and_then(|v| v.as_u64());
    let wal_lsn = meta.get("wal_lsn").and_then(|v| v.as_str()).unwrap_or("unknown");
    let filepath = meta.get("filepath").and_then(|v| v.as_str()).unwrap_or("unknown");

    let repo_path = crate::guc::get_pgbackrest_repo_path()
        .unwrap_or_else(|| "/var/lib/pgbackrest".into());
    let stanza = crate::guc::get_pgbackrest_stanza()
        .unwrap_or_else(|| "<stanza>".into());

    if relfilenode.is_none() {
        return format!(
            "Table '{}.{}': no backup metadata found (captured before Phase 0).\n\
             To restore manually:\n  \
             1. Identify the pgBackRest backup containing this table.\n  \
             2. Run: pgbackrest --stanza={} --type=lsn --target=<lsn> --target-action=promote restore\n  \
             3. Use pg_dump to extract the table, then pg_restore to import it.",
            schema, table_name, stanza
        );
    }

    format!(
        "Table '{schema}.{table}' — backup metadata found:\n\
         \n  relfilenode:       {relfilenode}\n\
         \n  db_oid:            {db_oid}\n\
         \n  filepath:          {filepath}\n\
         \n  wal_lsn at drop:   {wal_lsn}\n\
         \nTo restore automatically:\n\
         \n  SELECT flashback_restore_from_backup('{table}');\n\
         \nRequired GUC settings:\n\
         \n  SET flashback.pgbackrest_repo_path = '{repo_path}';\n\
         \n  SET flashback.pgbackrest_stanza    = '{stanza}';\n\
         \n  SET flashback.pgbackrest_pg_bin_dir = '/usr/lib/postgresql/16/bin';\n\
         \nTo restore manually from pgBackRest:\n\
         \n  pgbackrest --stanza={stanza} --type=lsn \\\n\
         \n    --target='{wal_lsn}' --target-action=promote \\\n\
         \n    --db-include=<db_name> restore\n\
         \n  pg_dump -Fc -t {schema}.{table} <temp_db> | \\\n\
         \n    pg_restore --no-owner --no-privileges --disable-triggers \\\n\
         \n      -d <prod_db>",
        schema = schema,
        table = table_name,
        relfilenode = relfilenode.unwrap_or(0),
        db_oid = db_oid.unwrap_or(0),
        filepath = filepath,
        wal_lsn = wal_lsn,
        repo_path = repo_path,
        stanza = stanza,
    )
}
