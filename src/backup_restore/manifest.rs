use super::{parse_ini, unquote, BackupType, CompressType};

/// Extract the cipher subpass from a decrypted manifest's [cipher] section.
/// This subpass is used to decrypt actual backup data files.
pub fn extract_cipher_subpass(manifest_content: &str) -> Option<String> {
    let ini = parse_ini(manifest_content);
    ini.get("cipher")
        .and_then(|sec| sec.get("cipher-subpass"))
        .map(|s| unquote(s).to_string())
        .filter(|s| !s.is_empty())
}

/// A file entry from the manifest [target:file] section.
#[derive(Debug, Clone)]
pub struct ManifestFile {
    pub path: String,
    pub size: u64,
    pub size_repo: Option<u64>,
    pub checksum: Option<String>,
    pub timestamp: i64,
    pub reference: Option<String>,
    pub bundle_id: Option<u64>,
    pub bundle_offset: Option<u64>,
    /// Non-zero when pgBackRest 2.46+ block-level incremental is used.
    /// Files with this set require block reassembly (Phase 5 — not yet supported).
    pub block_incr_size: Option<u64>,
}

/// A database entry from the manifest [db] section.
#[derive(Debug, Clone)]
pub struct ManifestDb {
    pub name: String,
    pub oid: u32,
}

/// Parsed contents of a pgBackRest backup.manifest file.
#[derive(Debug)]
pub struct BackupManifest {
    pub backup_type: BackupType,
    pub timestamp_start: i64,
    pub timestamp_stop: i64,
    pub lsn_start: String,
    pub lsn_stop: String,
    pub compress_type: CompressType,
    pub bundle: bool,
    pub prior: Option<String>,
    pub files: Vec<ManifestFile>,
    pub databases: Vec<ManifestDb>,
}

impl BackupManifest {
    /// Parse a backup.manifest from its (decompressed) text content.
    pub fn parse(content: &str) -> Result<Self, String> {
        let ini = parse_ini(content);

        // [backup] section
        let backup_sec = ini
            .get("backup")
            .ok_or("missing [backup] section in manifest")?;

        let backup_type = BackupType::parse(unquote(
            backup_sec
                .get("backup-type")
                .map(|s| s.as_str())
                .unwrap_or("full"),
        ))?;
        let timestamp_start: i64 = backup_sec
            .get("backup-timestamp-start")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let timestamp_stop: i64 = backup_sec
            .get("backup-timestamp-stop")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let lsn_start = unquote(
            backup_sec
                .get("backup-lsn-start")
                .map(|s| s.as_str())
                .unwrap_or(""),
        )
        .to_string();
        let lsn_stop = unquote(
            backup_sec
                .get("backup-lsn-stop")
                .map(|s| s.as_str())
                .unwrap_or(""),
        )
        .to_string();
        let bundle = backup_sec
            .get("backup-bundle")
            .map(|s| s == "true" || s == "\"true\"")
            .unwrap_or(false);
        let prior = backup_sec
            .get("backup-prior")
            .map(|s| unquote(s).to_string())
            .filter(|s| !s.is_empty() && s != "null");

        // [backup:option] section — compress type
        let compress_type = ini
            .get("backup:option")
            .and_then(|sec| sec.get("option-compress-type"))
            .map(|s| CompressType::parse(unquote(s)))
            .unwrap_or(CompressType::None);

        // [db] section — database name → OID mapping
        let mut databases = Vec::new();
        if let Some(db_sec) = ini.get("db") {
            for (name, json_str) in db_sec {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str) {
                    let oid = v.get("db-id").and_then(|t| t.as_u64()).unwrap_or(0) as u32;
                    if oid > 0 {
                        databases.push(ManifestDb {
                            name: name.clone(),
                            oid,
                        });
                    }
                }
            }
        }

        // [target:file] section — file entries
        let mut files = Vec::new();
        if let Some(file_sec) = ini.get("target:file") {
            for (path, json_str) in file_sec {
                let v: serde_json::Value = serde_json::from_str(json_str)
                    .map_err(|e| format!("bad JSON for file '{}': {}", path, e))?;

                let size = v.get("size").and_then(|t| t.as_u64()).unwrap_or(0);
                let size_repo = v.get("repo-size").and_then(|t| t.as_u64());
                let checksum = v
                    .get("checksum")
                    .and_then(|t| t.as_str())
                    .map(String::from);
                let timestamp = v.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0);
                let reference = v
                    .get("reference")
                    .and_then(|t| t.as_str())
                    .map(String::from);
                // Bundle fields: pgBackRest uses short keys "bi" and "bof"
                let bundle_id = v.get("bi").and_then(|t| t.as_u64());
                let bundle_offset = v.get("bof").and_then(|t| t.as_u64());
                let block_incr_size = v.get("bis").and_then(|t| t.as_u64());

                files.push(ManifestFile {
                    path: path.clone(),
                    size,
                    size_repo,
                    checksum,
                    timestamp,
                    reference,
                    bundle_id,
                    bundle_offset,
                    block_incr_size,
                });
            }
        }

        // Sort files by path for deterministic output
        files.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(BackupManifest {
            backup_type,
            timestamp_start,
            timestamp_stop,
            lsn_start,
            lsn_stop,
            compress_type,
            bundle,
            prior,
            files,
            databases,
        })
    }

    /// Find the database OID by name.
    pub fn db_oid(&self, db_name: &str) -> Option<u32> {
        self.databases.iter().find(|d| d.name == db_name).map(|d| d.oid)
    }

    /// Find all files belonging to a specific table (by relfilenode) in a database (by OID).
    /// Returns the main heap file, segment files (.1, .2, ...), fsm, vm,
    /// and optionally TOAST files if toast_relfilenode is provided.
    pub fn find_table_files(
        &self,
        db_oid: u32,
        relfilenode: u64,
        toast_relfilenode: Option<u64>,
    ) -> Vec<&ManifestFile> {
        let base_prefix = format!("pg_data/base/{}/{}", db_oid, relfilenode);
        let toast_prefix = toast_relfilenode
            .filter(|&t| t > 0)
            .map(|t| format!("pg_data/base/{}/{}", db_oid, t));

        self.files
            .iter()
            .filter(|f| {
                is_relfilenode_file(&f.path, &base_prefix)
                    || toast_prefix
                        .as_ref()
                        .map_or(false, |tp| is_relfilenode_file(&f.path, tp))
            })
            .collect()
    }

    /// Find global catalog files needed for a temp PG instance.
    ///
    /// Includes:
    /// - `pg_data/global/`   — shared catalogs and pg_control
    /// - `pg_data/pg_xact/`  — transaction commit log (CLOG)
    /// - `pg_data/pg_multixact/` — multi-transaction status
    pub fn find_global_files(&self) -> Vec<&ManifestFile> {
        self.files
            .iter()
            .filter(|f| {
                f.path.starts_with("pg_data/global/")
                    || f.path.starts_with("pg_data/pg_xact/")
                    || f.path.starts_with("pg_data/pg_multixact/")
            })
            .collect()
    }

    /// Find all database files needed for a temp PG instance.
    ///
    /// Returns ALL files under `base/{db_oid}/` — system catalogs, sequences,
    /// extension tables, and the target user tables. This is the simplest correct
    /// approach: the entire database directory in the backup repo is typically
    /// only a few MB compressed, so full extraction is cheap and avoids subtle
    /// missing-file errors (sequences, toast tables, extension objects, etc.).
    pub fn find_db_catalog_files(&self, db_oid: u32) -> Vec<&ManifestFile> {
        let db_prefix = format!("pg_data/base/{}/", db_oid);
        self.files
            .iter()
            .filter(|f| f.path.starts_with(&db_prefix))
            .collect()
    }

    /// Check if any of the provided files use block-level incremental (unsupported).
    pub fn has_block_incremental(files: &[&ManifestFile]) -> bool {
        files.iter().any(|f| f.block_incr_size.unwrap_or(0) > 0)
    }
}

/// Check if a file path matches a relfilenode base prefix.
/// Matches: `{prefix}`, `{prefix}.1`, `{prefix}_fsm`, `{prefix}_vm`, etc.
fn is_relfilenode_file(path: &str, prefix: &str) -> bool {
    if path == prefix {
        return true;
    }
    if let Some(rest) = path.strip_prefix(prefix) {
        // Segment files: .1, .2, .3, ...
        if let Some(seg) = rest.strip_prefix('.') {
            return seg.chars().all(|c| c.is_ascii_digit()) && !seg.is_empty();
        }
        // FSM and VM: _fsm, _vm
        if rest == "_fsm" || rest == "_vm" {
            return true;
        }
    }
    false
}

/// Extract the relfilenode number from a filename like "24576", "24576.1", "24576_fsm".
fn parse_relfilenode_from_filename(filename: &str) -> Option<u64> {
    let num_part = if let Some(dot_pos) = filename.find('.') {
        &filename[..dot_pos]
    } else if let Some(us_pos) = filename.find('_') {
        &filename[..us_pos]
    } else {
        filename
    };
    num_part.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest_full_plain() -> &'static str {
        r#"[backrest]
backrest-checksum="test"
backrest-format=5
backrest-version="2.53.1"

[backup]
backup-archive-start="000000010000000000000003"
backup-archive-stop="000000010000000000000003"
backup-lsn-start="0/3000028"
backup-lsn-stop="0/3000100"
backup-timestamp-start=1710921600
backup-timestamp-stop=1710921602
backup-type="full"

[backup:db]
db-catalog-version=202209071
db-control-version=1300
db-id=1
db-system-id=7345267395403563828
db-version="16"

[backup:option]
option-compress-type="none"

[backup:target]
pg_data={"path":"/var/lib/postgresql/16/main","type":"path"}

[db]
postgres={"db-id":5,"db-last-system-id":12345}
mydb={"db-id":16384,"db-last-system-id":12345}

[target:file]
pg_data/PG_VERSION={"checksum":"d8b928b2","size":3,"timestamp":1710921600}
pg_data/global/pg_control={"checksum":"abc123","size":8192,"timestamp":1710921600}
pg_data/global/pg_filenode.map={"checksum":"def456","size":512,"timestamp":1710921600}
pg_data/base/16384/PG_VERSION={"checksum":"d8b928b2","size":3,"timestamp":1710921600}
pg_data/base/16384/pg_filenode.map={"checksum":"f1f2f3","size":512,"timestamp":1710921600}
pg_data/base/16384/1={"checksum":"cat001","size":8192,"timestamp":1710921600}
pg_data/base/16384/1259={"checksum":"cat1259","size":65536,"timestamp":1710921600}
pg_data/base/16384/24576={"checksum":"tbl001","size":8388608,"timestamp":1710921600}
pg_data/base/16384/24576.1={"checksum":"tbl002","size":1073741824,"timestamp":1710921600}
pg_data/base/16384/24576_fsm={"checksum":"fsm001","size":32768,"timestamp":1710921600}
pg_data/base/16384/24576_vm={"checksum":"vm001","size":8192,"timestamp":1710921600}
pg_data/base/16384/24579={"checksum":"toast001","size":8192,"timestamp":1710921600}

[target:file:default]
group="postgres"
master=true
mode="0600"
user="postgres"

[target:path]
pg_data={}
pg_data/base={}
pg_data/base/16384={}
pg_data/global={}
"#
    }

    fn sample_manifest_incr_bundle() -> &'static str {
        r#"[backrest]
backrest-checksum="test2"
backrest-format=5
backrest-version="2.53.1"

[backup]
backup-archive-start="000000010000000000000007"
backup-archive-stop="000000010000000000000007"
backup-bundle=true
backup-lsn-start="0/7000028"
backup-lsn-stop="0/7000100"
backup-prior="20260322-100000F_20260322-120000D"
backup-timestamp-start=1711180800
backup-timestamp-stop=1711180802
backup-type="incr"

[backup:option]
option-compress-type="lz4"

[db]
mydb={"db-id":16384,"db-last-system-id":12345}

[target:file]
pg_data/PG_VERSION={"bi":1,"bof":0,"checksum":"d8b928b2","size":3,"repo-size":3,"timestamp":1711180800}
pg_data/global/pg_control={"checksum":"abc124","size":8192,"timestamp":1711180800}
pg_data/global/pg_filenode.map={"bi":1,"bof":3,"checksum":"def457","size":512,"repo-size":512,"timestamp":1711180800}
pg_data/base/16384/pg_filenode.map={"bi":1,"bof":515,"checksum":"f1f2f4","size":512,"repo-size":512,"timestamp":1711180800}
pg_data/base/16384/1259={"bi":2,"bof":0,"checksum":"cat1260","size":65536,"repo-size":4096,"timestamp":1711180800}
pg_data/base/16384/24576={"checksum":"tbl003","reference":"20260322-100000F_20260322-120000D","size":8388608,"timestamp":1711180800}
pg_data/base/16384/24576.1={"checksum":"tbl002","reference":"20260320-100000F","size":1073741824,"timestamp":1710921600}
pg_data/base/16384/24576_fsm={"bi":2,"bof":4096,"checksum":"fsm002","size":32768,"repo-size":2048,"timestamp":1711180800}
pg_data/base/16384/24576_vm={"bi":2,"bof":6144,"checksum":"vm002","size":8192,"repo-size":1024,"timestamp":1711180800}
pg_data/base/16384/24579={"checksum":"toast001","reference":"20260320-100000F","size":8192,"timestamp":1710921600}
"#
    }

    #[test]
    fn test_parse_manifest_full_plain() {
        let m = BackupManifest::parse(sample_manifest_full_plain()).unwrap();
        assert_eq!(m.backup_type, BackupType::Full);
        assert_eq!(m.compress_type, CompressType::None);
        assert!(!m.bundle);
        assert!(m.prior.is_none());
        assert_eq!(m.lsn_stop, "0/3000100");

        assert_eq!(m.databases.len(), 2);
        assert_eq!(m.db_oid("mydb"), Some(16384));
        assert_eq!(m.db_oid("postgres"), Some(5));
    }

    #[test]
    fn test_find_table_files_plain() {
        let m = BackupManifest::parse(sample_manifest_full_plain()).unwrap();
        let files = m.find_table_files(16384, 24576, Some(24579));
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"pg_data/base/16384/24576"));
        assert!(paths.contains(&"pg_data/base/16384/24576.1"));
        assert!(paths.contains(&"pg_data/base/16384/24576_fsm"));
        assert!(paths.contains(&"pg_data/base/16384/24576_vm"));
        assert!(paths.contains(&"pg_data/base/16384/24579"));
        assert_eq!(files.len(), 5);
    }

    #[test]
    fn test_parse_manifest_incr_bundle() {
        let m = BackupManifest::parse(sample_manifest_incr_bundle()).unwrap();
        assert_eq!(m.backup_type, BackupType::Incr);
        assert_eq!(m.compress_type, CompressType::Lz4);
        assert!(m.bundle);
        assert_eq!(
            m.prior.as_deref(),
            Some("20260322-100000F_20260322-120000D")
        );

        // Bundle entries should have bi and bof
        let pg_version = m.files.iter().find(|f| f.path == "pg_data/PG_VERSION").unwrap();
        assert_eq!(pg_version.bundle_id, Some(1));
        assert_eq!(pg_version.bundle_offset, Some(0));
        assert_eq!(pg_version.size_repo, Some(3));

        // Non-bundled entries with references
        let main_heap = m
            .files
            .iter()
            .find(|f| f.path == "pg_data/base/16384/24576")
            .unwrap();
        assert!(main_heap.bundle_id.is_none());
        assert_eq!(
            main_heap.reference.as_deref(),
            Some("20260322-100000F_20260322-120000D")
        );
    }

    #[test]
    fn test_find_table_files_with_references() {
        let m = BackupManifest::parse(sample_manifest_incr_bundle()).unwrap();
        let files = m.find_table_files(16384, 24576, Some(24579));

        // Should find all table files even when they reference other backups
        let main_heap = files.iter().find(|f| f.path == "pg_data/base/16384/24576").unwrap();
        assert_eq!(
            main_heap.reference.as_deref(),
            Some("20260322-100000F_20260322-120000D")
        );

        let seg1 = files.iter().find(|f| f.path == "pg_data/base/16384/24576.1").unwrap();
        assert_eq!(
            seg1.reference.as_deref(),
            Some("20260320-100000F")
        );

        // Bundled FSM/VM
        let fsm = files.iter().find(|f| f.path == "pg_data/base/16384/24576_fsm").unwrap();
        assert_eq!(fsm.bundle_id, Some(2));
        assert_eq!(fsm.bundle_offset, Some(4096));
    }

    #[test]
    fn test_find_global_files() {
        let m = BackupManifest::parse(sample_manifest_full_plain()).unwrap();
        let globals = m.find_global_files();
        assert_eq!(globals.len(), 2);
        let paths: Vec<&str> = globals.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"pg_data/global/pg_control"));
        assert!(paths.contains(&"pg_data/global/pg_filenode.map"));
    }

    #[test]
    fn test_find_db_catalog_files() {
        let m = BackupManifest::parse(sample_manifest_full_plain()).unwrap();
        let catalog = m.find_db_catalog_files(16384);
        let paths: Vec<&str> = catalog.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"pg_data/base/16384/PG_VERSION"));
        assert!(paths.contains(&"pg_data/base/16384/pg_filenode.map"));
        // relfilenode 1 and 1259 are system catalogs (< 16384)
        assert!(paths.contains(&"pg_data/base/16384/1"));
        assert!(paths.contains(&"pg_data/base/16384/1259"));
        // relfilenode 24576 is a user table (>= 16384), should NOT be included
        assert!(!paths.contains(&"pg_data/base/16384/24576"));
    }

    #[test]
    fn test_block_incremental_detection() {
        // Sample with block incremental field
        let content = r#"[backup]
backup-lsn-start="0/1"
backup-lsn-stop="0/2"
backup-timestamp-start=1
backup-timestamp-stop=2
backup-type="full"

[db]
mydb={"db-id":16384,"db-last-system-id":1}

[target:file]
pg_data/base/16384/24576={"checksum":"abc","size":8192,"bis":8192,"bim":128,"timestamp":1}
"#;
        let m = BackupManifest::parse(content).unwrap();
        let files = m.find_table_files(16384, 24576, None);
        assert!(BackupManifest::has_block_incremental(&files));
    }
}
