use super::{parse_ini, unquote, BackupType, CompressType};

/// A single backup set entry from backup.info [backup:current] section.
#[derive(Debug, Clone)]
pub struct BackupSet {
    pub label: String,
    pub backup_type: BackupType,
    pub timestamp_start: i64,
    pub timestamp_stop: i64,
    pub prior: Option<String>,
    pub reference: Vec<String>,
    pub lsn_start: String,
    pub lsn_stop: String,
    pub db_id: u32,
    pub compress_type: CompressType,
    pub archive_start: String,
    pub archive_stop: String,
    pub info_repo_size: u64,
    pub info_size: u64,
}

/// Parsed contents of a pgBackRest backup.info file.
#[derive(Debug)]
pub struct BackupInfo {
    pub backups: Vec<BackupSet>,
    pub db_version: String,
    pub db_system_id: u64,
    pub current_db_id: u32,
}

impl BackupInfo {
    /// Parse a backup.info file from its text content.
    pub fn parse(content: &str) -> Result<Self, String> {
        let ini = parse_ini(content);

        let db_section = ini
            .get("db")
            .ok_or("missing [db] section in backup.info")?;
        let db_version = unquote(db_section.get("db-version").map(|s| s.as_str()).unwrap_or(""))
            .to_string();
        let db_system_id: u64 = db_section
            .get("db-system-id")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let current_db_id: u32 = db_section
            .get("db-id")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let mut backups = Vec::new();
        if let Some(current) = ini.get("backup:current") {
            for (label, json_str) in current {
                let v: serde_json::Value = serde_json::from_str(json_str)
                    .map_err(|e| format!("bad JSON for backup '{}': {}", label, e))?;

                let backup_type = BackupType::parse(
                    v.get("backup-type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("full"),
                )?;
                let timestamp_start = v
                    .get("backup-timestamp-start")
                    .and_then(|t| t.as_i64())
                    .unwrap_or(0);
                let timestamp_stop = v
                    .get("backup-timestamp-stop")
                    .and_then(|t| t.as_i64())
                    .unwrap_or(0);
                let prior = v
                    .get("backup-prior")
                    .and_then(|t| t.as_str())
                    .map(String::from);
                let reference = v
                    .get("backup-reference")
                    .and_then(|t| t.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let lsn_start = v
                    .get("backup-lsn-start")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let lsn_stop = v
                    .get("backup-lsn-stop")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let db_id = v
                    .get("db-id")
                    .and_then(|t| t.as_u64())
                    .unwrap_or(0) as u32;
                // pgBackRest 2.37 and earlier used "option-compress": true/false (gz default).
                // 2.38+ added "option-compress-type": "gz"/"lz4"/"zst"/"none".
                let compress_type = if let Some(ct) = v
                    .get("option-compress-type")
                    .and_then(|t| t.as_str())
                {
                    CompressType::parse(ct)
                } else if v.get("option-compress").and_then(|t| t.as_bool()).unwrap_or(false) {
                    CompressType::Gz
                } else {
                    CompressType::None
                };
                let archive_start = v
                    .get("backup-archive-start")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let archive_stop = v
                    .get("backup-archive-stop")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                let info_repo_size = v
                    .get("backup-info-repo-size")
                    .and_then(|t| t.as_u64())
                    .unwrap_or(0);
                let info_size = v
                    .get("backup-info-size")
                    .and_then(|t| t.as_u64())
                    .unwrap_or(0);

                backups.push(BackupSet {
                    label: label.clone(),
                    backup_type,
                    timestamp_start,
                    timestamp_stop,
                    prior,
                    reference,
                    lsn_start,
                    lsn_stop,
                    db_id,
                    compress_type,
                    archive_start,
                    archive_stop,
                    info_repo_size,
                    info_size,
                });
            }
        }

        // Sort by timestamp_stop ascending (oldest first)
        backups.sort_by_key(|b| b.timestamp_stop);

        Ok(BackupInfo {
            backups,
            db_version,
            db_system_id,
            current_db_id,
        })
    }

    /// Select the most recent backup whose `timestamp_stop <= target_time`.
    pub fn select_for_time(&self, target_time: i64) -> Option<&BackupSet> {
        self.backups
            .iter()
            .rev()
            .find(|b| b.timestamp_stop <= target_time)
    }

    /// Select the most recent backup (regardless of time).
    pub fn select_latest(&self) -> Option<&BackupSet> {
        self.backups.last()
    }

    /// Find a backup by its label.
    pub fn find_by_label(&self, label: &str) -> Option<&BackupSet> {
        self.backups.iter().find(|b| b.label == label)
    }

    /// Resolve the full backup chain for a given backup (incr → diff → full).
    /// Returns the chain from the oldest (full) to the newest (target), inclusive.
    pub fn resolve_chain<'a>(&'a self, backup: &'a BackupSet) -> Vec<&'a BackupSet> {
        let mut chain = vec![backup];
        let mut current = backup;

        while current.backup_type != BackupType::Full {
            match &current.prior {
                Some(prior_label) => match self.find_by_label(prior_label) {
                    Some(prior) => {
                        chain.push(prior);
                        current = prior;
                    }
                    None => break,
                },
                None => break,
            }
        }

        chain.reverse();
        chain
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_backup_info() -> &'static str {
        r#"[backrest]
backrest-checksum="abc123"
backrest-format=5
backrest-version="2.53.1"

[backup:current]
20260320-100000F={"backrest-format":5,"backrest-version":"2.53.1","backup-archive-start":"000000010000000000000003","backup-archive-stop":"000000010000000000000003","backup-info-repo-size":2621263,"backup-info-repo-size-delta":2621263,"backup-info-size":30966232,"backup-info-size-delta":30966232,"backup-lsn-start":"0/3000028","backup-lsn-stop":"0/3000100","backup-pgid":1,"backup-prior":null,"backup-reference":null,"backup-timestamp-start":1710921600,"backup-timestamp-stop":1710921602,"backup-type":"full","db-id":1,"option-compress-type":"lz4"}
20260322-100000F_20260322-120000D={"backrest-format":5,"backrest-version":"2.53.1","backup-archive-start":"000000010000000000000005","backup-archive-stop":"000000010000000000000005","backup-info-repo-size":100000,"backup-info-repo-size-delta":50000,"backup-info-size":30966232,"backup-info-size-delta":100000,"backup-lsn-start":"0/5000028","backup-lsn-stop":"0/5000100","backup-pgid":1,"backup-prior":"20260320-100000F","backup-reference":["20260320-100000F"],"backup-timestamp-start":1711094400,"backup-timestamp-stop":1711094402,"backup-type":"diff","db-id":1,"option-compress-type":"lz4"}
20260323-100000F_20260323-120000I={"backrest-format":5,"backrest-version":"2.53.1","backup-archive-start":"000000010000000000000007","backup-archive-stop":"000000010000000000000007","backup-info-repo-size":50000,"backup-info-repo-size-delta":20000,"backup-info-size":30966232,"backup-info-size-delta":50000,"backup-lsn-start":"0/7000028","backup-lsn-stop":"0/7000100","backup-pgid":1,"backup-prior":"20260322-100000F_20260322-120000D","backup-reference":["20260320-100000F","20260322-100000F_20260322-120000D"],"backup-timestamp-start":1711180800,"backup-timestamp-stop":1711180802,"backup-type":"incr","db-id":1,"option-compress-type":"lz4"}

[db]
db-catalog-version=202209071
db-control-version=1300
db-id=1
db-system-id=7345267395403563828
db-version="16"

[db:history]
1={"db-catalog-version":202209071,"db-control-version":1300,"db-system-id":7345267395403563828,"db-version":"16"}
"#
    }

    #[test]
    fn test_parse_backup_info() {
        let info = BackupInfo::parse(sample_backup_info()).unwrap();
        assert_eq!(info.db_version, "16");
        assert_eq!(info.db_system_id, 7345267395403563828);
        assert_eq!(info.current_db_id, 1);
        assert_eq!(info.backups.len(), 3);

        let full = &info.backups[0];
        assert_eq!(full.label, "20260320-100000F");
        assert_eq!(full.backup_type, BackupType::Full);
        assert_eq!(full.compress_type, CompressType::Lz4);
        assert!(full.prior.is_none());

        let diff = &info.backups[1];
        assert_eq!(diff.backup_type, BackupType::Diff);
        assert_eq!(diff.prior.as_deref(), Some("20260320-100000F"));

        let incr = &info.backups[2];
        assert_eq!(incr.backup_type, BackupType::Incr);
        assert_eq!(
            incr.prior.as_deref(),
            Some("20260322-100000F_20260322-120000D")
        );
    }

    #[test]
    fn test_select_for_time() {
        let info = BackupInfo::parse(sample_backup_info()).unwrap();

        // Before any backup
        assert!(info.select_for_time(1710921000).is_none());

        // After full, before diff
        let sel = info.select_for_time(1711000000).unwrap();
        assert_eq!(sel.label, "20260320-100000F");

        // After diff, before incr
        let sel = info.select_for_time(1711100000).unwrap();
        assert_eq!(sel.label, "20260322-100000F_20260322-120000D");

        // After all
        let sel = info.select_for_time(1711200000).unwrap();
        assert_eq!(
            sel.label,
            "20260323-100000F_20260323-120000I"
        );
    }

    #[test]
    fn test_resolve_chain_full() {
        let info = BackupInfo::parse(sample_backup_info()).unwrap();
        let full = info.find_by_label("20260320-100000F").unwrap();
        let chain = info.resolve_chain(full);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].label, "20260320-100000F");
    }

    #[test]
    fn test_resolve_chain_diff() {
        let info = BackupInfo::parse(sample_backup_info()).unwrap();
        let diff = info
            .find_by_label("20260322-100000F_20260322-120000D")
            .unwrap();
        let chain = info.resolve_chain(diff);
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].label, "20260320-100000F");
        assert_eq!(chain[1].label, "20260322-100000F_20260322-120000D");
    }

    #[test]
    fn test_resolve_chain_incr() {
        let info = BackupInfo::parse(sample_backup_info()).unwrap();
        let incr = info
            .find_by_label("20260323-100000F_20260323-120000I")
            .unwrap();
        let chain = info.resolve_chain(incr);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].label, "20260320-100000F");
        assert_eq!(chain[1].label, "20260322-100000F_20260322-120000D");
        assert_eq!(chain[2].label, "20260323-100000F_20260323-120000I");
    }
}
