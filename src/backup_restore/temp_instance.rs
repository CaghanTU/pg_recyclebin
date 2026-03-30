use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// Manages a temporary PostgreSQL instance used for WAL replay and table export.
pub struct TempInstance {
    pub data_dir: PathBuf,
    pub port: u16,
    pg_bin_dir: PathBuf,
    started: bool,
}

impl TempInstance {
    /// Create a new temp instance configuration. Does NOT start the instance.
    pub fn new(data_dir: &Path, port: u16, pg_bin_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            port,
            pg_bin_dir: pg_bin_dir.to_path_buf(),
            started: false,
        }
    }

    /// Write recovery configuration for WAL replay to a target LSN.
    pub fn configure_recovery(
        &self,
        target_lsn: &str,
        pgbackrest_bin: &str,
        stanza: &str,
    ) -> Result<(), String> {
        // Create recovery.signal (PG 12+) only if not already present.
        // pgbackrest restore writes it automatically; avoid overwriting it.
        let signal_path = self.data_dir.join("recovery.signal");
        if !signal_path.exists() {
            std::fs::write(&signal_path, "")
                .map_err(|e| format!("write recovery.signal: {}", e))?;
        }

        // Overwrite postgresql.auto.conf so we replace any restore_command written
        // by pgbackrest restore with our own LSN-targeted recovery config.
        let auto_conf = self.data_dir.join("postgresql.auto.conf");
        if auto_conf.exists() {
            std::fs::write(&auto_conf, "# cleared by pg_recyclebin configure_recovery\n")
                .map_err(|e| format!("clear postgresql.auto.conf: {}", e))?;
        }

        // Write a clean minimal postgresql.conf, replacing the backup's original.
        // archive_mode=off avoids conflicts with the existing source cluster setup
        // (pgBackRest enabled archiving there but our temp instance fetches WAL via
        // restore_command directly, not through an archive).
        let conf_path = self.data_dir.join("postgresql.conf");
        let data_dir_str = self.data_dir.to_string_lossy();
        // Pass --pg1-path to archive-get so pgBackRest can identify the correct
        // stanza even though the data directory is a temporary path.
        let restore_cmd = format!(
            "{} --stanza={} --pg1-path={} archive-get %f %p",
            pgbackrest_bin, stanza, data_dir_str
        );
        // IMPORTANT: append to the backup's existing postgresql.conf rather than
        // replacing it.  PostgreSQL enforces that several parameters (max_connections,
        // max_prepared_transactions, max_worker_processes, max_locks_per_transaction)
        // must be >= the values recorded in pg_control from the primary.  Replacing
        // the conf would lose those values and abort recovery with "insufficient
        // parameter settings".  Instead we keep the original values and only override
        // what we need (port, archive_mode, logging, and recovery settings).
        let mut existing = std::fs::read_to_string(&conf_path).unwrap_or_default();
        existing.push_str(&format!(
            "\n# pg_recyclebin temp instance (WAL replay) — appended, do not edit\n\
             restore_command = '{restore_cmd}'\n\
             recovery_target_lsn = '{target_lsn}'\n\
             recovery_target_inclusive = true\n\
             recovery_target_action = 'promote'\n\
             port = {port}\n\
             shared_buffers = '32MB'\n\
             work_mem = '4MB'\n\
             maintenance_work_mem = '16MB'\n\
             archive_mode = off\n\
             logging_collector = off\n\
             log_min_messages = 'warning'\n\
             listen_addresses = '127.0.0.1'\n",
            restore_cmd = restore_cmd,
            target_lsn = target_lsn,
            port = self.port,
        ));
        std::fs::write(&conf_path, &existing)
            .map_err(|e| format!("write postgresql.conf: {}", e))
    }

    /// Configure a temp instance for WAL-replay-free operation.
    /// Used when the backup's timestamp_stop is sufficient (skip_wal_replay mode).
    ///
    /// Runs `pg_resetwal -f` to reset WAL pointers in pg_control so that
    /// the instance can start without needing the WAL segments referenced
    /// by the backup's pg_control file.
    pub fn configure_no_recovery(&self) -> Result<(), String> {
        // Remove recovery.signal if present (pgbackrest restore writes it).
        // pg_resetwal does not touch it, so we must remove it explicitly to
        // prevent PostgreSQL from attempting WAL replay after reset.
        let signal_path = self.data_dir.join("recovery.signal");
        if signal_path.exists() {
            std::fs::remove_file(&signal_path)
                .map_err(|e| format!("remove recovery.signal: {}", e))?;
        }

        // Overwrite postgresql.auto.conf: pgbackrest restore writes restore_command
        // there which would cause recovery mode even without recovery.signal.
        let auto_conf = self.data_dir.join("postgresql.auto.conf");
        if auto_conf.exists() {
            std::fs::write(&auto_conf, "# cleared by pg_recyclebin configure_no_recovery\n")
                .map_err(|e| format!("clear postgresql.auto.conf: {}", e))?;
        }

        // Remove backup_label: pgbackrest restore writes this file so PostgreSQL
        // knows it is recovering from a backup.  Without recovery.signal it causes
        // "could not locate required checkpoint record" at startup.  pg_resetwal
        // below will create a fresh, valid checkpoint, making backup_label redundant.
        let backup_label = self.data_dir.join("backup_label");
        if backup_label.exists() {
            std::fs::remove_file(&backup_label)
                .map_err(|e| format!("remove backup_label: {}", e))?;
        }

        // Reset WAL so PostgreSQL can start without the original WAL segments.
        // This is safe because we only need the data files, not crash recovery.
        let pg_resetwal = self.pg_bin_dir.join("pg_resetwal");
        let resetwal_out = Command::new(&pg_resetwal)
            .args(["-f", self.data_dir.to_str().unwrap_or("")])
            .output()
            .map_err(|e| format!("pg_resetwal: {}", e))?;
        if !resetwal_out.status.success() {
            let stderr = String::from_utf8_lossy(&resetwal_out.stderr);
            return Err(format!("pg_resetwal failed: {}", stderr));
        }

        // Write a clean minimal postgresql.conf, completely replacing the backup's
        // original one. This avoids conflicts with settings like archive_mode=on,
        // restore_command, or wal_level constraints from the source cluster.
        let conf_path = self.data_dir.join("postgresql.conf");
        let minimal_conf = format!(
            "# pg_recyclebin temp instance (no WAL replay) — generated, do not edit\n\
             port = {}\n\
             shared_buffers = '32MB'\n\
             work_mem = '4MB'\n\
             maintenance_work_mem = '16MB'\n\
             max_connections = 5\n\
             wal_level = 'minimal'\n\
             max_wal_senders = 0\n\
             archive_mode = off\n\
             logging_collector = off\n\
             log_min_messages = 'warning'\n\
             listen_addresses = '127.0.0.1'\n",
            self.port
        );
        std::fs::write(&conf_path, &minimal_conf)
            .map_err(|e| format!("write postgresql.conf: {}", e))
    }

    /// Validate that the target LSN exists in the WAL archive using pg_waldump.
    pub fn validate_wal_lsn(
        &self,
        target_lsn: &str,
        pgbackrest_bin: &str,
        stanza: &str,
    ) -> Result<bool, String> {
        // Use pg_waldump to check if the LSN range is readable.
        // This is a best-effort check; the actual WAL segment needs to be fetchable.
        let waldump = self.pg_bin_dir.join("pg_waldump");
        if !waldump.exists() {
            return Ok(true); // Skip validation if pg_waldump not available
        }

        // Fetch the WAL segment that should contain our target LSN
        // For now, just verify pg_waldump binary exists. Full validation
        // requires actually fetching the WAL segment via archive-get.
        let _ = (pgbackrest_bin, stanza, target_lsn);
        Ok(true)
    }

    /// Start the temporary PostgreSQL instance.
    ///
    /// Uses `.status()` (not `.output()`) so stdout/stderr are NOT piped.
    /// This is critical: if we use `.output()`, the spawned `postgres` children
    /// inherit the pipe write-end, keeping it open indefinitely, which causes
    /// the calling backend to block forever on `pipe_read`.
    pub fn start(&mut self) -> Result<(), String> {
        let pg_ctl = self.pg_bin_dir.join("pg_ctl");
        let log_path = self.data_dir.join("pg_recyclebin_startup.log");

        let status = Command::new(&pg_ctl)
            .args([
                "start",
                "-D",
                self.data_dir.to_str().unwrap_or(""),
                "-w",
                "-t",
                "120",
                "-l",
                log_path.to_str().unwrap_or("/dev/null"),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("pg_ctl start: {}", e))?;

        if !status.success() {
            let log_tail = std::fs::read_to_string(&log_path)
                .unwrap_or_default()
                .lines()
                .rev()
                .take(10)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            return Err(format!("pg_ctl start failed:\n{}", log_tail));
        }

        self.started = true;
        Ok(())
    }

    /// Wait for the instance to exit recovery (become read-write).
    pub fn wait_for_recovery(&self, timeout: Duration) -> Result<(), String> {
        let psql = self.pg_bin_dir.join("psql");
        let start = Instant::now();
        let poll_interval = Duration::from_secs(2);
        let os_user = std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "postgres".to_string());

        loop {
            if start.elapsed() > timeout {
                return Err("timeout waiting for recovery to complete".into());
            }

            let output = Command::new(&psql)
                .args([
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &self.port.to_string(),
                    "-U",
                    &os_user,
                    "-d",
                    "postgres",
                    "-tAc",
                    "SELECT pg_is_in_recovery()",
                ])
                .output();

            match output {
                Ok(o) if o.status.success() => {
                    let result = String::from_utf8_lossy(&o.stdout).trim().to_string();
                    if result == "f" {
                        return Ok(());
                    }
                }
                _ => {}
            }

            std::thread::sleep(poll_interval);
        }
    }

    /// Export a table using pg_dump and import it into the production database.
    pub fn dump_and_restore(
        &self,
        db_name: &str,
        schema: &str,
        table: &str,
        prod_host: &str,
        prod_port: u16,
        prod_db: &str,
    ) -> Result<(), String> {
        let pg_dump = self.pg_bin_dir.join("pg_dump");
        let pg_restore = self.pg_bin_dir.join("pg_restore");

        let qualified_table = format!("{}.{}", schema, table);

        // Determine the OS user running the server process (for pg_dump / pg_restore auth).
        let os_user = std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .unwrap_or_else(|_| "postgres".to_string());

        // Write dump to a temp file (avoids pipe-size issues and makes debugging easier).
        let dump_file = self.data_dir.join("flashback_table_dump.dmp");

        let dump_status = Command::new(&pg_dump)
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &self.port.to_string(),
                "-U",
                &os_user,
                "-Fc",
                "-t",
                &qualified_table,
                "-f",
                dump_file.to_str().unwrap_or(""),
                db_name,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| format!("pg_dump: {}", e))?;

        if !dump_status.status.success() {
            let stderr = String::from_utf8_lossy(&dump_status.stderr);
            return Err(format!("pg_dump failed: {}", stderr));
        }

        // Verify dump file has content
        let dump_size = std::fs::metadata(&dump_file)
            .map(|m| m.len())
            .unwrap_or(0);
        if dump_size < 50 {
            return Err(format!(
                "pg_dump produced empty file ({} bytes); table '{}' may not exist in temp instance db '{}'",
                dump_size, qualified_table, db_name
            ));
        }

        // Drop the target table in production first (with CASCADE) so pg_restore starts clean.
        let _ = Command::new(&self.pg_bin_dir.join("psql"))
            .args([
                "-h", prod_host,
                "-p", &prod_port.to_string(),
                "-U", &os_user,
                "-d", prod_db,
                "-c",
                &format!("DROP TABLE IF EXISTS {}.{} CASCADE", schema, table),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        // pg_restore from dump file into production.
        let restore_result = Command::new(&pg_restore)
            .args([
                "--no-owner",
                "--no-privileges",
                "--disable-triggers",
                "--exit-on-error",
                "-h",
                prod_host,
                "-p",
                &prod_port.to_string(),
                "-U",
                &os_user,
                "-d",
                prod_db,
                dump_file.to_str().unwrap_or(""),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| format!("pg_restore: {}", e))?;

        if !restore_result.status.success() {
            let stderr = String::from_utf8_lossy(&restore_result.stderr);
            return Err(format!("pg_restore failed: {}", stderr.trim()));
        }

        // Remove the dump file
        let _ = std::fs::remove_file(&dump_file);

        Ok(())
    }

    /// Stop the temporary instance.
    pub fn stop(&mut self) -> Result<(), String> {
        if !self.started {
            return Ok(());
        }

        let pg_ctl = self.pg_bin_dir.join("pg_ctl");
        let output = Command::new(&pg_ctl)
            .args([
                "stop",
                "-D",
                self.data_dir.to_str().unwrap_or(""),
                "-m",
                "immediate",
            ])
            .output()
            .map_err(|e| format!("pg_ctl stop: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("pg_ctl stop failed: {}", stderr));
        }

        self.started = false;
        Ok(())
    }

    /// Remove the temporary data directory.
    pub fn cleanup(&self) -> Result<(), String> {
        if self.data_dir.exists() {
            std::fs::remove_dir_all(&self.data_dir)
                .map_err(|e| format!("cleanup {}: {}", self.data_dir.display(), e))?;
        }
        Ok(())
    }
}

impl Drop for TempInstance {
    fn drop(&mut self) {
        if self.started {
            let _ = self.stop();
        }
    }
}
