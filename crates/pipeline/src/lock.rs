//! Single-instance lock for a pipeline work directory.
//!
//! Prevents two `anigraph` processes from writing to the same
//! `--work-dir` at once (which would interleave/corrupt the JSONL outputs).
//!
//! # Why this exists
//!
//! `cargo run` spawns the binary as a child. Killing `cargo` (e.g. via
//! `timeout`, or a programmatic `SIGTERM`) does not always propagate to that
//! child, leaving an orphaned `anigraph` process silently continuing to write.
//! A later `--resume` run would then race the orphan and corrupt the file.
//!
//! The lock makes that situation loud instead of corrupting data: a second
//! instance refuses to start while a live one holds the lock. A stale lock
//! (left behind only by a hard `SIGKILL`, which `Drop` cannot catch) is
//! detected via PID liveness and taken over automatically.
//!
//! Unix-only (`kill -0` for liveness). On non-unix targets the module is
//! compiled out, so callers must treat `acquire` as a no-op there.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

/// Guard that holds the lock for its lifetime; removes the lock file on drop.
pub struct LockGuard {
    path: PathBuf,
}

/// Acquire the single-instance lock for `work_dir`.
///
/// Errors if another *live* `anigraph` process already holds it. A stale
/// lock (PID no longer running) is removed and taken over.
pub fn acquire(work_dir: &Path) -> Result<LockGuard> {
    fs::create_dir_all(work_dir).context("creating work dir for lock")?;
    let path = work_dir.join(".anigraph.lock");

    let existing_pid = fs::read_to_string(&path)
        .ok()
        .and_then(|p| p.lines().next().and_then(|l| l.trim().parse::<i32>().ok()));
    if let Some(pid) = existing_pid {
        if is_alive(pid) {
            bail!(
                "Another anigraph instance is already running (PID {pid}). \
                 Refusing to start to avoid corrupting the dataset. \
                 Kill it (e.g. `kill -9 {pid}`) or use a different --work-dir."
            );
        }
        // Stale lock — take it over.
        let _ = fs::remove_file(&path);
    }

    let pid = std::process::id();
    let content = format!(
        "{pid}\nstarted_at={}\n",
        chrono::Utc::now().to_rfc3339()
    );
    fs::write(&path, content).context("writing lock file")?;

    Ok(LockGuard { path })
}

/// Returns `true` if a process with `pid` currently exists (Unix `kill -0`).
fn is_alive(pid: i32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
