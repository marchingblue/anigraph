use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

/// A writer for newline-delimited JSON (JSONL) files.
///
/// Intermediate pipeline outputs are written uncompressed to support
/// line-level truncation on resume. Tracks exact byte positions so
/// truncation is precise — no approximate `line_count * 256` estimates.
///
/// # Resume correctness
///
/// On resume, the writer opens in append mode. The caller truncates the
/// underlying file to the byte position stored in the checkpoint before
/// resuming. Since `sort: ID` pagination is stable, refetched pages
/// produce identical entries — no duplication at page boundaries.
pub struct JsonlWriter {
    /// Inner writer. Wrapped in `Option` so we can take it safely in flush
    /// and to avoid use-after-move if we ever need partial teardown.
    writer: Option<BufWriter<File>>,
    /// Current byte position in the file.
    byte_offset: u64,
}

impl JsonlWriter {
    /// Create a new file (truncates if exists).
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::create(path).context("creating JSONL file")?;
        Ok(Self {
            writer: Some(BufWriter::new(file)),
            byte_offset: 0,
        })
    }

    /// Open an existing file in append mode.
    pub fn append(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .context("opening JSONL file for append")?;
        let byte_offset = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            writer: Some(BufWriter::new(file)),
            byte_offset,
        })
    }

    /// Truncate the file to a specific byte position (for resume).
    /// Flushes the buffer first, then calls `set_len` on the underlying file.
    pub fn truncate_to(&mut self, byte_pos: u64) -> Result<()> {
        self.flush()?;
        if let Some(ref file) = self.writer {
            file.get_ref().set_len(byte_pos)?;
            file.get_ref().sync_all()?;
        }
        self.byte_offset = byte_pos;
        Ok(())
    }

    /// Write a single JSON line followed by a newline.
    /// Returns the byte offset of the start of this line.
    pub fn write(&mut self, value: &impl Serialize) -> Result<u64> {
        let offset = self.byte_offset;
        let line = serde_json::to_string(value)?;
        self.write_line(&line)?;
        Ok(offset)
    }

    /// Write a raw string as a JSONL line (no serialization).
    /// The string should already be valid JSON — it is written as-is followed
    /// by a newline. Useful for pass-through of unmodified entries.
    pub fn write_raw(&mut self, line: &str) -> Result<()> {
        self.write_line(line)?;
        Ok(())
    }

    /// Shared implementation: writes bytes + newline, updates offset.
    fn write_line(&mut self, line: &str) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        self.byte_offset += line.len() as u64 + 1;
        Ok(())
    }

    /// Current byte offset in the file (start position for the next write).
    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    /// Flush the buffer.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush().context("flushing JSONL writer")?;
        }
        Ok(())
    }
}

impl Drop for JsonlWriter {
    fn drop(&mut self) {
        if let Some(ref mut writer) = self.writer {
            let _ = writer.flush();
        }
    }
}
