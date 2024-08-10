// TODO: remove once used in other components
#![allow(dead_code)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::{fs::File, sync::atomic::AtomicU64};

use serde::{Deserialize, Serialize};

pub const WAL_MAX_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

/// Wal maintains a write-ahead log (WAL) as an append-only file to provide persistence
/// across crashes of the system.
pub struct Wal {
    log_directory: PathBuf,
    current_size: u64,
    max_size: u64,
    buffer: Vec<u8>,

    /// Closed, immutable WAL segment IDs.
    ///
    /// Once the incoming WAL that was placed into a memtable has subsequently
    /// been flushed to an SSTable, these can be safely archived or deleted.
    closed_segments: Vec<u64>,

    /// Active segment file
    segment: Segment,
}

struct Segment {
    /// ID of the segment.
    id: AtomicU64,
    log_file: File,
}

impl Segment {
    pub fn new(id: u64, path: PathBuf) -> Self {
        let id = AtomicU64::new(id);
        let log_file_path = format!(
            "{}/{}.wal",
            path.display(),
            id.load(std::sync::atomic::Ordering::Acquire)
        );
        let new_segment = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file_path)
            .unwrap();

        Self {
            id,
            log_file: new_segment,
        }
    }

    pub fn flush(&mut self) {
        self.log_file.sync_all().unwrap()
    }

    // The current WAL id.
    pub fn id(&self) -> u64 {
        self.id.load(Ordering::Acquire)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Wal {
    pub fn new(id: u64, log_directory: PathBuf, max_size: u64) -> Self {
        Self {
            log_directory: log_directory.clone(),
            current_size: 0,
            max_size,
            buffer: Vec::new(),
            segment: Segment::new(id, log_directory),
            closed_segments: Vec::new(),
        }
    }

    /// Append one or more key-value pairs to the WAL file.
    ///
    pub fn append(&mut self, entries: Vec<WalEntry>) -> u64 {
        self.append_batch(entries)
    }

    /// Append a batch of [`WalEntry`] into the current log file.
    fn append_batch(&mut self, entries: Vec<WalEntry>) -> u64 {
        for e in entries {
            // TODO: create append_entry func which is generic over Write trait
            bincode::serialize_into(&mut self.buffer, &e).unwrap();
            writeln!(&mut self.buffer).unwrap();
        }
        self.segment.log_file.write_all(&self.buffer).unwrap();
        self.current_size = self.buffer.len() as u64;
        self.buffer.len() as u64
    }

    /// Get the current path of the active WAL segment file.
    pub fn path(&self) -> PathBuf {
        format!("{}/{}.wal", self.log_directory.display(), self.segment.id()).into()
    }

    /// Rotate the currently active WAL segment file.
    ///
    /// This flushes all operations to the current file before creating a new file.
    pub fn rotate(&mut self) {
        eprintln!("WAL rotation");
        self.segment.flush();

        let current_id = self.segment.id();
        self.closed_segments.push(current_id);
        let new_segment = Segment::new(current_id + 1, self.log_directory.clone());
        self.current_size = 0;
        self.segment = new_segment;
    }

    pub fn size(&self) -> u64 {
        self.current_size
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufRead, BufReader};

    use super::*;

    use tempdir::TempDir;

    const TINY_WAL_MAX_SIZE: u64 = 10;

    #[test]
    fn write_to_wal() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.path().to_path_buf(), WAL_MAX_SEGMENT_SIZE_BYTES);

        let entry = WalEntry::Put {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
        };
        let wrote = wal.append(vec![entry]);
        assert_eq!(wal.current_size, wrote);

        let file = std::fs::File::open(wal.path()).unwrap();
        let entry: WalEntry = bincode::deserialize_from(&file).unwrap();
        match entry {
            WalEntry::Put { key, value } => {
                assert_eq!(&String::from_utf8_lossy(&key), "foo");
                assert_eq!(&String::from_utf8_lossy(&value), "bar");
            }
            WalEntry::Delete { key: _ } => panic!("Expected put operation, got delete!"),
        }

        // Write multiple entries
        let mut entries = vec![];
        for _ in 0..10 {
            entries.push(WalEntry::Put {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            });
        }
        let mut wal = Wal::new(1, temp_dir.path().to_path_buf(), WAL_MAX_SEGMENT_SIZE_BYTES);
        let wrote = wal.append(entries);
        assert_eq!(wal.current_size, wrote);
        let wal_file = BufReader::new(std::fs::File::open(wal.path()).unwrap());
        for line in wal_file.lines() {
            let entry: WalEntry = bincode::deserialize(line.unwrap().as_bytes()).unwrap();
            match entry {
                WalEntry::Put { key, value } => {
                    assert_eq!(&String::from_utf8_lossy(&key), "foo");
                    assert_eq!(&String::from_utf8_lossy(&value), "bar");
                }
                WalEntry::Delete { key: _ } => panic!("Expected put operation, got delete!"),
            }
        }
    }

    #[test]
    fn id() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir.into_path(), WAL_MAX_SEGMENT_SIZE_BYTES);
        assert_eq!(wal.segment.id(), 0);
    }

    #[test]
    fn wal_path() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir.path().to_path_buf(), WAL_MAX_SEGMENT_SIZE_BYTES);
        assert_eq!(
            wal.path(),
            temp_dir.into_path().join("0.wal"),
            "WAL filename was not in the expected format"
        );
    }

    #[test]
    fn rotation() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.into_path(), WAL_MAX_SEGMENT_SIZE_BYTES);

        for _ in 0..3 {
            wal.append(vec![WalEntry::Put {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            }]);
        }

        assert_eq!(wal.closed_segments.len(), 0, "No closed segments");
        assert_eq!(wal.segment.id(), 0);

        wal.rotate();

        assert_eq!(wal.current_size, 0, "WAL size should reset");
        assert_ne!(wal.segment.id(), 0, "WAL rotation has not occurred");
        assert_eq!(wal.closed_segments.len(), 1);
        assert_eq!(wal.segment.id(), 1);
    }
}
