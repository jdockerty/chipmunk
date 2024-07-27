// TODO: remove once used in other components
#![allow(dead_code)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::{fs::File, sync::atomic::AtomicU64};

use serde::{Deserialize, Serialize};

pub const WAL_MAX_SIZE_BYTES: u64 = 1048576; // 1 MiB

/// Wal maintains a write-ahead log (WAL) as an append-only file to provide persistence
/// across crashes of the system.
pub struct Wal<P: AsRef<Path>> {
    id: AtomicU64,
    log_file: File,
    log_directory: P,
    current_size: u64,
    max_size: u64,
    buffer: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl<P: AsRef<Path>> Wal<P> {
    pub fn new(id: u64, log_directory: P, max_size: u64) -> Self {
        let id = AtomicU64::new(id);
        let log_file_path = format!(
            "{}/{}.wal",
            log_directory.as_ref().display(),
            id.load(std::sync::atomic::Ordering::Acquire)
        );

        let log_file = std::fs::File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_file_path)
            .expect("Can create new WAL file");

        Self {
            id,
            log_file,
            log_directory,
            current_size: 0,
            max_size,
            buffer: Vec::new(),
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
        self.log_file.write_all(&self.buffer).unwrap();
        self.current_size = self.buffer.len() as u64;
        self.buffer.len() as u64
    }

    /// Get the current path of the WAL file.
    pub fn path(&self) -> PathBuf {
        format!(
            "{}/{}.wal",
            self.log_directory.as_ref().display(),
            self.id.load(std::sync::atomic::Ordering::Acquire)
        )
        .into()
    }

    /// Rotate the current WAL file.
    ///
    /// This flushes all operations to the current file before creating a new file.
    pub fn rotate(&mut self) {
        eprintln!("WAL rotation");
        self.log_file.sync_all().unwrap();

        let new_id = self.next_id();
        let log_file = std::fs::File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(self.log_directory.as_ref().join(format!("{new_id}.wal")))
            .expect("Can rotate WAL file");

        self.current_size = 0;
        self.log_file = log_file;
    }

    // The current WAL id.
    pub fn id(&self) -> u64 {
        self.id.load(Ordering::Acquire)
    }

    /// Set the next WAL id and return its value.
    pub fn next_id(&self) -> u64 {
        // This returns the previous, so we add one to it to get the new value.
        self.id.fetch_add(1, Ordering::Acquire) + 1
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
        let mut wal = Wal::new(0, &temp_dir, WAL_MAX_SIZE_BYTES);

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
        let mut wal = Wal::new(1, &temp_dir, WAL_MAX_SIZE_BYTES);
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
    fn next_id() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir, WAL_MAX_SIZE_BYTES);
        assert_eq!(wal.next_id(), 1);
        assert_eq!(wal.next_id(), 2);
    }

    #[test]
    fn id() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir, WAL_MAX_SIZE_BYTES);
        assert_eq!(wal.id(), 0);
        assert_eq!(wal.next_id(), 1);
    }

    #[test]
    fn wal_path() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, &temp_dir, WAL_MAX_SIZE_BYTES);
        assert_eq!(
            wal.path(),
            temp_dir.path().join("0.wal"),
            "WAL filename was not in the expected format"
        );
    }

    #[test]
    fn rotation() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir, WAL_MAX_SIZE_BYTES);

        for _ in 0..3 {
            wal.append(vec![WalEntry::Put {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            }]);
        }
        wal.rotate();
        assert_eq!(wal.current_size, 0, "WAL size should reset");
        assert_ne!(
            wal.id.load(Ordering::Acquire),
            0,
            "WAL rotation has not occurred"
        );
        assert_eq!(wal.id.load(Ordering::Acquire), 1);
    }
}
