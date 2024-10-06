// TODO: remove once used in other components
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Lines, Write};
use std::path::{Path, PathBuf};
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
    pub fn new(id: u64, path: &Path) -> Self {
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
    pub fn new(id: u64, log_directory: &Path, max_size: u64) -> Self {
        Self {
            log_directory: log_directory.to_path_buf(),
            current_size: 0,
            max_size,
            buffer: Vec::new(),
            segment: Segment::new(id, log_directory),
            closed_segments: Vec::new(),
        }
    }

    /// Restore the [`Wal`] through reading the segment files which are in the
    /// provided directory.
    pub fn restore(&mut self) {
        let wal_files = std::fs::read_dir(&self.log_directory).expect("Can read set log_directory");
        for w in wal_files {
            let w = w.expect("Valid file within log directory");
            let wal_file = File::open(w.path()).expect("File from given WAL should exist");
            let reader = BufReader::new(wal_file);

            // The segments are bounded by the [`WAL_MAX_SEGMENT_SIZE_BYTES`]
            // so this ensures the files themselves do not become huge for this
            // operation.
            let mut buf = Vec::new();

            for line in reader.lines() {
                let line = line.unwrap();
                let entry: WalEntry = bincode::deserialize(line.as_bytes()).unwrap();
                buf.push(entry);
            }
            self.append_batch(buf);
        }
    }

    /// Return a [`Lines`] iterator over the active segment file.
    pub fn lines(&self) -> Lines<BufReader<File>> {
        let segment_path = format!(
            "{}/{}.wal",
            self.log_directory.display(),
            self.segment.id.load(Ordering::Relaxed)
        );
        let segment_file = std::fs::File::open(&segment_path)
            .expect("Active segment file exists within log_directory");

        BufReader::new(segment_file).lines()
    }

    /// Append a [`WalEntry`] to the WAL file.
    pub fn append(&mut self, entry: WalEntry) -> u64 {
        self.append_batch(vec![entry])
    }

    /// Append a batch of [`WalEntry`] into the current log file.
    fn append_batch(&mut self, entries: Vec<WalEntry>) -> u64 {
        // Invariant: the buffer should be empty here as it was previously
        // cleared after appending older entries. If the buffer is not empty
        // then we can append duplicate data which may become unbounded in size
        // as more and more data is added.
        assert_eq!(self.buffer.len(), 0);

        for e in entries {
            // TODO: create append_entry func which is generic over Write trait
            bincode::serialize_into(&mut self.buffer, &e).unwrap();
            writeln!(&mut self.buffer).unwrap();
        }
        self.segment.log_file.write_all(&self.buffer).unwrap();
        let buffer_size = self.buffer.len() as u64;
        self.current_size += buffer_size;

        // The buffer has been written, we do not need to keep it around otherwise
        // we risk misinforming the current segment size, as well as appending
        // pre-existing data.
        self.buffer.clear();
        buffer_size
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
        let new_segment = Segment::new(current_id + 1, &self.log_directory);
        self.current_size = 0;
        self.segment = new_segment;
    }

    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// ID of the active segment file.
    pub fn id(&self) -> u64 {
        self.segment.id()
    }

    /// Retrieve all closed segments.
    ///
    /// NOTE: These are safe to remove when the current [`Memtable`] has been
    /// flushed to an [`SSTable`].
    pub fn closed_segments(&self) -> Vec<u64> {
        self.closed_segments.clone()
    }

    pub fn clear_segments(&mut self) {
        self.closed_segments.clear();
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufRead, BufReader};

    use super::*;

    use tempdir::TempDir;

    const TINY_WAL_MAX_SIZE: u64 = 10;

    fn put_entries() -> Vec<WalEntry> {
        vec![
            WalEntry::Put {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            },
            WalEntry::Put {
                key: b"baz".to_vec(),
                value: b"qux".to_vec(),
            },
        ]
    }

    #[test]
    fn write_to_wal() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);

        let wrote = wal.append_batch(put_entries());
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
        let mut wal = Wal::new(1, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);
        let wrote = wal.append_batch(entries);
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
    fn wal_replay() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);

        let mut wrote = 0;
        for i in 0..10 {
            match i {
                0 | 3 | 6 => {
                    wrote += wal.append(WalEntry::Delete {
                        key: format!("key{i}").as_bytes().to_vec(),
                    })
                }
                _ => {
                    let key = format!("key{i}");
                    let value = format!("value{i}");
                    wrote += wal.append(WalEntry::Put {
                        key: key.as_bytes().to_vec(),
                        value: value.as_bytes().to_vec(),
                    })
                }
            };
        }
        assert_eq!(wal.current_size, wrote);

        // Drop the WAL and perform a restore
        drop(wal);

        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);
        wal.restore();
        assert_eq!(
            wal.current_size, wrote,
            "WAL size should be the same prior to dropping"
        );
    }

    #[test]
    fn id() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);
        assert_eq!(wal.segment.id(), 0);
    }

    #[test]
    fn wal_path() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);
        assert_eq!(
            wal.path(),
            temp_dir.into_path().join("0.wal"),
            "WAL filename was not in the expected format"
        );
    }

    #[test]
    fn rotation() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES);

        for _ in 0..3 {
            wal.append(WalEntry::Put {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            });
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
