// TODO: remove once used in other components
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Lines, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::{fs::File, sync::atomic::AtomicU64};

use serde::{Deserialize, Serialize};

use crate::ChipmunkError;

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
    /// Create a new [`Segment`].
    ///
    /// # Panics
    ///
    /// When the underlying file for the segment cannot be created with write
    /// permissions.
    pub fn try_new(id: u64, path: &Path) -> Result<Self, ChipmunkError> {
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
            .map_err(ChipmunkError::SegmentOpen)?;

        Ok(Self {
            id,
            log_file: new_segment,
        })
    }

    pub fn flush(&mut self) -> Result<(), ChipmunkError> {
        self.log_file
            .sync_all()
            .map_err(ChipmunkError::SegmentFsync)
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

const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

impl Wal {
    pub fn new(id: u64, log_directory: &Path, max_size: u64, buffer_size: Option<usize>) -> Self {
        let buf = if let Some(size) = buffer_size {
            Vec::with_capacity(size)
        } else {
            Vec::with_capacity(DEFAULT_BUFFER_SIZE)
        };

        Self {
            log_directory: log_directory.to_path_buf(),
            current_size: 0,
            max_size,
            buffer: buf,
            segment: Segment::try_new(id, log_directory).unwrap(),
            closed_segments: Vec::new(),
        }
    }

    /// Restore the [`Wal`] through reading the segment files which are in the
    /// provided directory.
    pub fn restore(&mut self) -> Result<(), ChipmunkError> {
        let segment_files = std::fs::read_dir(&self.log_directory).map_err(|e| {
            ChipmunkError::WalDirectoryOpen {
                source: e,
                path: self.log_directory.clone(),
            }
        })?;
        for s in segment_files {
            let segment = s.expect("Valid file within log directory");
            let segment_file = File::open(segment.path()).map_err(ChipmunkError::SegmentOpen)?;
            let reader = BufReader::new(segment_file);

            // The segments are bounded by the [`WAL_MAX_SEGMENT_SIZE_BYTES`]
            // so this ensures the files themselves do not become huge for this
            // operation.
            let mut buf = Vec::new();

            for line in reader.lines() {
                let line = line.expect("WAL contains valid utf8");

                match bincode::deserialize(line.as_bytes()) {
                    Ok(entry) => buf.push(entry),
                    // Invalid entries are skipped and not restored
                    Err(e) => eprintln!("Invalid entry in segment: {e}"),
                }
            }
            self.append_batch(buf)?;
        }

        Ok(())
    }

    /// Return a [`Lines`] iterator over the active segment file.
    pub fn lines(&self) -> Result<Lines<BufReader<File>>, ChipmunkError> {
        let segment_path = format!(
            "{}/{}.wal",
            self.log_directory.display(),
            self.segment.id.load(Ordering::Relaxed)
        );
        let segment_file =
            std::fs::File::open(&segment_path).map_err(ChipmunkError::SegmentOpen)?;

        Ok(BufReader::new(segment_file).lines())
    }

    /// Append a [`WalEntry`] to the WAL file.
    pub fn append(&mut self, entry: WalEntry) -> Result<u64, ChipmunkError> {
        self.append_batch(vec![entry])
    }

    /// Append a batch of [`WalEntry`] into the current log file.
    fn append_batch(&mut self, entries: Vec<WalEntry>) -> Result<u64, ChipmunkError> {
        // Invariant: the buffer should be empty here as it was previously
        // cleared after appending older entries. If the buffer is not empty
        // then we can append duplicate data which may become unbounded in size
        // as more and more data is added.
        assert_eq!(self.buffer.len(), 0);

        for e in entries {
            // TODO: create append_entry func which is generic over Write trait
            bincode::serialize_into(&mut self.buffer, &e).expect("Can write known entry to buffer");
            writeln!(&mut self.buffer).expect("Can write known entry to buffer");
        }
        self.segment
            .log_file
            .write_all(&self.buffer)
            .map_err(ChipmunkError::WalAppend)?;
        let buffer_size = self.buffer.len() as u64;
        self.current_size += buffer_size;

        // The buffer has been written, we do not need to keep it around otherwise
        // we risk misinforming the current segment size, as well as appending
        // pre-existing data.
        self.buffer.clear();
        Ok(buffer_size)
    }

    /// Get the current path of the active WAL segment file.
    pub fn path(&self) -> PathBuf {
        format!("{}/{}.wal", self.log_directory.display(), self.segment.id()).into()
    }

    /// Rotate the currently active WAL segment file.
    ///
    /// This flushes all operations to the current file before creating a new file.
    pub fn rotate(&mut self) -> Result<(), ChipmunkError> {
        eprintln!("WAL rotation");
        self.segment.flush()?;

        let current_id = self.segment.id();
        self.closed_segments.push(current_id);
        self.current_size = 0;
        self.segment = Segment::try_new(current_id + 1, &self.log_directory)?;

        Ok(())
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
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);

        let wrote = wal.append_batch(put_entries()).unwrap();
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
        let mut wal = Wal::new(1, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);
        let wrote = wal.append_batch(entries).unwrap();
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
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);

        let mut wrote = 0;
        for i in 0..10 {
            match i {
                0 | 3 | 6 => {
                    wrote += wal
                        .append(WalEntry::Delete {
                            key: format!("key{i}").as_bytes().to_vec(),
                        })
                        .unwrap()
                }
                _ => {
                    let key = format!("key{i}");
                    let value = format!("value{i}");
                    wrote += wal
                        .append(WalEntry::Put {
                            key: key.as_bytes().to_vec(),
                            value: value.as_bytes().to_vec(),
                        })
                        .unwrap();
                }
            };
        }
        assert_eq!(wal.current_size, wrote);

        // Drop the WAL and perform a restore
        drop(wal);

        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);
        wal.restore().unwrap();
        assert_eq!(
            wal.current_size, wrote,
            "WAL size should be the same prior to dropping"
        );
    }

    #[test]
    fn id() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);
        assert_eq!(wal.segment.id(), 0);
    }

    #[test]
    fn wal_path() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);
        assert_eq!(
            wal.path(),
            temp_dir.into_path().join("0.wal"),
            "WAL filename was not in the expected format"
        );
    }

    #[test]
    fn rotation() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);

        for _ in 0..3 {
            wal.append(WalEntry::Put {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
            })
            .unwrap();
        }

        assert_eq!(wal.closed_segments.len(), 0, "No closed segments");
        assert_eq!(wal.segment.id(), 0);

        wal.rotate().expect("Can rotate WAL");

        assert_eq!(wal.current_size, 0, "WAL size should reset");
        assert_ne!(wal.segment.id(), 0, "WAL rotation has not occurred");
        assert_eq!(wal.closed_segments.len(), 1);
        assert_eq!(wal.segment.id(), 1);
    }
}
