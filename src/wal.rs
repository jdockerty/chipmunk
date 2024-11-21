// TODO: remove once used in other components
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Lines, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::{fs::File, sync::atomic::AtomicU64};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::ChipmunkError;

pub const WAL_MAX_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

// Taken from the Rust [sys_common](https://doc.rust-lang.org/src/std/sys_common/io.rs.html#3)
// crate for a sane default size.
const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

const WAL_INSERT_MARKER: u8 = 0;
const WAL_DELETE_MARKER: u8 = 1;

/// Wal maintains a write-ahead log (WAL) as an append-only file to provide persistence
/// across crashes of the system.
#[derive(Debug)]
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
        info!("Restoring WAL");
        let segment_files = std::fs::read_dir(&self.log_directory).map_err(|e| {
            ChipmunkError::WalDirectoryOpen {
                source: e,
                path: self.log_directory.clone(),
            }
        })?;

        let mut segment_count = 0;
        for s in segment_files.into_iter() {
            let segment = s.expect("Valid file within log directory");
            if !segment.file_type().unwrap().is_file() {
                debug!(name=?segment.file_name(), "Skipping directory during restore");
                continue;
            }

            if !segment.file_name().to_string_lossy().contains("wal") {
                info!(name=?segment.file_name(), "Skipping non-WAL file during restore");
                continue;
            }

            if segment.metadata().unwrap().len() == 0 {
                info!(name=?segment.file_name(), "Skipping empty WAL");
                continue;
            }
            // Only include segments which are valid
            segment_count += 1;
            let segment_file = File::open(segment.path()).map_err(ChipmunkError::SegmentOpen)?;
            let mut bytes_read = 0;
            let max_bytes = segment_file.metadata().expect("Can read metadata").len();
            info!(
                name = ?segment.file_name(),
                segment_size = max_bytes,
                current_segment_number = segment_count,
                "Restoring segment"
            );
            let reader = BufReader::new(segment_file);

            // The segments are bounded by the [`WAL_MAX_SEGMENT_SIZE_BYTES`]
            // so this ensures the files themselves do not become huge for this
            // operation.
            let mut buf = Vec::new();

            for line in reader.lines() {
                let line = line.expect("WAL contains valid utf8");
                bytes_read += line.as_bytes().len();
                buf.push(WalEntry::from_bytes(line.as_bytes()));
            }
            info!(
                bytes_read,
                current_segment = segment_count,
                "Completed segment"
            );
            self.append_batch(buf)?;
        }
        info!(total_segments = segment_count, "Restored segments");

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
            self.buffer
                .write_all(&e.as_bytes())
                .expect("Can write known entry to buffer");
        }
        self.segment
            .log_file
            .write_all(&self.buffer)
            .map_err(ChipmunkError::WalAppend)?;
        self.segment.log_file.flush().unwrap();
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
        info!("Rotating WAL");
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

    /// Remove closed segments and return the number of segments that were
    /// removed.
    pub fn remove_closed_segments(&mut self) -> Result<u64, ChipmunkError> {
        let segments = self.closed_segments();
        let mut cleared = 0;

        for s in segments {
            let segment_path = format!("{}/{}.wal", self.log_directory.display(), s);
            std::fs::remove_file(&segment_path).map_err(ChipmunkError::SegmentDelete)?;
            cleared += 1;
        }
        self.clear_segments();
        Ok(cleared)
    }
}

#[derive(Debug)]
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WalEntry {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        match self {
            Self::Put { key, value } => {
                buf.write_u8(WAL_INSERT_MARKER).unwrap();
                buf.write_u64::<BigEndian>(key.len() as u64).unwrap();
                buf.write_all(key).unwrap();
                buf.write_u64::<BigEndian>(value.len() as u64).unwrap();
                buf.write_all(value).unwrap();
                buf.write_all(b"\n").unwrap();
            }
            Self::Delete { key } => {
                buf.write_u8(WAL_DELETE_MARKER).unwrap();
                buf.write_u64::<BigEndian>(key.len() as u64).unwrap();
                buf.write_all(key).unwrap();
                buf.write_all(b"\n").unwrap();
            }
        }
        buf.shrink_to_fit();
        buf
    }

    pub fn from_bytes(mut reader: &[u8]) -> WalEntry {
        let marker = reader.read_u8().unwrap();
        match marker {
            WAL_INSERT_MARKER => {
                let key_sz = reader.read_u64::<BigEndian>().unwrap();
                let mut key = vec![0; key_sz as usize];
                reader.read_exact(&mut key).unwrap();

                let value_sz = reader.read_u64::<BigEndian>().unwrap();
                let mut value = vec![0; value_sz as usize];
                reader.read_exact(&mut value).unwrap();

                WalEntry::Put { key, value }
            }
            WAL_DELETE_MARKER => {
                let key_sz = reader.read_u64::<BigEndian>().unwrap();
                let mut key = vec![0; key_sz as usize];
                reader.read_exact(&mut key).unwrap();
                WalEntry::Delete { key }
            }
            _ => panic!("Unknown marker encountered"),
        }
    }

    pub fn from_reader<R: Read>(reader: &mut R) -> WalEntry {
        let marker = reader.read_u8().unwrap();
        match marker {
            WAL_INSERT_MARKER => {
                let key_sz = reader.read_u64::<BigEndian>().unwrap();
                let mut key = vec![0; key_sz as usize];
                reader.read_exact(&mut key).unwrap();

                let value_sz = reader.read_u64::<BigEndian>().unwrap();
                let mut value = vec![0; value_sz as usize];
                reader.read_exact(&mut value).unwrap();

                WalEntry::Put { key, value }
            }
            WAL_DELETE_MARKER => {
                let key_sz = reader.read_u64::<BigEndian>().unwrap();
                let mut key = vec![0; key_sz as usize];
                reader.read_exact(&mut key).unwrap();
                WalEntry::Delete { key }
            }
            _ => panic!("Unknown marker encountered"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{BufRead, BufReader, Cursor, Seek};

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
    fn wal_entry_bytes() {
        let mut buf = Cursor::new(Vec::new());
        let entry = WalEntry::Put {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };

        assert!(
            buf.write(&entry.as_bytes()).unwrap() > 0,
            "Expected non-zero write"
        );

        // Reset the cursor after writing
        buf.seek(std::io::SeekFrom::Start(0)).unwrap();
        let read_entry = WalEntry::from_reader(&mut buf);
        assert_eq!(entry, read_entry);
    }

    #[test]
    fn write_to_wal() {
        let temp_dir = TempDir::new("write_wal").unwrap();
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);

        let wrote = wal.append_batch(put_entries()).unwrap();
        assert_eq!(wal.current_size, wrote);

        let mut file = std::fs::File::open(wal.path()).unwrap();
        let entry: WalEntry = WalEntry::from_reader(&mut file);
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
            let entry: WalEntry = WalEntry::from_bytes(line.unwrap().as_bytes());
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

    #[test]
    fn segment_deletion() {
        let temp_dir = TempDir::new("clear_segments").unwrap();
        let mut wal = Wal::new(0, temp_dir.path(), WAL_MAX_SEGMENT_SIZE_BYTES, None);

        let segment_count = 5;
        for i in 0..segment_count {
            for _ in 0..3 {
                wal.append(WalEntry::Put {
                    key: b"foo".to_vec(),
                    value: b"bar".to_vec(),
                })
                .unwrap();
            }
            assert_eq!(wal.segment.id(), i);
            wal.rotate().expect("Can rotate WAL during test");
            assert_eq!(wal.segment.id(), i + 1);
        }

        assert_eq!(wal.closed_segments.len(), 5);
        assert_eq!(wal.segment.id(), 5, "Current active segment ID should be 5");

        let removed = wal
            .remove_closed_segments()
            .expect("Can remove segments in test");
        assert_eq!(removed, segment_count);
        assert_eq!(
            wal.closed_segments().len(),
            0,
            "There should be no closed segments remaining after removal"
        );
    }
}
