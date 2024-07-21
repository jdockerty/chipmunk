// TODO: remove once used in other components
#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;

use crate::wal::{Wal, WalEntry};

pub const MEMTABLE_MAX_SIZE_BYTES: u64 = 1048576; // 1 MiB

#[derive(Debug)]
pub struct Memtable {
    id: AtomicU64,
    tree: BTreeMap<Bytes, Bytes>,

    current_size: u64,
    max_size: u64,
}

impl Memtable {
    pub fn new(id: u64, max_size: u64) -> Self {
        Self {
            id: AtomicU64::new(id),
            tree: BTreeMap::new(),
            current_size: 0,
            max_size,
        }
    }

    pub fn load<P: AsRef<Path>>(&mut self, wal: Wal<P>) {
        let wal_file = File::open(wal.path()).expect("File from given WAL should exist");
        let reader = BufReader::new(wal_file);

        for line in reader.lines() {
            let line = line.unwrap();
            let entry: WalEntry = bincode::deserialize(line.as_bytes()).unwrap();
            match entry {
                WalEntry::Put { key, value } => {
                    self.tree.insert(key.into(), value.into());
                }
                WalEntry::Delete { key } => {
                    self.tree.remove(&*key);
                }
            }
        }
    }

    /// Put a key-value pair into the [`Memtable`].
    pub fn put(&mut self, key: &'static [u8], value: &'static [u8]) {
        let key = Bytes::from_static(key);
        let value = Bytes::from_static(value);
        self.current_size += (key.len() + value.len()) as u64;
        self.tree.insert(key, value);
    }

    /// Get a key-value pair from the [`Memtable`].
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.tree.get(key) {
            Some(v) => Some(v),
            None => None,
        }
    }

    /// Delete a key-value pair from the [`Memtable`].
    /// The key must exist in order to be deleted.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), &'static str> {
        match self.tree.remove(key) {
            Some(_) => Ok(()),
            None => Err("cannot remove nonexistent key"),
        }
    }

    /// Write the [`Memtable`] to disk, this then becomes a Sorted String Table
    /// (SSTable) and is immutable.
    pub fn flush(&mut self, flush_dir: PathBuf) {
        let data = bincode::serialize(&self.tree).unwrap();

        self.tree = BTreeMap::new();
        self.current_size = 0;

        std::fs::write(
            format!(
                "{}/sstable-{}",
                flush_dir.display(),
                self.id.load(Ordering::Acquire)
            ),
            data,
        )
        .unwrap();
        self.id.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use tempdir::TempDir;

    use crate::wal::{Wal, WalEntry, WAL_MAX_SIZE_BYTES};

    use super::{Memtable, MEMTABLE_MAX_SIZE_BYTES};

    const TINY_MEMTABLE_BYTES: u64 = 10;

    #[test]
    fn crud_operations() {
        let mut m = Memtable::new(0, MEMTABLE_MAX_SIZE_BYTES);
        m.put(b"foo", b"bar");

        assert_eq!(
            m.get(b"foo"),
            Some("bar".as_bytes()),
            "Expected key to exist after put"
        );

        assert!(m.delete(b"foo").is_ok());
        assert!(m.get(b"foo").is_none());
        assert!(m.delete(b"foo").is_err());
    }

    #[test]
    fn flush_to_sstable() {
        let mut m = Memtable::new(0, MEMTABLE_MAX_SIZE_BYTES);
        let flush_dir = TempDir::new("flush").unwrap();
        m.put(b"foo", b"bar");
        assert_eq!(m.current_size, b"foobar".len() as u64);
        m.flush(flush_dir.path().to_path_buf());
        assert_eq!(m.current_size, 0, "New memtable should have size of 0");
        assert!(m.tree.is_empty(), "New memtable should be empty");

        let sstable_file =
            std::fs::File::open(flush_dir.path().join("sstable-0")).expect("Flushed file exists");
        let data: BTreeMap<bytes::Bytes, bytes::Bytes> =
            bincode::deserialize_from(sstable_file).unwrap();
        assert_eq!(
            data.get(b"foo".as_ref()),
            Some(&bytes::Bytes::from_static(b"bar"))
        );
    }

    #[test]
    fn wal_replay() {
        let wal_dir = TempDir::new("replay").unwrap();

        let mut wal = Wal::new(0, wal_dir, WAL_MAX_SIZE_BYTES);
        for i in 0..10 {
            match i {
                0 | 3 | 6 => wal.append(WalEntry::Delete {
                    key: format!("key{i}").as_bytes().to_vec(),
                }),
                _ => {
                    let key = format!("key{i}");
                    let value = format!("value{i}");
                    wal.append(WalEntry::Put {
                        key: key.as_bytes().to_vec(),
                        value: value.as_bytes().to_vec(),
                    })
                }
            };
        }

        let mut m = Memtable::new(0, MEMTABLE_MAX_SIZE_BYTES);
        m.load(wal);

        for i in 0..10 {
            match i {
                0 | 3 | 6 => assert!(m.get(format!("key{i}").as_bytes()).is_none()),
                _ => assert_eq!(
                    m.get(format!("key{i}").as_bytes()),
                    Some(format!("value{i}").as_bytes())
                ),
            }
        }
    }
}
