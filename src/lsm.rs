#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::{
    memtable::Memtable,
    wal::{Wal, WalEntry},
};

pub struct Lsm<P: AsRef<Path>> {
    wal: Wal<P>,

    /// Active [`Memtable`]
    memtable: Memtable,

    /// IDs of sealed memtables
    /// TODO: hold these in memory too, so that I/O is greatly reduced?
    sealed_memtables: Vec<u64>,

    working_directory: PathBuf,
}

pub struct WalConfig<P: AsRef<Path>> {
    id: u64,
    max_size: u64,
    log_directory: P,
}

pub struct MemtableConfig {
    id: u64,
    max_size: u64,
}

impl<P: AsRef<Path>> Lsm<P> {
    pub fn new(id: u64, max_size: u64, working_directory: P) -> Self {
        let dir = PathBuf::from(working_directory.as_ref());
        Self {
            wal: Wal::new(id, working_directory, max_size),
            memtable: Memtable::new(id, max_size),
            sealed_memtables: Vec::new(),
            working_directory: dir,
        }
    }

    /// Put an item into the [`Lsm`] tree.
    ///
    /// A [`WalEntry`] is appended into the WAL before proceeding to insert the
    /// key-value pair into an in-memory index, the L0 [`Memtable`].
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry = WalEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.wal.append(entry);

        self.memtable.put(key, value);
    }

    pub fn rotate_memtable(&mut self) {
        self.sealed_memtables.push(self.memtable.id());
        self.memtable.flush(self.working_directory.clone());
    }

    /// Get a key and corresponding value from the LSM-tree.
    ///
    /// This first checks whether the value exists in the [`Memtable`] and continues
    /// the search through persisted SSTables if it does not. After exhausting
    /// all options, the value does not exist.
    ///
    /// TODO: Improve retrieval through use of a bloom filter
    pub fn get(&mut self, key: &'static [u8]) -> Option<Vec<u8>> {
        match self.memtable.get(key) {
            Some(v) => Some(v.to_vec()),
            None => {
                for memtable_id in self.sealed_memtables.iter().rev() {
                    let data = std::fs::read(
                        self.working_directory
                            .join(format!("sstable-{memtable_id}")),
                    )
                    .expect("Previously sealed memtable file should exist");
                    let memtable: BTreeMap<Bytes, Bytes> = bincode::deserialize(&data).unwrap();
                    match memtable.get(key) {
                        Some(v) => return Some(v.to_vec()),
                        None => continue,
                    };
                }
                // Exhausted search of entire structure did not find the key, so
                // it does not exist.
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use super::Lsm;

    #[test]
    fn crud() {
        let dir = TempDir::new("crud").unwrap();
        let mut lsm = Lsm::new(0, 10, dir.path());

        lsm.put(b"foo".to_vec(), b"bar".to_vec());
        assert_eq!(lsm.memtable.id(), 0);
        assert!(lsm.get(b"foo").is_some());
        assert_eq!(lsm.get(b"foo").unwrap(), b"bar");

        lsm.rotate_memtable();
        assert_eq!(
            lsm.memtable.id(),
            1,
            "Engine should have a new memtable after flush"
        );
        assert_eq!(
            lsm.get(b"foo"),
            Some(b"bar".to_vec()),
            "Value should be found in sstable on disk"
        );
    }
}
