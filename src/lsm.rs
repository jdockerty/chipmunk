#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::{
    memtable::Memtable,
    wal::{Wal, WalEntry},
};

pub struct Lsm<P: AsRef<Path> + Clone> {
    /// Write-ahead Log (WAL) which backs the operations performed on the LSM
    /// storage engine.
    wal: Wal<P>,
    /// The configuration which was used to initialise the [`Wal`].
    wal_config: WalConfig<P>,

    /// Currently active [`Memtable`]
    memtable: Memtable,
    /// The configuration which was used to initialise the [`Memtable`].
    memtable_config: MemtableConfig,

    /// IDs of the now immutable memtables
    /// TODO: hold these in memory too, so that I/O is greatly reduced?
    sstables: Vec<u64>,

    working_directory: PathBuf,
}

#[derive(Debug, Clone)]
pub struct WalConfig<P: AsRef<Path> + Clone + Sized> {
    id: u64,
    max_size: u64,
    log_directory: P,
}

#[derive(Debug, Clone)]
pub struct MemtableConfig {
    id: u64,
    max_size: u64,
}

impl<P: AsRef<Path> + Clone> Lsm<P> {
    pub fn new(wal_config: WalConfig<P>, memtable_config: MemtableConfig) -> Self {
        let dir = PathBuf::from(wal_config.log_directory.as_ref());
        Self {
            wal: Wal::new(
                wal_config.id,
                wal_config.clone().log_directory,
                wal_config.max_size,
            ),
            memtable: Memtable::new(memtable_config.id, memtable_config.max_size),
            sstables: Vec::new(),
            working_directory: dir,
            memtable_config,
            wal_config,
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
        if self.memtable.size() > self.memtable_config.max_size {
            eprintln!("Memtable rotation");
            self.rotate_memtable()
        }
    }

    /// Force a rotation of the current [`Memtable`].
    ///
    /// TODO: can we hold the various sealed tables in memory too for reduced I/O
    /// on get(k)?
    pub fn rotate_memtable(&mut self) {
        self.sstables.push(self.memtable.id());
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
                for memtable_id in self.sstables.iter().rev() {
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

    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), &'static str> {
        self.wal.append(WalEntry::Delete { key: key.clone() });
        // Delete the in-memory value if it exists.
        let _ = self.memtable.delete(key.as_slice());
        Ok(())
    }

    pub fn memtable_id(&self) -> u64 {
        self.memtable.id()
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::{
        lsm::{MemtableConfig, WalConfig},
        memtable::MEMTABLE_MAX_SIZE_BYTES,
        wal::WAL_MAX_SIZE_BYTES,
    };

    use super::Lsm;

    #[test]
    fn crud() {
        let dir = TempDir::new("crud").unwrap();
        let w = WalConfig {
            id: 0,
            max_size: WAL_MAX_SIZE_BYTES,
            log_directory: dir.path(),
        };
        let m = MemtableConfig {
            id: 0,
            max_size: MEMTABLE_MAX_SIZE_BYTES,
        };
        let mut lsm = Lsm::new(w, m);

        lsm.put(b"foo".to_vec(), b"bar".to_vec());
        assert_eq!(lsm.memtable.id(), 0);
        assert_eq!(lsm.get(b"foo"), Some(b"bar".to_vec()));
        assert_ne!(lsm.wal.size(), 0);
        let wal_size_after_put = lsm.wal.size();

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

        assert!(lsm.delete(b"foo".to_vec()).is_ok());
        assert!(
            lsm.wal.size() > wal_size_after_put,
            "Deletion should append to the WAL"
        );
    }

    #[test]
    fn compaction() {
        let dir = TempDir::new("compaction").unwrap();
        let w = WalConfig {
            id: 0,
            max_size: WAL_MAX_SIZE_BYTES,
            log_directory: dir.path(),
        };
        let m = MemtableConfig {
            id: 0,
            max_size: 1024, // Force lots of memtable flushes
        };
        let mut lsm = Lsm::new(w, m);

        for i in 0..10000 {
            let key = format!("key{i}").as_bytes().to_vec();
            let value = format!("value{i}").as_bytes().to_vec();
            lsm.put(key, value);
        }

        assert_ne!(lsm.sstables.len(), 0, "SSTables on disk should not be 0");
    }
}
