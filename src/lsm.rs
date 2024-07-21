#![allow(dead_code)]

use std::path::Path;

use crate::{
    memtable::{Memtable, MEMTABLE_MAX_SIZE_BYTES},
    wal::{Wal, WalEntry},
};

pub struct Lsm<P: AsRef<Path>> {
    wal: Wal<P>,
    memtable: Memtable,
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
    pub fn new(wal_config: WalConfig<P>, memtable_config: MemtableConfig) -> Self {
        Self {
            wal: Wal::new(wal_config.id, wal_config.log_directory, wal_config.max_size),
            memtable: Memtable::new(memtable_config.id, memtable_config.max_size),
        }
    }

    /// Put an item into the [`Lsm`] tree.
    ///
    /// A [`WalEntry`] is appended into the WAL before proceeding to insert the
    /// key-value pair into an in-memory index, the L0 [`Memtable`].
    pub fn put(&mut self, key: &'static [u8], value: &'static [u8]) {
        let entry = WalEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        self.wal.append(entry);

        self.memtable.put(key, value);
        if self.memtable.size() >= MEMTABLE_MAX_SIZE_BYTES {
            self.memtable.flush("".into());
        }
    }

    /// Get a key and corresponding value from the LSM-tree.
    ///
    /// This first checks whether the value exists in the [`Memtable`] and continues
    /// the search through persisted SSTables if it does not. After exhausting
    /// all options, the value does not exist.
    ///
    /// TODO: Improve retrieval through use of a bloom filter
    pub fn get(&mut self, key: &'static [u8]) -> Option<&[u8]> {
        match self.memtable.get(&key) {
            Some(v) => Some(v),
            None => {
                // TODO: search through sstables
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;

    use crate::{memtable::MEMTABLE_MAX_SIZE_BYTES, wal::WAL_MAX_SIZE_BYTES};

    use super::{Lsm, MemtableConfig, WalConfig};

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

        lsm.put(b"foo", b"bar");

        assert!(lsm.get(b"foo").is_some());
        assert_eq!(lsm.get(b"foo").unwrap(), b"bar");
    }
}
