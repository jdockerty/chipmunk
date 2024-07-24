#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::BufReader;
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
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry = WalEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };

        self.wal.append(entry);

        self.memtable.insert(key, value);
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

    /// Force a compaction cycle to occur.
    ///
    /// This operates as a full compaction. Taking all data from various sstables
    /// on disk and merging them into new files, removing any tombstones values
    /// to ensure only the most recent data is kept.
    pub fn force_compaction(&mut self) {
        let mut l2_tree: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        for l1_file_id in &self.sstables {
            let tree: BTreeMap<Bytes, Option<Bytes>> = Memtable::load(
                self.working_directory
                    .join(format!("sstable-{l1_file_id}"))
                    .clone(),
            );

            for (k, v) in tree {
                match v {
                    Some(v) => {
                        // Only insert values which are NOT tombstones
                        l2_tree.insert(k, v);
                    }
                    None => {}
                }
            }
            std::fs::remove_file(self.working_directory.join(format!("{l1_file_id}")))
                .expect("Can always remove existing SSTable after compaction");
        }
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
                    let memtable = Memtable::load(
                        self.working_directory
                            .join(format!("sstable-{memtable_id}")),
                    );
                    match memtable.get(key) {
                        Some(v) => match v {
                            Some(v) => return Some(v.to_vec()),
                            None => continue,
                        },
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
        let _ = self.memtable.delete(key);
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

        lsm.insert(b"foo".to_vec(), b"bar".to_vec());
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

    //#[test]
    //fn compaction() {
    //    let dir = TempDir::new("compaction").unwrap();
    //    let w = WalConfig {
    //        id: 0,
    //        max_size: 1024,
    //        log_directory: dir.path(),
    //    };
    //    let m = MemtableConfig {
    //        id: 0,
    //        max_size: 1024,
    //    };
    //    let mut lsm = Lsm::new(w, m);

    //    let mut current_size = dir.path().metadata().unwrap().len();
    //    let max = 10_000;
    //    for j in 0..=max {
    //        let key = format!("key{j}").as_bytes().to_vec();
    //        let value = format!("value{j}").as_bytes().to_vec();
    //        if j % 10 == 0 {
    //            lsm.insert(key, format!("value0").as_bytes().to_vec());
    //        } else {
    //            lsm.insert(key, value);
    //        }

    //        let new_size = dir.path().metadata().unwrap().len();
    //        if new_size >= current_size {
    //            if j == max {
    //                panic!("No compaction");
    //            }
    //            current_size = new_size;
    //        } else {
    //            println!("Compaction!");
    //        }
    //    }
    //    assert_ne!(
    //        lsm.memtable_id(),
    //        0,
    //        "Memtable rotation should increment the ID"
    //    );
    //    assert_ne!(lsm.sstables.len(), 0, "SSTables on disk should not be 0");
    //}
}
