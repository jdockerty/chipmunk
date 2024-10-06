#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

use bloomfx::BloomFilter;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::{
    config::{MemtableConfig, WalConfig},
    memtable::Memtable,
    wal::{Wal, WalEntry},
    ChipmunkError,
};

pub struct Lsm {
    /// Write-ahead Log (WAL) which backs the operations performed on the LSM
    /// storage engine.
    wal: Mutex<Wal>,
    /// The configuration which was used to initialise the [`Wal`].
    wal_config: WalConfig,

    /// Currently active [`Memtable`]
    memtable: Memtable,
    /// The configuration which was used to initialise the [`Memtable`].
    memtable_config: MemtableConfig,

    /// IDs of the now immutable memtables
    /// TODO: hold these in memory too, so that I/O is greatly reduced?
    sstables: Mutex<Vec<u64>>,

    bloom: Mutex<BloomFilter<Vec<u8>>>,

    l2_id: AtomicU64,
    l2_files: Mutex<Vec<u64>>,

    working_directory: PathBuf,
}

impl Lsm {
    pub fn new(wal_config: WalConfig, memtable_config: MemtableConfig) -> Self {
        Self {
            wal: Wal::new(
                wal_config.id,
                &wal_config.log_directory,
                wal_config.max_size,
            )
            .into(),
            memtable: Memtable::new(memtable_config.id, memtable_config.max_size),
            sstables: Vec::new().into(),
            l2_id: AtomicU64::new(0),
            l2_files: Vec::new().into(),
            working_directory: wal_config.log_directory.clone(),
            memtable_config,
            wal_config,
            bloom: BloomFilter::new(10000, 2).into(),
        }
    }

    /// Insert an item into the [`Lsm`] tree.
    ///
    /// A [`WalEntry`] is appended into the WAL before proceeding to insert the
    /// key-value pair into an in-memory index, the L0 [`Memtable`].
    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), ChipmunkError> {
        let entry = WalEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };

        {
            self.wal.lock().append(entry)?;
        }

        // Populate the internal bloom filter
        self.bloom_insert(key.clone());

        self.memtable.insert(key, value);
        if self.memtable.size() > self.memtable_config.max_size {
            eprintln!("Memtable rotation");
            self.rotate_memtable();
        }

        // This compaction trigger is not very scientific at the moment.
        if self.l2_files.lock().len() > 3 {
            self.force_compaction();
        }

        Ok(())
    }

    /// Force a rotation of the current [`Memtable`].
    ///
    /// TODO: can we hold the various sealed tables in memory too for reduced I/O
    /// on get(k)?
    pub fn rotate_memtable(&self) {
        self.sstables.lock().push(self.memtable.id());
        self.memtable.flush(self.working_directory.clone());
    }

    /// Remove closed [`Segment`] files. This should only be called when the [`Memtable`]
    /// has been flushed to an [`SSTable`].
    pub fn remove_closed_segments(&self) {
        let mut wal = self.wal.lock();
        wal.closed_segments().iter().for_each(|segment_id| {
            let path = format!("{}/{segment_id}.wal", self.working_directory.display());
            eprintln!("Removing {path}");
            std::fs::remove_file(&path).unwrap()
        });
        wal.clear_segments();
    }

    /// Force a compaction cycle to occur.
    ///
    /// This operates as a full compaction. Taking all data from various sstables
    /// on disk and merging them into new files, removing any tombstones values
    /// to ensure only the most recent data is kept.
    pub fn force_compaction(&self) {
        let mut l2_tree: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        let mut insert_count = 0;
        let mut skip_count = 0;
        {
            let mut sstables = self.sstables.lock();
            for l1_file_id in &*sstables {
                let l1_file = self.working_directory.join(format!("sstable-{l1_file_id}"));
                eprintln!("Compacting L1 file: {l1_file:?}");
                let tree: BTreeMap<Bytes, Option<Bytes>> = Memtable::load(l1_file.clone());

                for (k, v) in tree {
                    if let Some(v) = v {
                        insert_count += 1;
                        eprintln!(
                            "[Compaction] Inserting {} for L2",
                            String::from_utf8_lossy(&k)
                        );
                        // Only insert values which are NOT tombstones
                        l2_tree.insert(k, v);
                    } else {
                        skip_count += 1;
                    }
                }
                std::fs::remove_file(l1_file)
                    .expect("Can always remove existing SSTable after compaction");
            }
            sstables.clear();
        }
        eprintln!("[Compaction] Insertion count: {insert_count}");
        eprintln!("[Compaction] Skip count: {skip_count}");

        let l2_id = self
            .l2_id
            .fetch_add(1, std::sync::atomic::Ordering::Acquire);
        let flush_path = self.working_directory.join(format!("l2-{l2_id}"));
        let l2_data = bincode::serialize(&l2_tree).unwrap();
        std::fs::write(flush_path, l2_data).unwrap();
        self.l2_files.lock().push(l2_id);
    }

    /// Get a value from the LSM-tree.
    ///
    /// This first checks whether the value has passed through the internal
    /// [`BloomFilter`] first.
    /// Afterwards [`Memtable`] is then consulted to search through persisted SSTables
    /// if it exists.
    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        match self.check(key.clone()) {
            // We can return instantly if the value has not passed through the
            // filter.
            false => None,
            true => match self.memtable.get(&key) {
                Some(v) => Some(v.to_vec()),
                None => {
                    for memtable_id in self.sstables.lock().iter().rev() {
                        let memtable = Memtable::load(
                            self.working_directory
                                .join(format!("sstable-{memtable_id}")),
                        );
                        match memtable.get(key.as_slice()) {
                            Some(Some(v)) => return Some(v.to_vec()),
                            None | Some(None) => continue,
                        };
                    }
                    // Exhausted search of entire structure did not find the key, so
                    // it does not exist.
                    None
                }
            },
        }
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<(), ChipmunkError> {
        self.wal
            .lock()
            .append(WalEntry::Delete { key: key.clone() })?;
        self.memtable.delete(key);

        Ok(())
    }

    pub fn memtable_id(&self) -> u64 {
        self.memtable.id()
    }

    /// Restore the LSM-tree by recovering the internal [`Memtable`] and
    /// [`BloomFilter`].
    ///
    /// This works by restoring the WAL and building the memtable from there.
    /// After this, the bloom filter can be populated.
    ///
    /// # Panics
    /// When a restore operation is conducted when the components are not started
    /// from scratch - partial restore is not supported.
    pub fn restore(&mut self) -> Result<(), ChipmunkError> {
        {
            let mut wal = self.wal.lock();
            // Invariant: The restore operation implies that there is currently
            // nothing in any of the components. A partial restore is not supported
            // at the moment.
            assert_eq!(
                wal.size(),
                0,
                "WAL size should be 0 for a restore operation"
            );
            assert_eq!(
                self.memtable.len(),
                0,
                "Memtable can only be restored from scratch"
            );
            assert_eq!(
                self.memtable.size(),
                0,
                "Memtable can only be restored from scratch"
            );

            println!("Restoring WAL");
            wal.restore()?;
            println!("Restoring Memtable");
            for line in wal.lines()? {
                match line {
                    Ok(line) => {
                        let line: WalEntry = bincode::deserialize(line.as_bytes()).unwrap();
                        match line {
                            WalEntry::Put { key, value } => {
                                self.memtable.insert(key, value);
                            }
                            WalEntry::Delete { key } => {
                                self.memtable.delete(key);
                            }
                        }
                    }
                    // Entries which are not valid UTF-8 will be skipped.
                    Err(e) => eprintln!("Invalid entry in WAL: {e}"),
                }
            }
        }

        println!("Restoring bloom filter");
        for (k, v) in &self.memtable {
            if v.is_none() {
                continue;
            } else {
                self.bloom_insert(k.to_vec());
            }
        }

        Ok(())
    }

    /// Internal method for inserting into the [`BloomFilter`].
    fn bloom_insert(&self, key: Vec<u8>) {
        self.bloom.lock().insert(key);
    }

    /// Quickly check whether a key is within the LSM-tree, utilising the internal
    /// bloom filter.
    ///
    /// # Note
    /// As this is **only** a bloom filter check, this can return false positives
    /// but not false negatives.
    fn check(&self, key: Vec<u8>) -> bool {
        // TODO: this could be a read lock (ideally nothing), but the underlying
        // filter takes a mutable reference when it should not.
        self.bloom.lock().check(key)
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use tempdir::TempDir;
    use walkdir::WalkDir;

    use crate::{
        lsm::{MemtableConfig, WalConfig},
        memtable::MEMTABLE_MAX_SIZE_BYTES,
        wal::WAL_MAX_SEGMENT_SIZE_BYTES,
    };

    use super::Lsm;

    // Helper for creating an [`Lsm`] store within a test directory
    fn create_lsm(dir: &TempDir, wal_max_size: u64, memtable_max_size: u64) -> Lsm {
        let w = WalConfig {
            id: 0,
            max_size: wal_max_size,
            log_directory: dir.path().to_path_buf(),
        };
        let m = MemtableConfig {
            id: 0,
            max_size: memtable_max_size,
        };
        Lsm::new(w, m)
    }

    #[test]
    fn crud() {
        let dir = TempDir::new("crud").unwrap();
        let lsm = create_lsm(&dir, WAL_MAX_SEGMENT_SIZE_BYTES, MEMTABLE_MAX_SIZE_BYTES);

        lsm.insert(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        assert_eq!(lsm.memtable.id(), 0);
        assert_eq!(lsm.get(b"foo".to_vec()), Some(b"bar".to_vec()));
        assert_ne!(lsm.wal.lock().size(), 0);
        let wal_size_after_put = lsm.wal.lock().size();

        lsm.rotate_memtable();
        assert_eq!(
            lsm.memtable.id(),
            1,
            "Engine should have a new memtable after flush"
        );
        assert_eq!(
            lsm.get(b"foo".to_vec()),
            Some(b"bar".to_vec()),
            "Value should be found in sstable on disk"
        );

        lsm.delete(b"foo".to_vec()).unwrap();
        assert!(
            lsm.wal.lock().size() > wal_size_after_put,
            "Deletion should append to the WAL"
        );
    }

    #[test]
    fn compaction() {
        let dir = TempDir::new("compaction").unwrap();
        let lsm = create_lsm(&dir, 1024, 1024);

        let dir_size = || {
            let entries = WalkDir::new(dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let mut current_size = dir_size();

        for i in 1..=3 {
            for j in 0..=1000 {
                // The same key value is used, but overwritten on different
                // iterations to force entries which should be discarded.
                let key = format!("key{j}").as_bytes().to_vec();
                let value = format!("value{i}-{j}").as_bytes().to_vec();
                lsm.insert(key.clone(), value).unwrap();

                // Delete every 10th key to accumulate tombstones
                if j % 10 == 0 {
                    lsm.delete(key).unwrap();
                }

                current_size = dir_size();
            }
        }

        let final_size = current_size;
        lsm.force_compaction();
        let post_compaction_size = dir_size();

        assert!(post_compaction_size < final_size);
        assert_ne!(
            lsm.memtable_id(),
            0,
            "Memtable rotation should increment the ID"
        );
        assert_eq!(
            lsm.sstables.lock().len(),
            0,
            "L1 SSTables on disk should be 0 after a full compaction cycle"
        );
    }

    #[test]
    fn bloom() {
        let dir = TempDir::new("bloom").unwrap();
        let lsm = create_lsm(&dir, WAL_MAX_SEGMENT_SIZE_BYTES, MEMTABLE_MAX_SIZE_BYTES);

        lsm.insert(b"foo".to_vec(), b"bar".to_vec()).unwrap();

        assert!(lsm.check(b"foo".to_vec()));
        assert!(!lsm.check(b"baz".to_vec()));

        drop(lsm);

        let mut lsm = create_lsm(&dir, WAL_MAX_SEGMENT_SIZE_BYTES, MEMTABLE_MAX_SIZE_BYTES);
        lsm.restore().unwrap();
        assert!(
            lsm.check(b"foo".to_vec()),
            "The key 'foo' should exist in the filter after the structure was restored."
        );
        assert!(
            !lsm.check(b"baz".to_vec()),
            "A value which does not exist should not appear after restore"
        );
    }

    #[test]
    fn segment_cleanup() {
        let dir = TempDir::new("segment_cleanup").unwrap();
        let lsm = create_lsm(&dir, WAL_MAX_SEGMENT_SIZE_BYTES, MEMTABLE_MAX_SIZE_BYTES);
        for i in 0..100 {
            lsm.insert(
                format!("foo{i}").into_bytes(),
                format!("bar{i}").into_bytes(),
            )
            .unwrap();
        }

        {
            let mut lsm_wal = lsm.wal.lock();
            assert_eq!(lsm_wal.id(), 0);
            for _ in 1..=5 {
                // Force rotations
                lsm_wal.rotate().unwrap();
                lsm.rotate_memtable();
            }
            assert_eq!(lsm_wal.id(), 5);
            assert_eq!(lsm_wal.closed_segments().len(), 5);
        }

        for i in 0..=5 {
            assert!(
                Path::new(&format!("{}/{}.wal", lsm.working_directory.display(), i)).exists(),
                "WAL segments should exist after rotation"
            );
        }
        lsm.remove_closed_segments();
        for i in 0..5 {
            assert!(!Path::new(&format!("{}/{}.wal", lsm.working_directory.display(), i)).exists());
        }
        assert_eq!(
            lsm.wal.lock().closed_segments().len(),
            0,
            "No closed segments remaining after removal",
        );
    }
}
