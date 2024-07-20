// TODO: remove once used in other components
#![allow(dead_code)]

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use bytes::Bytes;

pub const MEMTABLE_MAX_SIZE_BYTES: u64 = 1048576; // 1 MiB

pub struct Memtable {
    tree: BTreeMap<bytes::Bytes, bytes::Bytes>,

    current_size: u64,
    max_size: u64,
}

impl Memtable {
    pub fn new(max_size: u64) -> Self {
        Self {
            tree: BTreeMap::new(),
            current_size: 0,
            max_size,
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
    /// (SSTable).
    pub fn flush(&mut self, flush_dir: PathBuf) {
        let data = bincode::serialize(&self.tree).unwrap();

        self.tree = BTreeMap::new();
        self.current_size = 0;

        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(format!("{}/sstable-1", flush_dir.display()))
            .unwrap();
        f.write(&data).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use tempdir::TempDir;

    use super::{Memtable, MEMTABLE_MAX_SIZE_BYTES};

    const TINY_MEMTABLE_BYTES: u64 = 10;

    #[test]
    fn crud_operations() {
        let mut m = Memtable::new(MEMTABLE_MAX_SIZE_BYTES);
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
        let mut m = Memtable::new(TINY_MEMTABLE_BYTES);
        let flush_dir = TempDir::new("flush").unwrap();
        m.put(b"foo", b"bar");
        assert_eq!(m.current_size, b"foobar".len() as u64);
        m.flush(flush_dir.path().to_path_buf());
        assert_eq!(m.current_size, 0, "New memtable should have size of 0");
        assert!(m.tree.is_empty(), "New memtable should be empty");

        let sstable_file =
            std::fs::File::open(flush_dir.path().join("sstable-1")).expect("Flushed file exists");
        let data: BTreeMap<bytes::Bytes, bytes::Bytes> =
            bincode::deserialize_from(sstable_file).unwrap();
        assert_eq!(
            data.get(b"foo".as_ref()),
            Some(&bytes::Bytes::from_static(b"bar"))
        );
    }
}
