// TODO: remove once used in other components
#![allow(dead_code)]

use std::collections::BTreeMap;

use bytes::Bytes;

pub const MEMTABLE_MAX_SIZE_BYTES: u64 = 1048576; // 1 MiB

pub struct Memtable {
    tree: BTreeMap<bytes::Bytes, bytes::Bytes>,

    max_size: u64,
}

impl Memtable {
    pub fn new(max_size: u64) -> Self {
        Self {
            tree: BTreeMap::new(),
            max_size,
        }
    }

    pub fn put(&mut self, key: &'static [u8], value: &'static [u8]) {
        let key = Bytes::from_static(key);
        let value = Bytes::from_static(value);
        self.tree.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.tree.get(key) {
            Some(v) => Some(v),
            None => None,
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), &'static str> {
        match self.tree.remove(key) {
            Some(_) => Ok(()),
            None => Err("cannot remove nonexistent key"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Memtable, MEMTABLE_MAX_SIZE_BYTES};

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
}
