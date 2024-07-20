use std::path::Path;

use crate::{memtable::Memtable, wal::Wal};

#[allow(dead_code)]
pub struct Lsm<P: AsRef<Path>> {
    wal: Wal<P>,
    memtable: Memtable,
}
