use std::path::Path;

use crate::{memtable::Memtable, wal::Wal};

pub struct LSM<P: AsRef<Path>> {
    wal: Wal<P>,
    memtable: Memtable,
}
