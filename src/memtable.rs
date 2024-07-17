// TODO: remove once used in other components
#![allow(dead_code)]

use std::collections::BTreeMap;

pub struct Memtable {
    // TODO: contained types
    tree: BTreeMap<(), ()>,
}
