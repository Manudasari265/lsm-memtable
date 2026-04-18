use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::atomic::Ordering;

use crate::entry::{Entry, ValueType};
use crate::skiplist::SkipList;

pub struct Memtable {
    pub skiplist: SkipList,
    pub size: AtomicUsize, // total bytes of key + value data stored
    pub next_seqno: AtomicU64, // monotonically increasing, each write gets the next one 
}

impl Memtable {
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let seqno = self.next_seqno.fetch_add(1, Ordering::Relaxed);
        let size = key.len() + value.len();

        let entry = Entry {
            key, 
            value,
            sequence_number: seqno,
            value_type: ValueType::Put,
        };

        self.skiplist.insert(entry);
        self.size.fetch_add(size, Ordering::Relaxed);
    }
}