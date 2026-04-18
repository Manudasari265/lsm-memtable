use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use crate::consts::MAX_SIZE;
use crate::entry::{Entry, ValueType};
use crate::skiplist::SkipList;

pub struct Memtable {
    pub skiplist: SkipList,
    pub size: AtomicUsize,     // total bytes of key + value data stored
    pub next_seqno: AtomicU64, // monotonically increasing, each write gets the next one
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            skiplist: SkipList::new(),
            size: AtomicUsize::new(0),
            next_seqno: AtomicU64::new(0),
        }
    }

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

    pub fn delete(&self, key: Vec<u8>) {
        let seqno = self.next_seqno.fetch_add(1, Ordering::Relaxed);
        let size = key.len();

        let entry = Entry {
            key,
            value: Vec::new(), // for delete the value is empty
            sequence_number: seqno,
            value_type: ValueType::Delete,
        };

        self.skiplist.insert(entry);
        self.size.fetch_add(size, Ordering::Relaxed);
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.skiplist.get(key)
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.size() >= MAX_SIZE
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.skiplist.iter()
    }
}
