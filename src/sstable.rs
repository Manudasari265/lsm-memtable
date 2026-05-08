//! File format layout:
//!
//! [ Data Block 0 ][ Data Block 1 ]...[ Data Block N ][ Index Block ][ Footer ]
//!
//! Data Block:
//!   entry_count: u32
//!   for each entry: key_len(u64) | key | value_len(u64) | value | seqno(u64) | value_type(u8)
//!
//! Index Block:
//!   num_entries: u32
//!   for each entry: first_key_len(u64) | first_key | block_offset(u64)
//!
//! Footer (last 16 bytes):
//!   index_offset: u64 | index_length: u64

use crate::entry::Entry;

// const BLOCK_SIZE: usize = 4096;

struct DataBlock {
    entries: Vec<Entry>,
    first_key: Vec<u8>,
}

struct IndexEntry {
    first_key: Vec<u8>,
    block_offset: u64,
}

pub struct SSTable;