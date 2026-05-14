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

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::entry::{Entry, ValueType};
use crate::memtable::Memtable;

const BLOCK_SIZE: usize = 4096;

struct DataBlock {
    entries: Vec<Entry>,
    first_key: Vec<u8>,
}

struct IndexEntry {
    first_key: Vec<u8>,
    block_offset: u64,
}

pub struct SSTable;

impl SSTable {
    pub fn flush(memtable: &Memtable, path: &Path) -> Result<PathBuf, std::io::Error> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let mut bytes_written: u64 = 0;

        let mut blocks: Vec<DataBlock> = Vec::new();
        let mut current_block = DataBlock {
            entries: Vec::new(),
            first_key: Vec::new(),
        };
        let mut current_size: usize = 0;

        for entry in memtable.iter() {
            let entry_size = 8 + entry.key.len() + 8 + entry.value.len() + 8 + 1;

            if current_size + entry_size > BLOCK_SIZE && !current_block.entries.is_empty() {
                blocks.push(current_block);
                current_block = DataBlock {
                    entries: Vec::new(),
                    first_key: Vec::new(),
                };
                current_size = 0;
            }

            if current_block.first_key.is_empty() {
                current_block.first_key = entry.key.clone();
            }

            current_block.entries.push(entry.clone());
            current_size += entry_size;
        }

        if !current_block.entries.is_empty() {
            blocks.push(current_block);
        }

        let mut index_entries: Vec<IndexEntry> = Vec::new();

        for block in &blocks {
            let block_offset = bytes_written;

            let count = block.entries.len() as u32;
            writer.write_all(&count.to_le_bytes())?;
            bytes_written += 4;

            for entry in &block.entries {
                writer.write_all(&(entry.key.len() as u64).to_le_bytes())?;
                writer.write_all(&entry.key)?;
                writer.write_all(&(entry.value.len() as u64).to_le_bytes())?;
                writer.write_all(&entry.value)?;
                writer.write_all(&entry.sequence_number.to_le_bytes())?;
                let vtype: u8 = match entry.value_type {
                    ValueType::Put => 0,
                    ValueType::Delete => 1,
                };
                writer.write_all(&[vtype])?;

                bytes_written += 8 + entry.key.len() as u64 + 8 + entry.value.len() as u64 + 8 + 1;
            }

            index_entries.push(IndexEntry {
                first_key: block.first_key.clone(),
                block_offset,
            });
        }

        //? write index block
        let index_offset = bytes_written;

        writer.write_all(&(index_entries.len() as u32).to_le_bytes())?;
        bytes_written += 4;

        for idx in &index_entries {
            writer.write_all(&(idx.first_key.len() as u64).to_le_bytes())?;
            writer.write_all(&idx.first_key)?;
            writer.write_all(&idx.block_offset.to_le_bytes())?;
            bytes_written += 8 + idx.first_key.len() as u64 + 8;
        }

        let index_length = bytes_written - index_offset;

        //? write footer (last 16 bytes)
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&index_length.to_le_bytes())?;

        writer.flush()?;
        Ok(path.to_path_buf())
    }

    pub fn get(path: &Path, target_key: &[u8]) -> Result<Option<Entry>, std::io::Error> {
        let mut file = File::open(path)?;

        file.seek(SeekFrom::End(-16))?;
        let mut footer = [0u8; 16];
        file.read_exact(&mut footer)?;
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());

        file.seek(SeekFrom::Start(index_offset))?;
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let num_entries = u32::from_le_bytes(buf4) as usize;

        let mut index_entries: Vec<IndexEntry> = Vec::with_capacity(num_entries);
        let mut buf8 = [0u8; 8];

        for _ in 0..num_entries {
            file.read_exact(&mut buf8)?;
            let key_len = u64::from_le_bytes(buf8) as usize;
            let mut first_key = vec![0u8; key_len];
            file.read_exact(&mut first_key)?;
            file.read_exact(&mut buf8)?;
            let block_offset = u64::from_le_bytes(buf8);
            index_entries.push(IndexEntry {
                first_key,
                block_offset,
            });
        }

        let block_offset = index_entries
            .iter()
            .rfind(|e| e.first_key.as_slice() <= target_key)
            .map(|e| e.block_offset);

        let block_offset = match block_offset {
            Some(o) => o,
            None => return Ok(None),
        };

        file.seek(SeekFrom::Start(block_offset))?;
        file.read_exact(&mut buf4)?;
        let entry_count = u32::from_le_bytes(buf4) as usize;

        for _ in 0..entry_count {
            file.read_exact(&mut buf8)?;
            let key_len = u64::from_le_bytes(buf8) as usize;
            let mut key = vec![0u8; key_len];
            file.read_exact(&mut key)?;

            file.read_exact(&mut buf8)?;
            let value_len = u64::from_le_bytes(buf8) as usize;
            let mut value = vec![0u8; value_len];
            file.read_exact(&mut value)?;

            file.read_exact(&mut buf8)?;
            let seqno = u64::from_le_bytes(buf8);

            let mut vtype_buf = [0u8; 1];
            file.read_exact(&mut vtype_buf)?;
            let value_type = if vtype_buf[0] == 0 {
                ValueType::Put
            } else {
                ValueType::Delete
            };

            if key == target_key {
                return Ok(Some(Entry {
                    key,
                    value,
                    sequence_number: seqno,
                    value_type,
                }));
            }
        }

        Ok(None)
    }

    pub fn scan(path: &Path, start: &[u8], end: &[u8]) -> Result<Vec<Entry>, std::io::Error> {
        let mut file = File::open(path)?;

        file.seek(SeekFrom::End(-16))?;
        let mut footer = [0u8; 16];
        file.read_exact(&mut footer)?;
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());

        file.seek(SeekFrom::Start(index_offset))?;
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let num_entries = u32::from_le_bytes(buf4) as usize;

        let mut index_entries: Vec<IndexEntry> = Vec::with_capacity(num_entries);
        let mut buf8 = [0u8; 8];

        for _ in 0..num_entries {
            file.read_exact(&mut buf8)?;
            let key_len = u64::from_le_bytes(buf8) as usize;
            let mut first_key = vec![0u8; key_len];
            file.read_exact(&mut first_key)?;
            file.read_exact(&mut buf8)?;
            let block_offset = u64::from_le_bytes(buf8);
            index_entries.push(IndexEntry {
                first_key,
                block_offset,
            });
        }

        //? find all candidate blocks: first_key <= end (may contain keys >= start)
        let candidate_offsets: Vec<u64> = index_entries
            .iter()
            .filter(|e| e.first_key.as_slice() <= end)
            .map(|e| e.block_offset)
            .collect();

        let mut results: Vec<Entry> = Vec::new();

        for block_offset in candidate_offsets {
            file.seek(SeekFrom::Start(block_offset))?;
            file.read_exact(&mut buf4)?;
            let entry_count = u32::from_le_bytes(buf4) as usize;

            for _ in 0..entry_count {
                file.read_exact(&mut buf8)?;
                let key_len = u64::from_le_bytes(buf8) as usize;
                let mut key = vec![0u8; key_len];
                file.read_exact(&mut key)?;

                file.read_exact(&mut buf8)?;
                let value_len = u64::from_le_bytes(buf8) as usize;
                let mut value = vec![0u8; value_len];
                file.read_exact(&mut value)?;

                file.read_exact(&mut buf8)?;
                let seqno = u64::from_le_bytes(buf8);

                let mut vtype_buf = [0u8; 1];
                file.read_exact(&mut vtype_buf)?;
                let value_type = if vtype_buf[0] == 0 {
                    ValueType::Put
                } else {
                    ValueType::Delete
                };

                if key.as_slice() >= start && key.as_slice() <= end {
                    results.push(Entry {
                        key,
                        value,
                        sequence_number: seqno,
                        value_type,
                    });
                }
            }
        }

        Ok(results)
    }
}
