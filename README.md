# lsm-memtable

Lock-free in-memory write buffer built on an atomic skiplist — the hot path before data hits disk in an LSM tree.

Been exploring lock-free concurrency in Rust. This is the write buffer layer, same class of agave's accounts-db about account state in AccountsDB

## What's here

- `Entry` — the unit of data: key, value, sequence number, value type (put or delete)
- `Node` — skiplist node with a randomized tower of `AtomicPtr`s
- `SkipList` — lock-free insert, point lookup, and sorted iteration

## Status

Work in progress. Open sourcing as I build.
