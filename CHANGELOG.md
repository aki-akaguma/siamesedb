TBD: siamesedb
===
Unreleased changes. Release notes have not yet been written.

* changes many many codes for performance.
* removes `record_cache` from features.
* removes `kc-lfu` and `kc-lru` from features.
* adds `get_k8()` and `put_k8()`.
* adds `htx-file` for supporting hash table index cache.
* change separates dat-file to key-file and val-file.

0.1.14 (2021-12-23)
=====

* adds `iter()`, `iter_mut()` into `trait DbMap`.
* adds a basic iterator.
* adds `piece.rs` source code.

0.1.13 (2021-12-20)
=====

* remove `kc_btree` and `kc_hash` from features.
* remove `offidx_btreemap` from features.
* remove `buf_idx_btreemap` from features.
* adds `kc_print_hits` and `nc_print_hits`  to features.
* adds key_length_stats() and value_length_stats() into trait CheckFileDbMap.
* removes `node_dm32` from features and source codes.
* changes node offset and node size to 8 bytes boundary.
* changes record size and record offset to 8 bytes boundary.
* adds more REC_SIZE_ARY of dat.

0.1.12 (2021-12-13)
=====

* refactoring key type source codes.
* rename `trait FileDbXxxInnerKT` to `trait DbXxxKeyType`.
* many performance tunings.
* adds `read_fill_buffer()`.
* adds DbMapBytes with `Vec<u8>` KEY.
* adds `node_dm32` to features.

0.1.11 (2021-12-05)
=====

* refactoring node.
* adds `FileBufSizeParam`.
* adds `buf_auto_buf_size` to features
* changes max node slots for best performance.

0.1.10 (2021-11-26)
=====

* adds `buf_overf_rem_all` to default features.
* bug fix: a node size is calculated small.
* adds `nc_lru` to features
* adds `kc_lru` to features
* adds bulk_put() method that has pre sort function.

0.1.9 (2021-11-21)
=====

* source code refatoring.
* adds record_cache to features.
* revives feature `"key_cache"`.
* rewrites key parameter of put() and put_string().
* rewrites trait DbMapU64 and DbMapString with trait DbXxx<KT>.

0.1.8 (2021-11-17)
=====

* adds `flush()` method into `pub trait DbXxx<KT>`.
* source code refactoring.

0.1.7 (2021-11-16)
=====

* fix bugs: node_cache and write_node()

0.1.6 (2021-11-16)
=====

* fix bugs: writing beyond the size limit in dat_file_pop_free_list()

0.1.5 (2021-11-12)
=====

* fix bugs: If the key is empty, it will result in an error.
* adds more test

0.1.4 (2021-11-11)
=====

* renames FileDbMap to FileDbMapString, FileDbList to FileDbMapU64.
* separates crates: vu64, rabuf.

0.1.3 (2021-11-09)
=====

* removes VarCursor. so slow.
* adds read_inplace() into buf.rs for the fast.
* fix some bugs.
* adds minimum support rustc version into README.

0.1.2 (2021-11-04)
=====

* adds node cache for read and write
* changes file header signature: `siamdb`
* removes unused enum KeyType from src/filedb/mod.rs
* adds RecordSizeStats into src/filedb/mod.rs
* adds read_one_byte() and read_exact_small() into BufFile for the fast.
* fix: seek over the end.

0.1.1 (2021-10-30)
=====

* changes package name: shamdb to siamesedb.
* adds sementic types.
* changes the node size and the record size are variables.
* adds vu64.rs and vu64_io.rs.
* adds inner/dbxxx.rs.
* writes a lot of code but it's still incomplete.

0.1.0 (2021-09-23)
=====

first commit
