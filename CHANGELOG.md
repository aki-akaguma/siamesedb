TBD
===
Unreleased changes. Release notes have not yet been written.

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
