# Changelog: siamesedb

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] *
### Changed
* reformat `CHANGELOG.md`

### Fixed
* clippy: this let-binding has unit value
* clippy: this block may be rewritten with the `?` operator
* clippy: format_push_string


## [0.1.18] (2022-06-13)
### Changed
* changes to edition 2021

## [0.1.17] (2022-02-07)
### Changed
* many.

## [0.1.16] (2022-01-18)
### Added
* `myhasher` to features.
* `prepare()` to `VarFile`.
* `buf_hash_turbo` to feature. important for performance.

### Changed
* organize source code.
* change `put()` to `put<'a, Q>()`
* rename `Record` to `Piece`.

## [0.1.15] (2022-01-07)
### Added
* `DbInt` and `DbString` to key types.
* `get_kt()`, `put_kt()` and `del_kt()`.
* `htx-file` for supporting hash table index cache.

### Changed
* change many many codes for performance.
* separate `DbXxx` to `DbXxx`, `DbXxxObjectSafe` and `DbXxxBase`.
* rename `DbXxxKeyType` to `DbMapKeyType`.
* change separates dat-file to key-file and val-file.

### Removed
* `key_cache` and `record_cache` from features.
* `kc-lfu` and `kc-lru` from features.

## [0.1.14] (2021-12-23)
### Added
* `iter()`, `iter_mut()` into `trait DbMap`.
* a basic iterator.
* `piece.rs` source code.

## [0.1.13] (2021-12-20)
### Added
* `kc_print_hits` and `nc_print_hits`  to features.
* key_length_stats() and value_length_stats() into trait CheckFileDbMap.
* more REC_SIZE_ARY of dat.

### Changed
* changes node offset and node size to 8 bytes boundary.
* changes record size and record offset to 8 bytes boundary.

### Removed
* `kc_btree` and `kc_hash` from features.
* `offidx_btreemap` from features.
* `buf_idx_btreemap` from features.
* `node_dm32` from features and source codes.

## [0.1.12] (2021-12-13)
### Added
* `read_fill_buffer()`.
* DbMapBytes with `Vec<u8>` KEY.
* `node_dm32` to features.

### Changed
* refactoring key type source codes.
* rename `trait FileDbXxxInnerKT` to `trait DbXxxKeyType`.
* many performance tunings.

## [0.1.11] (2021-12-05)
### Added
* `FileBufSizeParam`.
* `buf_auto_buf_size` to features

### Changed
* refactoring node.
* changes max node slots for best performance.

## [0.1.10] (2021-11-26)
### Added
* `buf_overf_rem_all` to default features.
* `nc_lru` to features
* `kc_lru` to features
* bulk_put() method that has pre sort function.

### Fixed
* bug: a node size is calculated small.

## [0.1.9] (2021-11-21)
### Added
* record_cache to features.

### Changed
* source code refatoring.
* revives feature `"key_cache"`.
* rewrites key parameter of put() and put_string().
* rewrites trait DbMapU64 and DbMapString with trait DbXxx<KT>.

## [0.1.8] (2021-11-17)
### Added
* `flush()` method into `pub trait DbXxx<KT>`.

### Changed
* source code refactoring.

## [0.1.7] (2021-11-16)
### Fixed
* bugs: node_cache and write_node()

## [0.1.6] (2021-11-16)
### Fixed
* bugs: writing beyond the size limit in dat_file_pop_free_list()

## [0.1.5] (2021-11-12)
### Added
* more test

### Fixed
* bugs: If the key is empty, it will result in an error.

## [0.1.4] (2021-11-11)
### Changed
* rename FileDbMap to FileDbMapString, FileDbList to FileDbMapU64.
* separate crates: vu64, rabuf.

## [0.1.3] (2021-11-09)
### Added
* read_inplace() into buf.rs for the fast.
* minimum support rustc version into README.

### Removed
* removes VarCursor. so slow.

### Fixed
* some bugs.

## [0.1.2] (2021-11-04)
### Added
* node cache for read and write
* RecordSizeStats into src/filedb/mod.rs
* read_one_byte() and read_exact_small() into BufFile for the fast.

### Changed
* change file header signature: `siamdb`

### Removed
* remove unused enum KeyType from src/filedb/mod.rs

### Fixed
* fix: seek over the end.

## [0.1.1] (2021-10-30)
### Added
* sementic types.
* vu64.rs and vu64_io.rs.
* inner/dbxxx.rs.

### Changed
* change package name: shamdb to siamesedb.
* change the node size and the record size are variables.
* writes a lot of code but it's still incomplete.

## [0.1.0] (2021-09-23)
* first commit

[Unreleased]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.18..HEAD
[0.1.18]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.17..v0.1.18
[0.1.17]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.16..v0.1.17
[0.1.16]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.15..v0.1.16
[0.1.15]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.14..v0.1.15
[0.1.14]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.13..v0.1.14
[0.1.13]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.12..v0.1.13
[0.1.12]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.11..v0.1.12
[0.1.11]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.10..v0.1.11
[0.1.10]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.9..v0.1.10
[0.1.9]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.8..v0.1.9
[0.1.8]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.7..v0.1.8
[0.1.7]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.6..v0.1.7
[0.1.6]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.5..v0.1.6
[0.1.5]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.4..v0.1.5
[0.1.4]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.3..v0.1.4
[0.1.3]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.2..v0.1.3
[0.1.2]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.1..v0.1.2
[0.1.1]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.0..v0.1.1
[0.1.0]: https://github.com/aki-akaguma/siamesedb/releases/tag/v0.1.0
