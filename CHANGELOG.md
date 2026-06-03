# Changelog: siamesedb
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- More tests

### Changed
- Rust version to "1.65.0"

## [0.1.23] - 2024-06-18

### Fixed
- Clippy warning: `unexpected_cfgs`

## [0.1.22] - 2024-06-18

### Changed
- Rename `config` to `config.toml`

### Fixed
- Clippy warnings: `dead_code`, `suspicious_open_options`, and `empty_docs`
- Clippy warning: `four_forward_slashes`

## [0.1.21] - 2023-02-13

### Added
- `MIRIFLAGS=-Zmiri-disable-isolation` on `cargo miri`
- Miri support for tests

### Changed
- Rust version from "1.56.0" to "1.58.0"
- Refactor `Makefile`

### Removed
- `COPYING` file

### Fixed
- Issues in `LICENSE-APACHE` and `LICENSE-MIT`
- Clippy warnings: `redundant_field_names`, `unnecessary_unwrap`, and `needless_bool`
- Clippy warning: `unnecessary_unwrap`

## [0.1.20] - 2023-01-31

### Added
- `.github/workflows/test-ubuntu.yml`, `.github/workflows/test-macos.yml`, and `.github/workflows/test-windows.yml`
- Test status badges to `README.tpl`
- `rust-version = "1.56.0"` to `Cargo.toml`

### Fixed
- Clippy warnings: `seek_from_current` and `explicit_counter_loop`
- Bypass `test_size_of()` on Windows

## [0.1.19] - 2023-01-10

### Added
- Badges to `README.tpl`

### Changed
- Reformat `CHANGELOG.md`

### Fixed
- Clippy warning: let-binding has unit value
- Clippy warning: block may be rewritten with `?` operator
- Clippy warning: `format_push_string`
- Clippy warnings: `bool_to_int_with_if` and `needless_borrow`

## [0.1.18] - 2022-06-13

### Changed
- Edition to 2021

## [0.1.17] - 2022-02-07

### Changed
- Various internal updates

## [0.1.16] - 2022-01-18

### Added
- `myhasher` feature
- `prepare()` method to `VarFile`
- `buf_hash_turbo` feature for performance

### Changed
- Organize source code
- `put()` to `put<'a, Q>()`
- Rename `Record` to `Piece`

## [0.1.15] - 2022-01-07

### Added
- `DbInt` and `DbString` key types
- `get_kt()`, `put_kt()`, and `del_kt()` methods
- `htx-file` for supporting hash table index cache

### Changed
- Improve performance across codebase
- Split `DbXxx` into `DbXxx`, `DbXxxObjectSafe`, and `DbXxxBase`
- Rename `DbXxxKeyType` to `DbMapKeyType`
- Separate `dat-file` into `key-file` and `val-file`

### Removed
- `key_cache` and `record_cache` features
- `kc-lfu` and `kc-lru` features

## [0.1.14] - 2021-12-23

### Added
- `iter()` and `iter_mut()` to `trait DbMap`
- Basic iterator
- `piece.rs` source code

## [0.1.13] - 2021-12-20

### Added
- `kc_print_hits` and `nc_print_hits` features
- `key_length_stats()` and `value_length_stats()` to `trait CheckFileDbMap`
- Additional `REC_SIZE_ARY` for dat

### Changed
- Node offset and size to 8-byte boundary
- Record size and offset to 8-byte boundary

### Removed
- `kc_btree` and `kc_hash` features
- `offidx_btreemap` feature
- `buf_idx_btreemap` feature
- `node_dm32` feature and source code

## [0.1.12] - 2021-12-13

### Added
- `read_fill_buffer()` method
- `DbMapBytes` with `Vec<u8>` key
- `node_dm32` feature

### Changed
- Refactor key type source code
- Rename `trait FileDbXxxInnerKT` to `trait DbXxxKeyType`
- Various performance optimizations

## [0.1.11] - 2021-12-05

### Added
- `FileBufSizeParam`
- `buf_auto_buf_size` feature

### Changed
- Refactor node
- Max node slots for optimal performance

## [0.1.10] - 2021-11-26

### Added
- `buf_overf_rem_all` to default features
- `nc_lru` feature
- `kc_lru` feature
- `bulk_put()` method with pre-sort functionality

### Fixed
- Correct node size calculation

## [0.1.9] - 2021-11-21

### Added
- `record_cache` feature

### Changed
- Refactor source code
- Restore `"key_cache"` feature
- Rewrite key parameter of `put()` and `put_string()`
- Rewrite `trait DbMapU64` and `trait DbMapString` with `trait DbXxx<KT>`

## [0.1.8] - 2021-11-17

### Added
- `flush()` method to `pub trait DbXxx<KT>`

### Changed
- Refactor source code

## [0.1.7] - 2021-11-16

### Fixed
- Issues in `node_cache` and `write_node()`

## [0.1.6] - 2021-11-16

### Fixed
- Write beyond size limit in `dat_file_pop_free_list()`

## [0.1.5] - 2021-11-12

### Added
- More tests

### Fixed
- Error when key is empty

## [0.1.4] - 2021-11-11

### Changed
- Rename `FileDbMap` to `FileDbMapString`, and `FileDbList` to `FileDbMapU64`
- Separate crates `vu64` and `rabuf`

## [0.1.3] - 2021-11-09

### Added
- `read_inplace()` to `buf.rs` for performance
- Minimum supported Rust version to `README.md`

### Removed
- `VarCursor` due to poor performance

### Fixed
- Various bugs

## [0.1.2] - 2021-11-04

### Added
- Node cache for read and write operations
- `RecordSizeStats` to `src/filedb/mod.rs`
- `read_one_byte()` and `read_exact_small()` to `BufFile` for performance

### Changed
- File header signature to `siamdb`

### Removed
- Unused `enum KeyType` from `src/filedb/mod.rs`

### Fixed
- Seek over the end of file

## [0.1.1] - 2021-10-30

### Added
- Semantic types
- `vu64.rs` and `vu64_io.rs`
- `inner/dbxxx.rs`

### Changed
- Package name from `shamdb` to `siamesedb`
- Variable node and record sizes
- Initial development and incomplete features

## [0.1.0] - 2021-09-23

### Added
- First commit

[Unreleased]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.23..HEAD
[0.1.23]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.22..v0.1.23
[0.1.22]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.21..v0.1.22
[0.1.21]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.20..v0.1.21
[0.1.20]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.19..v0.1.20
[0.1.19]: https://github.com/aki-akaguma/siamesedb/compare/v0.1.18..v0.1.19
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
