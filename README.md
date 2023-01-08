# siamesedb

[![crate][crate-image]][crate-link]
[![Docs][docs-image]][docs-link]
![Rust Version][rustc-image]
![Apache2/MIT licensed][license-image]

The simple local key-value store.

## Features

- key-value store.
- in-memory and file store.
- `DbMapDbString` has keys as utf-8 string.
- `DbMapDbInt` has keys as u64.
- `DbMapDbBytes` has keys as Vec<u8>.
- The value is any bytes included utf-8 string.
- The file store is implemented the basic B-Tree. (no leaf)
- The file store is included the htx file that is hash cache table for performance.
- Small db file size.
- Separated files. (key file, value file, index file and htx file)
- One database has some db-map-string and some db-map-int and some db-map-bytes.
- Swiss army knife with easy-to-use and good performance
- minimum support rustc 1.56.1 (59eed8a2a 2021-11-01)

## Compatibility

- Nothing?

## Todo

- [ ] more performance
- [ ] DB lock as support for multi-process-safe

## Low priority todo

- [ ] transaction support that handles multiple key-space at a time.
- [ ] thread-safe support
- [ ] non db lock multi-process-safe support

## Examples

### Example DbMapDbString:

```rust
use siamesedb::{DbMapDbString, DbXxx, DbXxxBase, DbXxxObjectSafe};

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test1.siamesedb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    // create or get db map
    let mut db_map = db.db_map_string("some_map1")?;
    //
    let r = db_map.get_string("key1")?;
    assert_eq!(r, None);
    db_map.put_string("key1", "value1")?;
    let r = db_map.get_string("key1")?;
    assert_eq!(r, Some("value1".into()));
    db_map.sync_data()?;
    Ok(())
}
```

### Example DbMapDbInt:

```rust
use siamesedb::{DbMapDbInt, DbXxx, DbXxxBase, DbXxxObjectSafe};

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test2.siamesedb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    let mut db_map = db.db_map_int("some_list1")?;
    let r = db_map.get_string(&120)?;
    assert_eq!(r, None);
    db_map.put_string(&120, "value120")?;
    let r = db_map.get_string(&120)?;
    assert_eq!(r, Some("value120".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

### Example Iterator:

```rust
use siamesedb::{DbMapDbString, DbMap, DbXxx, DbXxxBase, DbXxxObjectSafe};

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test3.siamesedb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    // create or get db map
    let mut db_map = db.db_map_string("some_map1")?;
    //
    // insert
    db_map.put_string("key01", "value1").unwrap();
    db_map.put_string("key02", "value2").unwrap();
    db_map.put_string("key03", "value3").unwrap();
    //
    // iterator
    let mut iter = db_map.iter();
    assert_eq!(iter.next(), Some(("key01".into(), "value1".into())));
    assert_eq!(iter.next(), Some(("key02".into(), "value2".into())));
    assert_eq!(iter.next(), Some(("key03".into(), "value3".into())));
    assert_eq!(iter.next(), None);
    //
    db_map.sync_data()?;
    Ok(())
}
```

# Changelogs

[This crate's changelog here.](https://github.com/aki-akaguma/siamesedb/blob/main/CHANGELOG.md)

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

at your option.

[//]: # (badges)

[crate-image]: https://img.shields.io/crates/v/siamesedb.svg
[crate-link]: https://crates.io/crates/siamesedb
[docs-image]: https://docs.rs/siamesedb/badge.svg
[docs-link]: https://docs.rs/siamesedb/
[rustc-image]: https://img.shields.io/badge/rustc-1.56+-blue.svg
[license-image]: https://img.shields.io/badge/license-Apache2.0/MIT-blue.svg
