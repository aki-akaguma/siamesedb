# siamesedb

The simple local key-value store.

## Features

- key-value store.
- in-memory and file store.
- `DbMapString` has keys as utf-8 string.
- `DbMapU64` has keys as u64.
- The value is any bytes included utf-8 string.
- The file store is implemented the basic B-Tree. (no hash and no leaf)
- Small db file size.
- Separated files. (data record file and index file)
- One database has some db-map-string and some db-map-u64.
- minimum support rustc rustc 1.53.0 (53cb7b09b 2021-06-17)

## Compatibility

- Nothing?

## Examples

### Example DbMapString:

```rust
use siamesedb::{DbMapString, DbXxx};

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
    db_map.put_string("key1".to_string(), "value1")?;
    let r = db_map.get_string("key1")?;
    assert_eq!(r, Some("value1".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

### Example DbMapU64:

```rust
use siamesedb::{DbMapU64, DbXxx};

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test2.siamesedb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    let mut db_map = db.db_map_u64("some_list1")?;
    let r = db_map.get_string(&120)?;
    assert_eq!(r, None);
    db_map.put_string(120, "value120")?;
    let r = db_map.get_string(&120)?;
    assert_eq!(r, Some("value120".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

### Example Iterator:

```rust
use siamesedb::{DbMapString, DbMap, DbXxx};

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
    db_map.put_string("key01".into(), "value1").unwrap();
    db_map.put_string("key02".into(), "value2").unwrap();
    db_map.put_string("key03".into(), "value3").unwrap();
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
