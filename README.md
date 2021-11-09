# siamesedb

The simple local key-value store.

## Features

- key-value store.
- in-memory and file store.
- DbMap has keys as utf-8 string.
- DbList has keys as u64.
- The value is any bytes included utf-8 string.
- The file store is implemented the basic B-Tree. (no hash and no leaf)
- Small db file size.
- Separated files. (data record file and index file)
- One database has some db-maps and some db-lists.
- minimum support rustc 1.54.0 (a178d0322 2021-07-26)

## Compatibility

- Nothing?

## Examples

### Example DbMap:

```rust
use siamesedb::DbMap;

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test1.shamdb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    // create or get db map
    let mut db_map = db.db_map("some_map1")?;
    //
    let r = db_map.get_string("key1")?;
    assert_eq!(r, None);
    db_map.put_string("key1", "value1")?;
    let r = db_map.get_string("key1")?;
    assert_eq!(r, Some("value1".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

### Example DbList:

```rust
use siamesedb::DbList;

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test2.shamdb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    let mut db_list = db.db_list("some_list1")?;
    let r = db_list.get_string(120)?;
    assert_eq!(r, None);
    db_list.put_string(120, "value120")?;
    let r = db_list.get_string(120)?;
    assert_eq!(r, Some("value120".to_string()));
    db_list.sync_data()?;
    Ok(())
}
```


# Changelogs

[This crate's changelog here.](https://github.com/aki-akaguma/shamdb/blob/main/CHANGELOG.md)

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)

at your option.
