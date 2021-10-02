# shamdb

The simple local key-value store.

## Features

- key-value store.
- in-memory and file store.
- DbMap has keys as utf-8 string.
- DbList has keys as u64.
- The value is any bytes included utf-8 string.
- Small db file size.

## Compatibility

- Nothing?

## Examples

### Example DbMap:

```
use shamdb::{ShamDb, DbMap};

let db = ShamDb::open_file("test1.shamdb").unwrap();
let db_map = db.db_map("some_map1");
let r = db_map.get("key1");
assert_eq!(r, None);
db_map.put("key1", "value1");
let r = db_map.get("key1");
assert_eq!(r, Some("value1"));
db_map.sync();
```

### Example DbList:

```
use shamdb::{ShamDb, DbList};

let db = ShamDb::open_file("test1.shamdb").unwrap();
let db_list = db.db_list("some_list1");
let r = db_list.get(120);
assert_eq!(r, None);
db_list.put(120, "value120");
let r = db_list.get(120);
assert_eq!(r, Some("value120"));
db_list.sync();
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
