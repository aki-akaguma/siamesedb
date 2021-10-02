/*!
The simple local key-value store.

# Features

- key-value store.
- in-memory and file store.
- DbMap has keys as utf-8 string.
- DbList has keys as u64.
- The value is any bytes included utf-8 string.
- Small db file size.

# Compatibility

- Nothing?

# Examples

## Example DbMap:

```text
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

## Example DbList:

```text
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

*/
use std::io::Result;
use std::path::Path;

pub mod filedb;
pub mod memdb;

pub fn open_memory<'a>() -> memdb::MemoryDb<'a> {
    memdb::MemoryDb::open()
}

pub fn open_file<P: AsRef<Path>>(path: P) -> Result<filedb::FileDb> {
    filedb::FileDb::open(path)
}

/// key-value map store interface. the key type is `&str`.
pub trait DbMap {
    /// returns the value corresponding to the key.
    fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// inserts a key-value pair into the db-map.
    fn put(&mut self, key: &str, value: &[u8]);

    /// removes a key from the db-map.
    fn delete(&mut self, key: &str);

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self);
    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self);

    /// returns true if the map contains a value for the specified key.
    fn has_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }
    /// returns the value corresponding to the key. the value is converted to `String`.
    fn get_string(&self, key: &str) -> Option<String> {
        self.get(key)
            .as_ref()
            .map(|val| String::from_utf8_lossy(val).to_string())
    }
    /// inserts a key-value pair into the db-map. the value is `&str` and it is converted to `&[u8]`
    fn put_string(&mut self, key: &str, value: &str) {
        self.put(key, value.as_bytes())
    }
}

/// key-value list store interface. the key type is `u64`.
pub trait DbList {
    /// returns the value corresponding to the key.
    fn get(&self, key: u64) -> Option<Vec<u8>>;
    /// inserts a key-value pair into the db-list.
    fn put(&mut self, key: u64, value: &[u8]);

    /// removes a key from the db-list.
    fn delete(&mut self, key: u64);

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self);
    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self);

    /// returns true if the list contains a value for the specified key.
    fn has_key(&self, key: u64) -> bool {
        self.get(key).is_some()
    }
    /// returns the value corresponding to the key. the value is converted to `String`.
    fn get_string(&self, key: u64) -> Option<String> {
        self.get(key)
            .as_ref()
            .map(|val| String::from_utf8_lossy(val).to_string())
    }
    /// inserts a key-value pair into the db-list. the value is `&str` and it is converted to `&[u8]`
    fn put_string(&mut self, key: u64, value: &str) {
        self.put(key, value.as_bytes())
    }
}
