/*!
The simple local key-value store.

# Features

- key-value store.
- in-memory and file store.
- `DbMapString` has keys as utf-8 string.
- `DbMapU64` has keys as u64.
- The value is any bytes included utf-8 string.
- The file store is implemented the basic B-Tree. (no hash and no leaf)
- Small db file size.
- Separated files. (data record file and index file)
- One database has some db-map-string and some db-map-u64.
- minimum support rustc 1.54.0 (a178d0322 2021-07-26)

# Compatibility

- Nothing?

# Examples

## Example DbMapString:

```
use siamesedb::DbMapString;

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test1.shamdb";
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
    assert_eq!(r, Some("value1".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

## Example DbMapU64:

```
use siamesedb::DbMapU64;

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test2.shamdb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    let mut db_map = db.db_map_u64("some_list1")?;
    let r = db_map.get_string(120)?;
    assert_eq!(r, None);
    db_map.put_string(120, "value120")?;
    let r = db_map.get_string(120)?;
    assert_eq!(r, Some("value120".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

*/
use std::io::Result;
use std::path::Path;

pub mod filedb;
pub mod memdb;

/// Open the memory db. This data is not stored in file.
pub fn open_memory<'a>() -> memdb::MemoryDb<'a> {
    memdb::MemoryDb::open()
}

/// Open the file db. This data is stored in file.
pub fn open_file<P: AsRef<Path>>(path: P) -> Result<filedb::FileDb> {
    filedb::FileDb::open(path)
}

/// key-value map store interface. the key type is `&str`.
pub trait DbMapString {
    /// returns the value corresponding to the key.
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>>;

    /// inserts a key-value pair into the db-map.
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()>;

    /// removes a key from the db-map.
    fn delete(&mut self, key: &str) -> Result<()>;

    /// flush file buffer, the dirty intermediate buffered content is written
    fn flush(&mut self) -> Result<()>;

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self) -> Result<()>;

    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self) -> Result<()>;

    /// returns true if the map contains a value for the specified key.
    fn has_key(&mut self, key: &str) -> Result<bool> {
        self.get(key).map(|opt| opt.is_some())
    }

    /// returns the value corresponding to the key. the value is converted to `String`.
    fn get_string(&mut self, key: &str) -> Result<Option<String>> {
        self.get(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// inserts a key-value pair into the db-map. the value is `&str` and it is converted to `&[u8]`
    fn put_string(&mut self, key: &str, value: &str) -> Result<()> {
        self.put(key, value.as_bytes())
    }
}

/// key-value list store interface. the key type is `u64`.
pub trait DbMapU64 {
    /// returns the value corresponding to the key.
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>>;

    /// inserts a key-value pair into the db-list.
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()>;

    /// removes a key from the db-list.
    fn delete(&mut self, key: u64) -> Result<()>;

    /// flush file buffer, the dirty intermediate buffered content is written
    fn flush(&mut self) -> Result<()>;

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self) -> Result<()>;

    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self) -> Result<()>;

    /// returns true if the list contains a value for the specified key.
    fn has_key(&mut self, key: u64) -> Result<bool> {
        self.get(key).map(|opt| opt.is_some())
    }

    /// returns the value corresponding to the key. the value is converted to `String`.
    fn get_string(&mut self, key: u64) -> Result<Option<String>> {
        self.get(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// inserts a key-value pair into the db-list. the value is `&str` and it is converted to `&[u8]`
    fn put_string(&mut self, key: u64, value: &str) -> Result<()> {
        self.put(key, value.as_bytes())
    }
}

/// generic key-value map store interface. the key type is `KT`.
pub trait DbXxx<KT> {
    /// returns the value corresponding to the key.
    fn get(&mut self, key: &KT) -> Result<Option<Vec<u8>>>;

    /// inserts a key-value pair into the db.
    fn put(&mut self, key: &KT, value: &[u8]) -> Result<()>;

    /// removes a key from the db.
    fn delete(&mut self, key: &KT) -> Result<()>;

    /// flush file buffer, the dirty intermediate buffered content is written
    fn flush(&mut self) -> Result<()>;

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self) -> Result<()>;

    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self) -> Result<()>;

    /// returns true if the map contains a value for the specified key.
    fn has_key(&mut self, key: &KT) -> Result<bool> {
        self.get(key).map(|opt| opt.is_some())
    }

    /// returns the value corresponding to the key. the value is converted to `String`.
    fn get_string(&mut self, key: &KT) -> Result<Option<String>> {
        self.get(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// inserts a key-value pair into the db-map. the value is `&str` and it is converted to `&[u8]`
    fn put_string(&mut self, key: &KT, value: &str) -> Result<()> {
        self.put(key, value.as_bytes())
    }
}
