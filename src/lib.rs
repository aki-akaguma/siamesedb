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
- minimum support rustc rustc 1.53.0 (53cb7b09b 2021-06-17)

# Compatibility

- Nothing?

# Examples

## Example DbMapString:

```
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

## Example DbMapU64:

```
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

*/
use std::borrow::Borrow;
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

/// generic key-value map store interface. the key type is `KT`.
pub trait DbXxx<KT> {
    /// returns the value corresponding to the key.
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized;

    /// inserts a key-value pair into the db.
    fn put(&mut self, key: KT, value: &[u8]) -> Result<()>
    where
        KT: Ord;

    /// removes a key from the db.
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized;

    /// flush file buffer, the dirty intermediate buffered content is written
    fn flush(&mut self) -> Result<()>;

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self) -> Result<()>;

    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self) -> Result<()>;

    /// returns true if the map contains a value for the specified key.
    fn has_key<Q>(&mut self, key: &Q) -> Result<bool>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.get(key).map(|opt| opt.is_some())
    }

    /// returns the value corresponding to the key. the value is converted to `String`.
    fn get_string<Q>(&mut self, key: &Q) -> Result<Option<String>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        self.get(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// inserts a key-value pair into the db-map. the value is `&str` and it is converted to `&[u8]`
    fn put_string(&mut self, key: KT, value: &str) -> Result<()>
    where
        KT: Ord,
    {
        self.put(key, value.as_bytes())
    }

    /// inserts bulk key-value pairs into the db.
    fn bulk_put(&mut self, bulk: &[(KT, &[u8])]) -> Result<()>
    where
        KT: Ord + Clone,
    {
        let mut vec = bulk.to_vec();
        vec.sort_by(|a, b| b.0.cmp(&(a.0)));
        while let Some(kv) = vec.pop() {
            self.put(kv.0, kv.1)?;
        }
        Ok(())
    }

    /// inserts bulk key-value pairs into the db.
    fn bulk_put_string(&mut self, bulk: &[(KT, String)]) -> Result<()>
    where
        KT: Ord + Clone,
    {
        let mut vec = bulk.to_vec();
        vec.sort_by(|a, b| b.0.cmp(&(a.0)));
        while let Some(kv) = vec.pop() {
            self.put(kv.0, kv.1.as_bytes())?;
        }
        Ok(())
    }

    /// gets bulk key-value paires from the db.
    fn bulk_get<Q>(&mut self, bulk_keys: &[&Q]) -> Result<Vec<Option<Vec<u8>>>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        /*
        let mut vec = Vec::with_capacity(bulk_keys.len());
        for a in bulk_keys {
            let value = self.get(a)?;
            vec.push(value);
        }
        Ok(vec)
        */
        /*
        */
        let mut result: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        let mut vec: Vec<(usize, &Q)> =
            bulk_keys.iter().enumerate().map(|(i, &a)| (i, a)).collect();
        vec.sort_by(|a, b| b.1.cmp(a.1));
        while let Some(ik) = vec.pop() {
            let result_value = self.get(ik.1)?;
            result.push((ik.0, result_value));
        }
        result.sort_by(|a, b| a.0.cmp(&(b.0)));
        let ret: Vec<Option<Vec<u8>>> = result.iter().map(|a| a.1.clone()).collect();
        Ok(ret)
    }
    /// gets bulk key-value paires from the db.
    fn bulk_get_string<Q>(&mut self, bulk_keys: &[&Q]) -> Result<Vec<Option<String>>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let vec = self.bulk_get(bulk_keys)?;
        let mut ret = Vec::new();
        for opt in vec {
            let b = opt.map(|val| String::from_utf8_lossy(&val).to_string());
            ret.push(b);
        }
        Ok(ret)
    }
}

/// key-value map store interface. the key type is `String`.
pub trait DbMapString: DbXxx<String> {}

/// key-value map store interface. the key type is `u64`.
pub trait DbMapU64: DbXxx<u64> {}
