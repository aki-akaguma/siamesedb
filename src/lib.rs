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
    db_map.put_string("key1".into(), "value1")?;
    let r = db_map.get_string("key1")?;
    assert_eq!(r, Some("value1".into()));
    db_map.sync_data()?;
    Ok(())
}
```

## Example DbMapDbInt:

```
use siamesedb::{DbMapDbInt, DbXxx};

fn main() -> std::io::Result<()> {
    let db_name = "target/tmp/doc-test2.siamesedb";
    // remove database
    let _ = std::fs::remove_dir_all(db_name);
    // create or open database
    let db = siamesedb::open_file(db_name)?;
    let mut db_map = db.db_map_int("some_list1")?;
    let r = db_map.get_string(&120)?;
    assert_eq!(r, None);
    db_map.put_string(120.into(), "value120")?;
    let r = db_map.get_string(&120)?;
    assert_eq!(r, Some("value120".to_string()));
    db_map.sync_data()?;
    Ok(())
}
```

## Example Iterator:

```
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
*/
use std::hash::Hash;
use std::io::Result;
use std::ops::Deref;
use std::path::Path;

pub mod filedb;
pub mod memdb;

pub use filedb::{DbBytes, DbInt, DbString};
pub use filedb::{DbXxxIter, DbXxxIterMut};

/// Open the memory db. This data is not stored in file.
pub fn open_memory<'a>() -> memdb::MemoryDb<'a> {
    memdb::MemoryDb::open()
}

/// Open the file db. This data is stored in file.
pub fn open_file<P: AsRef<Path>>(path: P) -> Result<filedb::FileDb> {
    filedb::FileDb::open(path)
}

/// generic key-value map store interface. the key type is `KT`.
pub trait DbXxx<KT: DbXxxKeyType> {
    /// read and fill buffer.
    fn read_fill_buffer(&mut self) -> Result<()>;

    /// flush file buffer, the dirty intermediate buffered content is written.
    fn flush(&mut self) -> Result<()>;

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self) -> Result<()>;

    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self) -> Result<()>;

    /// returns the value corresponding to the key. this key is store raw data and type `&[u8]`.
    fn get_k8(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// inserts a key-value pair into the db. this key is store raw data and type `&[u8]`.
    fn put_k8(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// removes a key from the db. this key is store raw data and type `&[u8]`.
    fn del_k8(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    //
    #[inline]
    fn get_string_k8(&mut self, key: &[u8]) -> Result<Option<String>> {
        self.get_k8(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }
    //
    fn bulk_get_k8(&mut self, bulk_keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        /*
        let mut vec = Vec::with_capacity(bulk_keys.len());
        for a in bulk_keys {
            let value = self.get_k8(a)?;
            vec.push(value);
        }
        Ok(vec)
        */
        let mut result: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        let mut vec: Vec<(usize, &[u8])> =
            bulk_keys.iter().enumerate().map(|(i, &a)| (i, a)).collect();
        vec.sort_by(|a, b| b.1.cmp(a.1));
        while let Some(ik) = vec.pop() {
            let result_value = self.get_k8(ik.1)?;
            result.push((ik.0, result_value));
        }
        result.sort_by(|a, b| a.0.cmp(&(b.0)));
        let ret: Vec<Option<Vec<u8>>> = result.iter().map(|a| a.1.clone()).collect();
        Ok(ret)
    }
    //
    #[inline]
    fn bulk_get_string_k8(&mut self, bulk_keys: &[&[u8]]) -> Result<Vec<Option<String>>> {
        let vec = self.bulk_get_k8(bulk_keys)?;
        let mut ret = Vec::new();
        for opt in vec {
            let b = opt.map(|val| String::from_utf8_lossy(&val).to_string());
            ret.push(b);
        }
        Ok(ret)
    }
    //
    #[inline]
    fn put_string_k8(&mut self, key: &[u8], value: &str) -> Result<()> {
        self.put_k8(key, value.as_bytes())
    }
    //
    #[inline]
    fn bulk_put_k8(&mut self, bulk: &[(&[u8], &[u8])]) -> Result<()> {
        let mut vec = bulk.to_vec();
        vec.sort_by(|a, b| b.0.cmp(a.0));
        while let Some(kv) = vec.pop() {
            self.put_k8(kv.0, kv.1)?;
        }
        Ok(())
    }
    //
    #[inline]
    fn bulk_put_string_k8(&mut self, bulk: &[(&[u8], String)]) -> Result<()> {
        let mut vec = bulk.to_vec();
        vec.sort_by(|a, b| b.0.cmp(a.0));
        while let Some(kv) = vec.pop() {
            self.put_k8(kv.0, kv.1.as_bytes())?;
        }
        Ok(())
    }
    //
    #[inline]
    fn del_string_k8(&mut self, key: &[u8]) -> Result<Option<String>> {
        self.del_k8(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }
    //
    #[inline]
    fn bulk_del_k8(&mut self, bulk_keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut result: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        let mut vec: Vec<(usize, &[u8])> =
            bulk_keys.iter().enumerate().map(|(i, &a)| (i, a)).collect();
        vec.sort_by(|a, b| b.1.cmp(a.1));
        while let Some(ik) = vec.pop() {
            let result_value = self.del_k8(ik.1)?;
            result.push((ik.0, result_value));
        }
        result.sort_by(|a, b| a.0.cmp(&(b.0)));
        let ret: Vec<Option<Vec<u8>>> = result.iter().map(|a| a.1.clone()).collect();
        Ok(ret)
    }
    //
    #[inline]
    fn bulk_del_string_k8(&mut self, bulk_keys: &[&[u8]]) -> Result<Vec<Option<String>>> {
        let vec = self.bulk_del_k8(bulk_keys)?;
        let mut ret = Vec::new();
        for opt in vec {
            let b = opt.map(|val| String::from_utf8_lossy(&val).to_string());
            ret.push(b);
        }
        Ok(ret)
    }

    // returns the value corresponding to the key.
    #[inline]
    fn get<'a, Q>(&mut self, key: &'a Q) -> Result<Option<Vec<u8>>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let key_kt: DbBytes = From::from(key);
        self.get_k8(key_kt.deref())
    }

    /// returns the value corresponding to the key. the value is converted to `String`.
    #[inline]
    fn get_string<'a, Q>(&mut self, key: &'a Q) -> Result<Option<String>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        self.get(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// gets bulk key-value paires from the db.
    fn bulk_get<'a, Q>(&mut self, bulk_keys: &[&'a Q]) -> Result<Vec<Option<Vec<u8>>>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
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
    #[inline]
    fn bulk_get_string<'a, Q>(&mut self, bulk_keys: &[&'a Q]) -> Result<Vec<Option<String>>>
    where
        DbBytes: From<&'a Q>,
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

    /// inserts a key-value pair into the db.
    #[inline]
    fn put(&mut self, key_kt: KT, value: &[u8]) -> Result<()> {
        self.put_k8(key_kt.as_bytes(), value)
    }

    /// inserts a key-value pair into the db-map. the value is `&str` and it is converted to `&[u8]`
    #[inline]
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
    #[inline]
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

    /// removes a key from the db.
    #[inline]
    fn delete<'a, Q>(&mut self, key: &'a Q) -> Result<Option<Vec<u8>>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let key_kt: DbBytes = From::from(key);
        self.del_k8(key_kt.deref())
    }

    /// removes a key from the db.
    #[inline]
    fn delete_string<'a, Q>(&mut self, key: &'a Q) -> Result<Option<String>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        self.delete(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// delete bulk key-value paires from the db.
    fn bulk_delete<'a, Q>(&mut self, bulk_keys: &[&'a Q]) -> Result<Vec<Option<Vec<u8>>>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let mut result: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        let mut vec: Vec<(usize, &Q)> =
            bulk_keys.iter().enumerate().map(|(i, &a)| (i, a)).collect();
        vec.sort_by(|a, b| b.1.cmp(a.1));
        while let Some(ik) = vec.pop() {
            let result_value = self.delete(ik.1)?;
            result.push((ik.0, result_value));
        }
        result.sort_by(|a, b| a.0.cmp(&(b.0)));
        let ret: Vec<Option<Vec<u8>>> = result.iter().map(|a| a.1.clone()).collect();
        Ok(ret)
    }

    /// delete bulk key-value paires from the db.
    #[inline]
    fn bulk_delete_string<'a, Q>(&mut self, bulk_keys: &[&'a Q]) -> Result<Vec<Option<String>>>
    where
        DbBytes: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let vec = self.bulk_delete(bulk_keys)?;
        let mut ret = Vec::new();
        for opt in vec {
            let b = opt.map(|val| String::from_utf8_lossy(&val).to_string());
            ret.push(b);
        }
        Ok(ret)
    }
}

/// key-value db map store interface.
pub trait DbMap<KT: DbXxxKeyType>: DbXxx<KT> {
    fn iter(&self) -> DbXxxIter<KT>;
    fn iter_mut(&mut self) -> DbXxxIterMut<KT>;
}

/// key-value map store interface. the key type is `String`.
pub trait DbMapString: DbXxx<DbString> {}

/// key-value map store interface. the key type is `u64`.
pub trait DbMapDbInt: DbXxx<DbInt> {}

/// key-value map store interface. the key type is `Vec<u8>`.
pub trait DbMapDbBytes: DbXxx<DbBytes> {}

/// key type
pub trait DbXxxKeyType: Ord + Clone + Default + HashValue + Deref {
    /// Signature of database file.
    fn signature() -> [u8; 8];
    fn as_bytes(&self) -> &[u8];
    fn to_bytes_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    /// Converts a KeyType into a byte vector.
    //fn into_bytes(self) -> Vec<u8>;
    fn from(bytes: &[u8]) -> Self;
    fn byte_len(&self) -> usize {
        self.as_bytes().len()
    }
    fn cmp_u8(&self, other: &[u8]) -> std::cmp::Ordering;
}

pub trait HashValue: Hash {
    fn hash_value(&self) -> u64 {
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}
impl HashValue for &[u8] {}
