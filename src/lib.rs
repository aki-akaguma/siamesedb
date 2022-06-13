/*!
The simple local key-value store.

# Features

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

# Compatibility

- Nothing?

# Todo

- [ ] more performance
- [ ] DB lock as support for multi-process-safe

# Low priority todo

- [ ] transaction support that handles multiple key-space at a time.
- [ ] thread-safe support
- [ ] non db lock multi-process-safe support

# Examples

## Example DbMapDbString:

```
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

## Example DbMapDbInt:

```
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

## Example Iterator:

```
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
*/
use std::hash::Hash;
use std::io::Result;
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

/// base interface for generic key-value map store interface. this is not include `KT`
pub trait DbXxxBase {
    /// read and fill buffer.
    fn read_fill_buffer(&mut self) -> Result<()>;

    /// flush file buffer, the dirty intermediate buffered content is written.
    fn flush(&mut self) -> Result<()>;

    /// synchronize all OS-internal metadata to storage.
    fn sync_all(&mut self) -> Result<()>;

    /// synchronize data to storage, except file metadabe.
    fn sync_data(&mut self) -> Result<()>;
}

/// generic key-value map store interface. the key type is `KT`. this is only object safe.
pub trait DbXxxObjectSafe<KT: DbMapKeyType>: DbXxxBase {
    /// returns the value corresponding to the key. this key is store raw data and type `&[u8]`.
    fn get_kt(&mut self, key: &KT) -> Result<Option<Vec<u8>>>;

    /// inserts a key-value pair into the db. this key is store raw data and type `&[u8]`.
    fn put_kt(&mut self, key: &KT, value: &[u8]) -> Result<()>;

    /// removes a key from the db. this key is store raw data and type `&[u8]`.
    fn del_kt(&mut self, key: &KT) -> Result<Option<Vec<u8>>>;
}

/// generic key-value map store interface. the key type is `KT`.
pub trait DbXxx<KT: DbMapKeyType>: DbXxxObjectSafe<KT> {
    /// returns the value corresponding to the key.
    #[inline]
    fn get<'a, Q>(&mut self, key: &'a Q) -> Result<Option<Vec<u8>>>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let key_kt: KT = From::from(key);
        self.get_kt(&key_kt)
    }

    /// returns the value corresponding to the key. the value is converted to `String`.
    #[inline]
    fn get_string<'a, Q>(&mut self, key: &'a Q) -> Result<Option<String>>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        self.get(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// gets bulk key-value paires from the db.
    fn bulk_get<'a, Q>(&mut self, bulk_keys: &[&'a Q]) -> Result<Vec<Option<Vec<u8>>>>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let mut result: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        let mut vec: Vec<(usize, &Q)> =
            bulk_keys.iter().enumerate().map(|(i, &a)| (i, a)).collect();
        vec.sort_unstable_by(|a, b| b.1.cmp(a.1));
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
        KT: From<&'a Q>,
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
    fn put<'a, Q>(&mut self, key: &'a Q, value: &[u8]) -> Result<()>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let key_kt: KT = From::from(key);
        self.put_kt(&key_kt, value)
    }

    /// inserts a key-value pair into the db-map. the value is `&str` and it is converted to `&[u8]`
    #[inline]
    fn put_string<'a, Q>(&mut self, key: &'a Q, value: &str) -> Result<()>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        self.put(key, value.as_bytes())
    }

    /// inserts bulk key-value pairs into the db.
    fn bulk_put<'a, Q>(&mut self, bulk: &[(&'a Q, &[u8])]) -> Result<()>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let mut vec = bulk.to_vec();
        vec.sort_by(|a, b| b.0.cmp(a.0));
        while let Some(kv) = vec.pop() {
            self.put(kv.0, kv.1)?;
        }
        Ok(())
    }

    /// inserts bulk key-value pairs into the db.
    #[inline]
    fn bulk_put_string<'a, Q>(&mut self, bulk: &[(&'a Q, String)]) -> Result<()>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let mut vec = bulk.to_vec();
        vec.sort_unstable_by(|a, b| b.0.cmp(a.0));
        while let Some(kv) = vec.pop() {
            self.put(kv.0, kv.1.as_bytes())?;
        }
        Ok(())
    }

    /// removes a key from the db.
    #[inline]
    fn delete<'a, Q>(&mut self, key: &'a Q) -> Result<Option<Vec<u8>>>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let key_kt: KT = From::from(key);
        self.del_kt(&key_kt)
    }

    /// removes a key from the db.
    #[inline]
    fn delete_string<'a, Q>(&mut self, key: &'a Q) -> Result<Option<String>>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        self.delete(key)
            .map(|opt| opt.map(|val| String::from_utf8_lossy(&val).to_string()))
    }

    /// delete bulk key-value paires from the db.
    fn bulk_delete<'a, Q>(&mut self, bulk_keys: &[&'a Q]) -> Result<Vec<Option<Vec<u8>>>>
    where
        KT: From<&'a Q>,
        Q: Ord + ?Sized,
    {
        let mut result: Vec<(usize, Option<Vec<u8>>)> = Vec::new();
        let mut vec: Vec<(usize, &Q)> =
            bulk_keys.iter().enumerate().map(|(i, &a)| (i, a)).collect();
        vec.sort_unstable_by(|a, b| b.1.cmp(a.1));
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
        KT: From<&'a Q>,
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
pub trait DbMap<KT: DbMapKeyType>: DbXxx<KT> {
    fn iter(&self) -> DbXxxIter<KT>;
    fn iter_mut(&mut self) -> DbXxxIterMut<KT>;
}

/// key-value map store interface. the key type is `String`.
pub trait DbMapDbString: DbXxx<DbString> {}

/// key-value map store interface. the key type is `u64`.
pub trait DbMapDbInt: DbXxx<DbInt> {}

/// key-value map store interface. the key type is `Vec<u8>`.
pub trait DbMapDbBytes: DbXxx<DbBytes> {}

/// key type
pub trait DbMapKeyType: Ord + Clone + Default + HashValue {
    /// Convert a byte slice to Key.
    fn from_bytes(bytes: &[u8]) -> Self;
    /// Signature in header of database file.
    fn signature() -> [u8; 8];
    /// Byte slice of data to be saved.
    fn as_bytes(&self) -> &[u8];
    /// Compare with stored data
    fn cmp_u8(&self, other: &[u8]) -> std::cmp::Ordering;
    /// Short byte slice of data to be saved node.
    #[cfg(feature = "tr_has_short_key")]
    fn as_short_bytes(&self) -> Option<&[u8]> {
        let b_sl = self.as_bytes();
        if b_sl.len() <= 32 {
            Some(b_sl)
        } else {
            None
        }
    }
}

/// hash value for htx
pub trait HashValue: Hash {
    /// hash value for htx
    fn hash_value(&self) -> u64 {
        use std::hash::Hasher;
        #[cfg(not(feature = "myhasher"))]
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        #[cfg(feature = "myhasher")]
        let mut hasher = MyHasher::default();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Default)]
struct MyHasher(u64);

impl std::hash::Hasher for MyHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for chunk8 in bytes.chunks(8) {
            let len = chunk8.len();
            if len == 8 {
                let mut ary = [0u8; 8];
                ary.copy_from_slice(chunk8);
                let a = u64::from_le_bytes(ary);
                self.0 = _xorshift64s(self.0.wrapping_add(a));
            } else {
                let mut a = 0;
                for b in chunk8 {
                    a = (a << 8) | *b as u64;
                }
                self.0 = _xorshift64s(self.0.wrapping_add(a));
            }
        }
    }
}

#[inline]
fn _xorshift64s(a: u64) -> u64 {
    //let mut x = a.rotate_right(12);
    let mut x = a;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x
}
