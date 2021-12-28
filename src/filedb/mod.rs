use std::cell::RefCell;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

mod dbmap;
mod inner;

pub use dbmap::Bytes;
pub use dbmap::FileDbMapBytes;
pub use dbmap::FileDbMapString;
pub use dbmap::FileDbMapU64;

pub use inner::dbxxx::{DbXxxIntoIter, DbXxxIter, DbXxxIterMut, FileDbXxxInner};
use inner::semtype::*;
use inner::FileDbInner;

/// Parameters of buffer.
#[derive(Debug, Clone)]
pub enum FileBufSizeParam {
    /// Fixed buffer size
    Size(u32),
    /// Auto buffer size by file size.
    PerMille(u16),
    /// Default auto buffer size by file size.
    Auto,
}

/// Parameters of filedb.
///
/// chunk_size is MUST power of 2.
#[derive(Debug, Clone)]
pub struct FileDbParams {
    /// buffer size of key file buffer. None is auto buffer size.
    pub key_buf_size: FileBufSizeParam,
    /// buffer size of val file buffer. None is auto buffer size.
    pub val_buf_size: FileBufSizeParam,
    /// buffer size of idx file buffer. None is auto buffer size.
    pub idx_buf_size: FileBufSizeParam,
    /// buffer size of htx file buffer. None is auto buffer size.
    pub htx_buf_size: FileBufSizeParam,
}

impl std::default::Default for FileDbParams {
    fn default() -> Self {
        Self {
            key_buf_size: FileBufSizeParam::Auto,
            val_buf_size: FileBufSizeParam::Auto,
            idx_buf_size: FileBufSizeParam::Auto,
            htx_buf_size: FileBufSizeParam::Auto,
        }
    }
}

/// Checks the file db map for debug.
pub trait CheckFileDbMap {
    #[cfg(feature = "htx")]
    fn ht_size_and_count(&self) -> Result<(u64, u64)>;
    /// convert the index node tree to graph string for debug.
    fn graph_string(&self) -> Result<String>;
    /// convert the index node tree to graph string for debug.
    fn graph_string_with_key_string(&self) -> Result<String>;
    /// check the index node tree is balanced
    fn is_balanced(&self) -> Result<bool>;
    /// check the index node tree is multi search tree
    fn is_mst_valid(&self) -> Result<bool>;
    /// check the index node except the root and leaves of the tree has branches of hm or more.
    fn is_dense(&self) -> Result<bool>;
    /// get the depth of the index node.
    fn depth_of_node_tree(&self) -> Result<u64>;
    /// count of the free node
    fn count_of_free_node(&self) -> Result<CountOfPerSize>;
    /// count of the free record
    fn count_of_free_record(&self) -> Result<CountOfPerSize>;
    /// count of the used record and the used node
    fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize)>;
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    fn buf_stats(&self) -> Vec<(String, i64)>;
    /// key record size statistics
    fn key_record_size_stats(&self) -> Result<RecordSizeStats<Key>>;
    /// value record size statistics
    fn value_record_size_stats(&self) -> Result<RecordSizeStats<Value>>;
    /// keys count statistics
    fn keys_count_stats(&self) -> Result<KeysCountStats>;
    /// key length statistics
    fn key_length_stats(&self) -> Result<LengthStats<Key>>;
    /// value length statistics
    fn value_length_stats(&self) -> Result<LengthStats<Value>>;
}

pub type CountOfPerSize = Vec<(u32, u64)>;

/// record size statistics.
#[derive(Debug, Default)]
pub struct RecordSizeStats<T>(Vec<(RecordSize<T>, u64)>);

impl<T: Copy + Ord> RecordSizeStats<T> {
    pub fn new(vec: Vec<(RecordSize<T>, u64)>) -> Self {
        Self(vec)
    }
    pub fn touch_size(&mut self, record_size: RecordSize<T>) {
        match self.0.binary_search_by_key(&record_size, |&(a, _b)| a) {
            Ok(sz_idx) => {
                self.0[sz_idx].1 += 1;
            }
            Err(sz_idx) => {
                self.0.insert(sz_idx, (record_size, 1));
            }
        }
    }
}

impl<T: Copy> std::fmt::Display for RecordSizeStats<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("[")?;
        if self.0.len() > 1 {
            for (a, b) in self.0.iter().take(self.0.len() - 1) {
                formatter.write_fmt(format_args!("({}, {})", a, b))?;
                formatter.write_str(", ")?;
            }
        }
        if !self.0.is_empty() {
            let (a, b) = self.0[self.0.len() - 1];
            formatter.write_fmt(format_args!("({}, {})", a, b))?;
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

pub type KeyRecordSizeStats = RecordSizeStats<Key>;
pub type ValueRecordSizeStats = RecordSizeStats<Value>;

/// record size statistics.
#[derive(Debug, Default)]
pub struct KeysCountStats(Vec<(KeysCount, u64)>);

impl KeysCountStats {
    pub fn new(vec: Vec<(KeysCount, u64)>) -> Self {
        Self(vec)
    }
    pub fn touch_size(&mut self, keys_count: KeysCount) {
        match self.0.binary_search_by_key(&keys_count, |&(a, _b)| a) {
            Ok(sz_idx) => {
                self.0[sz_idx].1 += 1;
            }
            Err(sz_idx) => {
                self.0.insert(sz_idx, (keys_count, 1));
            }
        }
    }
}

impl std::fmt::Display for KeysCountStats {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("[")?;
        if self.0.len() > 1 {
            for (a, b) in self.0.iter().take(self.0.len() - 1) {
                formatter.write_fmt(format_args!("({}, {})", a, b))?;
                formatter.write_str(", ")?;
            }
        }
        if !self.0.is_empty() {
            let (a, b) = self.0[self.0.len() - 1];
            formatter.write_fmt(format_args!("({}, {})", a, b))?;
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

/// key or value length statistics.
#[derive(Debug, Default)]
pub struct LengthStats<T: Default>(Vec<(Length<T>, u64)>);

impl<T: Ord + Default + Copy> LengthStats<T> {
    pub fn new(vec: Vec<(Length<T>, u64)>) -> Self {
        Self(vec)
    }
    pub fn touch_length(&mut self, key_length: Length<T>) {
        match self.0.binary_search_by_key(&key_length, |&(a, _b)| a) {
            Ok(sz_idx) => {
                self.0[sz_idx].1 += 1;
            }
            Err(sz_idx) => {
                self.0.insert(sz_idx, (key_length, 1));
            }
        }
    }
}

impl<T: Default + Copy> std::fmt::Display for LengthStats<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("[")?;
        if self.0.len() > 1 {
            for (a, b) in self.0.iter().take(self.0.len() - 1) {
                formatter.write_fmt(format_args!("({}, {})", a, b))?;
                formatter.write_str(", ")?;
            }
        }
        if !self.0.is_empty() {
            let (a, b) = self.0[self.0.len() - 1];
            formatter.write_fmt(format_args!("({}, {})", a, b))?;
        }
        formatter.write_str("]")?;
        Ok(())
    }
}

/// File Database.
#[derive(Debug, Clone)]
pub struct FileDb(Rc<RefCell<FileDbInner>>);

impl FileDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self(Rc::new(RefCell::new(FileDbInner::open(path)?))))
    }
    pub fn db_map_string(&self, name: &str) -> Result<FileDbMapString> {
        self.db_map_string_with_params(name, FileDbParams::default())
    }
    pub fn db_map_string_with_params(
        &self,
        name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMapString> {
        if let Some(m) = RefCell::borrow(&self.0).db_map(name) {
            return Ok(m);
        }
        RefCell::borrow_mut(&self.0).create_db_map(name, params)?;
        match RefCell::borrow(&self.0).db_map(name) {
            Some(m) => Ok(m),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_map_u64(&self, name: &str) -> Result<FileDbMapU64> {
        self.db_map_u64_with_params(name, FileDbParams::default())
    }
    pub fn db_map_u64_with_params(&self, name: &str, params: FileDbParams) -> Result<FileDbMapU64> {
        if let Some(m) = RefCell::borrow(&self.0).db_list(name) {
            return Ok(m);
        }
        RefCell::borrow_mut(&self.0).create_db_list(name, params)?;
        match RefCell::borrow(&self.0).db_list(name) {
            Some(m) => Ok(m),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_map_bytes(&self, name: &str) -> Result<FileDbMapBytes> {
        self.db_map_bytes_with_params(name, FileDbParams::default())
    }
    pub fn db_map_bytes_with_params(
        &self,
        name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMapBytes> {
        if let Some(m) = RefCell::borrow(&self.0).db_map_bytes(name) {
            return Ok(m);
        }
        RefCell::borrow_mut(&self.0).create_db_map_bytes(name, params)?;
        match RefCell::borrow(&self.0).db_map_bytes(name) {
            Some(m) => Ok(m),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn path(&self) -> PathBuf {
        RefCell::borrow(&self.0).path().to_path_buf()
    }
    pub fn sync_all(&self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_data()
    }
}

//--
#[cfg(test)]
mod debug {
    use super::FileDbInner;
    use super::{FileDb, FileDbMapString, FileDbMapU64};
    use super::{KeyRecordSizeStats, ValueRecordSizeStats};
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<FileDb>(), 8);
            assert_eq!(std::mem::size_of::<FileDbMapString>(), 8);
            assert_eq!(std::mem::size_of::<FileDbMapU64>(), 8);
            //
            assert_eq!(std::mem::size_of::<FileDbInner>(), 96);
            //
            assert_eq!(std::mem::size_of::<KeyRecordSizeStats>(), 24);
            assert_eq!(std::mem::size_of::<ValueRecordSizeStats>(), 24);
        }
        //
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<FileDb>(), 4);
            assert_eq!(std::mem::size_of::<FileDbMapString>(), 4);
            assert_eq!(std::mem::size_of::<FileDbMapU64>(), 4);
            //
            assert_eq!(std::mem::size_of::<FileDbInner>(), 48);
            //
            assert_eq!(std::mem::size_of::<KeyRecordSizeStats>(), 12);
            assert_eq!(std::mem::size_of::<ValueRecordSizeStats>(), 12);
        }
    }
}
