use super::{DbMapString, DbMapU64, DbXxx};
use std::cell::RefCell;
use std::io::Result;
use std::path::Path;
use std::rc::{Rc, Weak};

mod inner;

use inner::dbxxx::{FileDbXxxInner, FileDbXxxInnerKT};
use inner::semtype::*;
use inner::FileDbInner;

type CountOfPerSize = Vec<(u32, u64)>;

type FileDbMapStringInner = FileDbXxxInner<String>;
type FileDbMapU64Inner = FileDbXxxInner<u64>;

impl FileDbXxxInnerKT for String {
    fn signature() -> [u8; 8] {
        [b's', b't', b'r', b'i', b'n', b'g', 0u8, 0u8]
    }
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        std::cmp::Ord::cmp(self, other)
    }
    fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn from(bytes: &[u8]) -> Self {
        String::from_utf8_lossy(bytes).to_string()
    }
}

#[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
use std::convert::TryInto;

impl FileDbXxxInnerKT for u64 {
    fn signature() -> [u8; 8] {
        [b'u', b'6', b'4', 0u8, 0u8, 0u8, 0u8, 0u8]
    }
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        std::cmp::Ord::cmp(self, other)
    }
    #[cfg(feature = "vf_u32u32")]
    fn as_bytes(&self) -> Vec<u8> {
        (*self as u32).to_le_bytes().to_vec()
    }
    #[cfg(feature = "vf_u32u32")]
    fn from(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() == 4, "bytes.len():{} == 4", bytes.len());
        u32::from_le_bytes(bytes.try_into().unwrap()) as u64
    }
    #[cfg(feature = "vf_u64u64")]
    fn as_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    #[cfg(feature = "vf_u64u64")]
    fn from(bytes: &[u8]) -> Self {
        u64::from_le_bytes(bytes.try_into().unwrap())
    }
    #[cfg(feature = "vf_vu64")]
    fn as_bytes(&self) -> Vec<u8> {
        vu64::encode(*self).as_ref().to_vec()
    }
    #[cfg(feature = "vf_vu64")]
    fn from(bytes: &[u8]) -> Self {
        vu64::decode(bytes).unwrap()
    }
}

/// File Database.
#[derive(Debug, Clone)]
pub struct FileDb(Rc<RefCell<FileDbInner>>);

#[derive(Debug, Clone)]
pub(crate) struct FileDbNode(Weak<RefCell<FileDbInner>>);

/// Map in a file database.
#[derive(Debug, Clone)]
pub struct FileDbMapString(Rc<RefCell<FileDbMapStringInner>>);

/// List in a file databse.
#[derive(Debug, Clone)]
pub struct FileDbMapU64(Rc<RefCell<FileDbMapU64Inner>>);

/// Parameters of filedb.
///
/// chunk_size is MUST power of 2.
#[derive(Debug, Clone)]
pub struct FileDbParams {
    /// number of chunks at dat file buffer.
    pub dat_buf_num_chunks: u16,
    /// chunk size of dat file buffer.
    pub dat_buf_chunk_size: u32,
    /// number of chunks at idx file buffer.
    pub idx_buf_num_chunks: u16,
    /// chunk size of idx file buffer.
    pub idx_buf_chunk_size: u32,
}

impl std::default::Default for FileDbParams {
    fn default() -> Self {
        Self {
            dat_buf_num_chunks: 32,
            dat_buf_chunk_size: 16 * 1024,
            idx_buf_num_chunks: 16,
            idx_buf_chunk_size: 4 * 1024,
        }
    }
}

impl FileDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self(Rc::new(RefCell::new(FileDbInner::open(path)?))))
    }
    fn to_node(&self) -> FileDbNode {
        FileDbNode(Rc::downgrade(&self.0))
    }
    pub fn db_map_string(&self, name: &str) -> Result<FileDbMapString> {
        self.db_map_string_with_params(name, FileDbParams::default())
    }
    pub fn db_map_string_with_params(&self, name: &str, params: FileDbParams) -> Result<FileDbMapString> {
        if let Some(m) = self.0.borrow().db_map(name) {
            return Ok(m);
        }
        //
        let x = self.to_node();
        x.create_db_map(name, params)?;
        //
        match self.0.borrow().db_map(name) {
            Some(m) => Ok(m),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_map_u64(&self, name: &str) -> Result<FileDbMapU64> {
        self.db_map_u64_with_params(name, FileDbParams::default())
    }
    pub fn db_map_u64_with_params(&self, name: &str, params: FileDbParams) -> Result<FileDbMapU64> {
        if let Some(m) = self.0.borrow().db_list(name) {
            return Ok(m);
        }
        //
        let x = self.to_node();
        x.create_db_list(name, params)?;
        //
        match self.0.borrow().db_list(name) {
            Some(m) => Ok(m),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn sync_all(&self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

impl FileDbNode {
    fn create_db_map(&self, name: &str, params: FileDbParams) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let child: FileDbMapString = FileDbMapString::open(self.clone(), name, params)?;
        let mut locked = rc.borrow_mut();
        let _ = locked.db_map_insert(name, child);
        Ok(())
    }
    fn create_db_list(&self, name: &str, params: FileDbParams) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let child: FileDbMapU64 = FileDbMapU64::open(self.clone(), name, params)?;
        let mut locked = rc.borrow_mut();
        let _ = locked.db_list_insert(name, child);
        Ok(())
    }
    fn _sync_all(&self) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let r = rc.borrow_mut().sync_all();
        r
    }
    fn _sync_data(&self) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let r = rc.borrow_mut().sync_data();
        r
    }
}

impl FileDbMapString {
    pub(crate) fn open(
        parent: FileDbNode,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMapString> {
        Ok(Self(Rc::new(RefCell::new(
            FileDbMapStringInner::open_with_params(parent, ks_name, params)?,
        ))))
    }
    pub fn is_dirty(&self) -> bool {
        self.0.borrow().is_dirty()
    }
}

/// for debug
impl FileDbMapString {
    /// convert index to graph string for debug.
    pub fn to_graph_string(&self) -> Result<String> {
        self.0.borrow().to_graph_string()
    }
    /// convert index to graph string with key string for debug.
    pub fn to_graph_string_with_key_string(&self) -> Result<String> {
        self.0.borrow_mut().to_graph_string_with_key_string()
    }
    /// check the index tree is balanced
    pub fn is_balanced(&self) -> Result<bool> {
        self.0.borrow().is_balanced()
    }
    /// check it is multi search tree
    pub fn is_mst_valid(&self) -> Result<bool> {
        self.0.borrow().is_mst_valid()
    }
    /// check the node except the root and leaves of the tree has branches of half or more.
    pub fn is_dense(&self) -> Result<bool> {
        self.0.borrow().is_dense()
    }
    /// get a depth of the node tree.
    pub fn depth_of_node_tree(&self) -> Result<u64> {
        self.0.borrow().depth_of_node_tree()
    }
    /// count of free node
    pub fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        self.0.borrow().count_of_free_node()
    }
    /// count of free record
    pub fn count_of_free_record(&self) -> Result<CountOfPerSize> {
        self.0.borrow().count_of_free_record()
    }
    /// count of used record and used node
    pub fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize)> {
        self.0.borrow().count_of_used_node()
    }
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        self.0.borrow().buf_stats()
    }
    /// record size statistics
    pub fn record_size_stats(&self) -> Result<RecordSizeStats> {
        self.0.borrow().record_size_stats()
    }
}

impl DbMapString for FileDbMapString {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        self.0.borrow_mut().get(&(key.to_string()))
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(&(key.to_string()), value)
    }
    fn delete(&mut self, key: &str) -> Result<()> {
        self.0.borrow_mut().delete(&(key.to_string()))
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
    fn has_key(&mut self, key: &str) -> Result<bool> {
        self.0.borrow_mut().has_key(&(key.to_string()))
    }
}

impl FileDbMapU64 {
    pub(crate) fn open(
        parent: FileDbNode,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMapU64> {
        Ok(Self(Rc::new(RefCell::new(
            FileDbMapU64Inner::open_with_params(parent, ks_name, params)?,
        ))))
    }
    pub fn is_dirty(&self) -> bool {
        self.0.borrow().is_dirty()
    }
}

/// for debug
impl FileDbMapU64 {
    /// convert index to graph string for debug.
    pub fn to_graph_string(&self) -> Result<String> {
        self.0.borrow().to_graph_string()
    }
    /// convert index to graph string with key string for debug.
    pub fn to_graph_string_with_key_string(&self) -> Result<String> {
        self.0.borrow_mut().to_graph_string_with_key_string()
    }
    /// check the index tree is balanced
    pub fn is_balanced(&self) -> Result<bool> {
        self.0.borrow().is_balanced()
    }
    /// check it is multi search tree
    pub fn is_mst_valid(&self) -> Result<bool> {
        self.0.borrow().is_mst_valid()
    }
    /// check the node except the root and leaves of the tree has branches of half or more.
    pub fn is_dense(&self) -> Result<bool> {
        self.0.borrow().is_dense()
    }
    /// get a depth of the node tree.
    pub fn depth_of_node_tree(&self) -> Result<u64> {
        self.0.borrow().depth_of_node_tree()
    }
    /// count of free node
    pub fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        self.0.borrow().count_of_free_node()
    }
    /// count of free record
    pub fn count_of_free_record(&self) -> Result<CountOfPerSize> {
        self.0.borrow().count_of_free_record()
    }
    /// count of used record and used node
    pub fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize)> {
        self.0.borrow().count_of_used_node()
    }
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        self.0.borrow().buf_stats()
    }
    /// record size statistics
    pub fn record_size_stats(&self) -> Result<RecordSizeStats> {
        self.0.borrow().record_size_stats()
    }
}

impl DbMapU64 for FileDbMapU64 {
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        self.0.borrow_mut().get(&key)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(&key, value)
    }
    fn delete(&mut self, key: u64) -> Result<()> {
        self.0.borrow_mut().delete(&key)
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

/// record size statistics.
#[derive(Debug, Default)]
pub struct RecordSizeStats(Vec<(RecordSize, u64)>);

impl RecordSizeStats {
    pub fn new(vec: Vec<(RecordSize, u64)>) -> Self {
        Self(vec)
    }
    pub fn touch_size(&mut self, record_size: RecordSize) {
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

impl std::fmt::Display for RecordSizeStats {
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

//--
#[cfg(test)]
mod debug {
    use super::RecordSizeStats;
    use super::{FileDb, FileDbMapU64, FileDbMapString};
    use super::{FileDbInner, FileDbMapU64Inner, FileDbMapStringInner};
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<FileDb>(), 8);
            assert_eq!(std::mem::size_of::<FileDbMapString>(), 8);
            assert_eq!(std::mem::size_of::<FileDbMapU64>(), 8);
            //
            assert_eq!(std::mem::size_of::<FileDbInner>(), 72);
            assert_eq!(std::mem::size_of::<FileDbMapStringInner>(), 56);
            assert_eq!(std::mem::size_of::<FileDbMapU64Inner>(), 56);
            //
            assert_eq!(std::mem::size_of::<RecordSizeStats>(), 24);
        }
        //
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<FileDb>(), 4);
            assert_eq!(std::mem::size_of::<FileDbMapString>(), 4);
            assert_eq!(std::mem::size_of::<FileDbMapU64>(), 4);
            //
            assert_eq!(std::mem::size_of::<FileDbInner>(), 36);
            assert_eq!(std::mem::size_of::<FileDbMapStringInner>(), 28);
            assert_eq!(std::mem::size_of::<FileDbMapU64Inner>(), 28);
            //
            assert_eq!(std::mem::size_of::<RecordSizeStats>(), 12);
        }
    }
}
