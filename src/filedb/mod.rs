use super::{DbList, DbMap};
use std::cell::RefCell;
use std::io::Result;
use std::path::Path;
use std::rc::{Rc, Weak};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyType {
    Str = 1,
    Int = 2,
}
impl KeyType {
    pub fn signature(&self) -> u8 {
        match self {
            KeyType::Str => b's',
            KeyType::Int => b'i',
        }
    }
}

mod inner;
use inner::dbxxx::{FileDbXxxInner, FileDbXxxInnerKT};
#[cfg(feature = "vf_vu64")]
use inner::vu64;
use inner::FileDbInner;

use super::DbXxx;

type CountOfPerSize = Vec<(u32, u64)>;

type RecordSizeStats = Vec<(u32, u64)>;

type FileDbMapInner = FileDbXxxInner<String>;
type FileDbListInner = FileDbXxxInner<u64>;

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

#[derive(Debug, Clone)]
pub struct FileDb(Rc<RefCell<FileDbInner>>);

#[derive(Debug, Clone)]
pub(crate) struct FileDbNode(Weak<RefCell<FileDbInner>>);

#[derive(Debug, Clone)]
pub struct FileDbMap(Rc<RefCell<FileDbMapInner>>);

#[derive(Debug, Clone)]
pub struct FileDbList(Rc<RefCell<FileDbListInner>>);

impl FileDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self(Rc::new(RefCell::new(FileDbInner::open(path)?))))
    }
    fn to_node(&self) -> FileDbNode {
        FileDbNode(Rc::downgrade(&self.0))
    }
    pub fn db_map(&self, name: &str) -> Result<FileDbMap> {
        if let Some(m) = self.0.borrow().db_map(name) {
            return Ok(m);
        }
        //
        let x = self.to_node();
        x.create_db_map(name)?;
        //
        match self.0.borrow().db_map(name) {
            Some(m) => Ok(m),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_list(&self, name: &str) -> Result<FileDbList> {
        if let Some(m) = self.0.borrow().db_list(name) {
            return Ok(m);
        }
        //
        let x = self.to_node();
        x.create_db_list(name)?;
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
    pub fn _parent(&self) -> Option<Self> {
        let rc = self.0.upgrade().expect("FileDbNode is already dispose");
        let locked = rc.borrow();
        locked._parent()
    }
    fn create_db_map(&self, name: &str) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let child: FileDbMap = FileDbMap::open(self.clone(), name)?;
        let mut locked = rc.borrow_mut();
        let _ = locked.db_map_insert(name, child);
        Ok(())
    }
    fn create_db_list(&self, name: &str) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let child: FileDbList = FileDbList::open(self.clone(), name)?;
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

impl FileDbMap {
    pub(crate) fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbMap> {
        Ok(Self(Rc::new(RefCell::new(FileDbMapInner::open(
            parent, ks_name,
        )?))))
    }
    pub fn is_dirty(&self) -> bool {
        self.0.borrow().is_dirty()
    }
}

/// for debug
impl FileDbMap {
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

impl DbMap for FileDbMap {
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

impl FileDbList {
    pub(crate) fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbList> {
        Ok(Self(Rc::new(RefCell::new(FileDbListInner::open(
            parent, ks_name,
        )?))))
    }
    pub fn is_dirty(&self) -> bool {
        self.0.borrow().is_dirty()
    }
}

/// for debug
impl FileDbList {
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

impl DbList for FileDbList {
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

//--
#[cfg(test)]
mod debug {
    #[test]
    fn test_size_of() {
        use super::{FileDb, FileDbList, FileDbMap};
        use super::{FileDbInner, FileDbListInner, FileDbMapInner};
        //
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<FileDb>(), 8);
            assert_eq!(std::mem::size_of::<FileDbMap>(), 8);
            assert_eq!(std::mem::size_of::<FileDbList>(), 8);
            //
            assert_eq!(std::mem::size_of::<FileDbInner>(), 80);
            assert_eq!(std::mem::size_of::<FileDbMapInner>(), 80);
            assert_eq!(std::mem::size_of::<FileDbListInner>(), 80);
        }
        //
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<FileDb>(), 4);
            assert_eq!(std::mem::size_of::<FileDbMap>(), 4);
            assert_eq!(std::mem::size_of::<FileDbList>(), 4);
            //
            assert_eq!(std::mem::size_of::<FileDbInner>(), 44);
            assert_eq!(std::mem::size_of::<FileDbMapInner>(), 44);
            assert_eq!(std::mem::size_of::<FileDbListInner>(), 44);
        }
    }
}
