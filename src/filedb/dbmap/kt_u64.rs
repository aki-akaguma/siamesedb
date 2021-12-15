use super::super::super::{DbMapU64, DbXxx, DbXxxKeyType};
use super::super::{
    CheckFileDbMap, CountOfPerSize, FileDbParams, FileDbXxxInner, KeysCountStats, RecordSizeStats,
};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::io::Result;
use std::path::Path;
use std::rc::Rc;

#[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
use std::convert::TryInto;

impl DbXxxKeyType for u64 {
    #[inline]
    fn signature() -> [u8; 8] {
        [b'u', b'6', b'4', 0u8, 0u8, 0u8, 0u8, 0u8]
    }
    #[cfg(feature = "vf_u32u32")]
    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        (*self as u32).to_le_bytes().to_vec()
    }
    #[cfg(feature = "vf_u32u32")]
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() == 4, "bytes.len():{} == 4", bytes.len());
        u32::from_le_bytes(bytes.try_into().unwrap()) as u64
    }
    #[cfg(feature = "vf_u64u64")]
    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    #[cfg(feature = "vf_u64u64")]
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        u64::from_le_bytes(bytes.try_into().unwrap())
    }
    #[cfg(feature = "vf_vu64")]
    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        vu64::encode(*self).as_ref().to_vec()
    }
    #[cfg(feature = "vf_vu64")]
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        vu64::decode(bytes).unwrap()
    }
}

/// List in a file databse.
#[derive(Debug, Clone)]
pub struct FileDbMapU64(Rc<RefCell<FileDbXxxInner<u64>>>);

impl FileDbMapU64 {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMapU64> {
        Ok(Self(Rc::new(RefCell::new(
            FileDbXxxInner::<u64>::open_with_params(path, ks_name, params)?,
        ))))
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        RefCell::borrow(&self.0).is_dirty()
    }
}

/// for debug
impl CheckFileDbMap for FileDbMapU64 {
    /// convert index to graph string for debug.
    fn graph_string(&self) -> Result<String> {
        RefCell::borrow(&self.0).graph_string()
    }
    /// convert index to graph string with key string for debug.
    fn graph_string_with_key_string(&self) -> Result<String> {
        RefCell::borrow_mut(&self.0).graph_string_with_key_string()
    }
    /// check the index tree is balanced
    fn is_balanced(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_balanced()
    }
    /// check it is multi search tree
    fn is_mst_valid(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_mst_valid()
    }
    /// check the node except the root and leaves of the tree has branches of half or more.
    fn is_dense(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_dense()
    }
    /// get a depth of the node tree.
    fn depth_of_node_tree(&self) -> Result<u64> {
        RefCell::borrow(&self.0).depth_of_node_tree()
    }
    /// count of free node
    fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_node()
    }
    /// count of free record
    fn count_of_free_record(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_record()
    }
    /// count of used record and used node
    fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize)> {
        RefCell::borrow(&self.0).count_of_used_node()
    }
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    fn buf_stats(&self) -> Vec<(String, i64)> {
        RefCell::borrow(&self.0).buf_stats()
    }
    /// record size statistics
    fn record_size_stats(&self) -> Result<RecordSizeStats> {
        RefCell::borrow(&self.0).record_size_stats()
    }
    /// keys count statistics
    fn keys_count_stats(&self) -> Result<KeysCountStats> {
        RefCell::borrow(&self.0).keys_count_stats()
    }
}

impl DbXxx<u64> for FileDbMapU64 {
    #[inline]
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        u64: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.borrow_mut().get(&(*key.borrow()))
    }
    #[inline]
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(key, value)
    }
    #[inline]
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        u64: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.borrow_mut().delete(&(*key.borrow()))
    }
    #[inline]
    fn read_fill_buffer(&mut self) -> Result<()> {
        self.0.borrow_mut().read_fill_buffer()
    }
    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.0.borrow_mut().flush()
    }
    #[inline]
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    #[inline]
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}
impl DbMapU64 for FileDbMapU64 {}

//--
#[cfg(test)]
mod debug {
    use super::FileDbXxxInner;
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            //
            #[cfg(not(feature = "key_cache"))]
            assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 32);
            #[cfg(feature = "key_cache")]
            {
                #[cfg(not(feature = "kc_lru"))]
                {
                    #[cfg(not(feature = "kc_hash"))]
                    assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 72);
                    #[cfg(feature = "kc_hash")]
                    assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 96);
                }
                #[cfg(feature = "kc_lru")]
                assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 72);
            }
        }
        //
        #[cfg(target_pointer_width = "32")]
        {
            //
            #[cfg(not(feature = "key_cache"))]
            assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 16);
            #[cfg(feature = "key_cache")]
            {
                #[cfg(not(feature = "kc_hash"))]
                assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 44);
                #[cfg(feature = "kc_hash")]
                assert_eq!(std::mem::size_of::<FileDbXxxInner<u64>>(), 64);
            }
        }
    }
}
