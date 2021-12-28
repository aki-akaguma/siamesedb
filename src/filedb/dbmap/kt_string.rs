use super::super::super::{DbXxxKeyType, HashValue};
use super::FileDbMap;

impl DbXxxKeyType for String {
    #[inline]
    fn signature() -> [u8; 8] {
        [b's', b't', b'r', b'i', b'n', b'g', 0u8, 0u8]
    }
    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        String::from_utf8_lossy(bytes).to_string()
    }
    #[inline]
    fn byte_len(&self) -> usize {
        self.as_bytes().len()
    }
    fn cmp_u8(&self, other: &[u8]) -> std::cmp::Ordering {
        self.as_str().as_bytes().cmp(other)
    }
}
impl HashValue for String {}

/// String Map in a file database.
pub type FileDbMapString = FileDbMap<String>;

/*
use super::super::super::{DbMapString, DbXxx, DbXxxKeyType};
use super::super::{
    CheckFileDbMap, CountOfPerSize, FileDbParams, FileDbXxxInner, Key, KeysCountStats, LengthStats,
    RecordSizeStats, Value, DbXxxIterMut, DbXxxIntoIter
};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::io::Result;
use std::path::Path;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct FileDbMapString(Rc<RefCell<FileDbXxxInner<String>>>);

impl FileDbMapString {
    pub(crate) fn open<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbMapString> {
        Ok(Self(Rc::new(RefCell::new(
            FileDbXxxInner::<String>::open_with_params(path, ks_name, params)?,
        ))))
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        RefCell::borrow(&self.0).is_dirty()
    }
}

/// for debug
impl CheckFileDbMap for FileDbMapString {
    /// convert the index node tree to graph string for debug.
    fn graph_string(&self) -> Result<String> {
        RefCell::borrow(&self.0).graph_string()
    }
    /// convert the index node tree to graph string for debug.
    fn graph_string_with_key_string(&self) -> Result<String> {
        RefCell::borrow_mut(&self.0).graph_string_with_key_string()
    }
    /// check the index node tree is balanced
    fn is_balanced(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_balanced()
    }
    /// check the index node tree is multi search tree
    fn is_mst_valid(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_mst_valid()
    }
    /// check the index node except the root and leaves of the tree has branches of hm or more.
    fn is_dense(&self) -> Result<bool> {
        RefCell::borrow(&self.0).is_dense()
    }
    /// get the depth of the index node.
    fn depth_of_node_tree(&self) -> Result<u64> {
        RefCell::borrow(&self.0).depth_of_node_tree()
    }
    /// count of the free node
    fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_node()
    }
    /// count of the free record
    fn count_of_free_record(&self) -> Result<CountOfPerSize> {
        RefCell::borrow(&self.0).count_of_free_record()
    }
    /// count of the used record and the used node
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
    /// key length statistics
    fn key_length_stats(&self) -> Result<LengthStats<Key>> {
        RefCell::borrow(&self.0).key_length_stats()
    }
    /// value length statistics
    fn value_length_stats(&self) -> Result<LengthStats<Value>> {
        RefCell::borrow(&self.0).value_length_stats()
    }
}

impl DbXxx<String> for FileDbMapString {
    #[inline]
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).get(&(*key.borrow()))
    }
    #[inline]
    fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put(key, value)
    }
    #[inline]
    fn bulk_put(&mut self, bulk: &[(String, &[u8])]) -> Result<()> {
        RefCell::borrow_mut(&self.0).bulk_put(bulk)
    }
    #[inline]
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).delete(&(*key.borrow()))
    }
    #[inline]
    fn read_fill_buffer(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).read_fill_buffer()
    }
    #[inline]
    fn flush(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).flush()
    }
    #[inline]
    fn sync_all(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_all()
    }
    #[inline]
    fn sync_data(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_data()
    }
    #[inline]
    fn has_key<Q>(&mut self, key: &Q) -> Result<bool>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).has_key(&(*key.borrow()))
    }
}
impl DbMapString for FileDbMapString {}

// for iter
impl FileDbMapString {
    pub fn iter(&mut self) -> DbXxxIterMut<String> {
        DbXxxIterMut::new(self.0.clone()).unwrap()
    }
    pub fn iter_mut(&mut self) -> DbXxxIterMut<String> {
        DbXxxIterMut::new(self.0.clone()).unwrap()
    }
}

// impl trait: IntoIterator
impl IntoIterator for FileDbMapString {
    type Item = (String, Vec<u8>);
    type IntoIter = DbXxxIntoIter<String>;
    //
    fn into_iter(self) -> DbXxxIntoIter<String> {
        DbXxxIntoIter::new(self.0.clone()).unwrap()
    }
}

impl IntoIterator for &FileDbMapString {
    type Item = (String, Vec<u8>);
    type IntoIter = DbXxxIterMut<String>;
    //
    fn into_iter(self) -> DbXxxIterMut<String> {
        DbXxxIterMut::new(self.0.clone()).unwrap()
    }
}

impl IntoIterator for &mut FileDbMapString {
    type Item = (String, Vec<u8>);
    type IntoIter = DbXxxIterMut<String>;
    //
    fn into_iter(self) -> DbXxxIterMut<String> {
        DbXxxIterMut::new(self.0.clone()).unwrap()
    }
}
*/

/*
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
            assert_eq!(std::mem::size_of::<FileDbXxxInner<String>>(), 24);
            #[cfg(feature = "key_cache")]
            {
                #[cfg(not(feature = "kc_lru"))]
                assert_eq!(std::mem::size_of::<FileDbXxxInner<String>>(), 56);
                #[cfg(feature = "kc_lru")]
                assert_eq!(std::mem::size_of::<FileDbXxxInner<String>>(), 72);
            }
        }
        //
        #[cfg(target_pointer_width = "32")]
        {
            //
            #[cfg(not(feature = "key_cache"))]
            assert_eq!(std::mem::size_of::<FileDbXxxInner<String>>(), 12);
            #[cfg(feature = "key_cache")]
            assert_eq!(std::mem::size_of::<FileDbXxxInner<String>>(), 28);
        }
    }
}
*/
