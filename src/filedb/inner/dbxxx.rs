use super::super::super::DbXxx;
use super::super::{
    CheckFileDbMap, CountOfPerSize, FileDbNode, FileDbParams, KeysCountStats, RecordSizeStats,
};
use super::semtype::*;
use super::tr::{IdxNode, TreeNode};
use super::{dat, idx};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::io::Result;
use std::rc::Rc;

#[cfg(feature = "key_cache")]
use super::kc::KeyCacheTrait;

#[cfg(feature = "key_cache")]
use super::kc;

pub trait FileDbXxxInnerKT: Ord + Clone + Default {
    fn signature() -> [u8; 8];
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
    fn as_bytes(&self) -> Vec<u8>;
    fn from(bytes: &[u8]) -> Self;
    fn byte_len(&self) -> usize {
        self.as_bytes().len()
    }
}

#[derive(Debug)]
pub struct FileDbXxxInner<KT: FileDbXxxInnerKT> {
    parent: FileDbNode,
    dirty: bool,
    //
    dat_file: dat::DatFile<KT>,
    idx_file: idx::IdxFile,
    //
    #[cfg(feature = "key_cache")]
    key_cache: kc::KeyCache<KT>,
    //
    _phantom: std::marker::PhantomData<KT>,
}

impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    pub(crate) fn open_with_params(
        parent: FileDbNode,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbXxxInner<KT>> {
        let path = {
            let rc = parent.0.upgrade().expect("FileDbNode is already disposed");
            let locked = RefCell::borrow(&rc);
            locked.path.clone()
        };
        //
        let dat_file = dat::DatFile::open_with_params(&path, ks_name, KT::signature(), &params)?;
        let idx_file = idx::IdxFile::open_with_params(&path, ks_name, KT::signature(), &params)?;
        Ok(Self {
            parent,
            dat_file,
            idx_file,
            dirty: false,
            #[cfg(feature = "key_cache")]
            key_cache: kc::KeyCache::new(),
            _phantom: std::marker::PhantomData,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

// for utils
impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    #[cfg(feature = "key_cache")]
    fn clear_key_cache(&mut self, record_offset: RecordOffset) {
        self.key_cache.delete(&record_offset);
    }
    #[cfg(feature = "key_cache")]
    fn _clear_key_cache_all(&mut self) {
        self.key_cache.clear();
    }
    #[cfg(feature = "key_cache")]
    pub fn load_key_string(&mut self, record_offset: RecordOffset) -> Result<Rc<KT>> {
        debug_assert!(record_offset != RecordOffset::new(0));
        let string = match self.key_cache.get(&record_offset) {
            Some(s) => s,
            None => {
                let key = self.dat_file.read_record_only_key(record_offset)?;
                self.key_cache.put(&record_offset, key)
            }
        };
        Ok(string)
    }
    #[cfg(not(feature = "key_cache"))]
    pub fn load_key_string(&mut self, record_offset: RecordOffset) -> Result<Rc<KT>> {
        self.load_key_string_no_cache(record_offset)
            .map(|a| Rc::new(a))
    }
    pub fn load_key_string_no_cache(&self, record_offset: RecordOffset) -> Result<KT> {
        debug_assert!(record_offset != RecordOffset::new(0));
        self.dat_file.read_record_only_key(record_offset)
    }
    fn load_value(&self, record_offset: RecordOffset) -> Result<Vec<u8>> {
        debug_assert!(record_offset != RecordOffset::new(0));
        self.dat_file.read_record_only_value(record_offset)
    }
    fn load_record_size(&self, record_offset: RecordOffset) -> Result<RecordSize> {
        self.dat_file.read_record_only_size(record_offset)
    }

    fn keys_binary_search<Q>(
        &mut self,
        node: &TreeNode,
        key: &Q,
    ) -> Result<std::result::Result<usize, usize>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        /*
        match node.keys.binary_search_by(|&key_offset| {
            debug_assert!(!key_offset.is_zero());
            let key_string = self.load_key_string(key_offset).unwrap();
            key_string.as_ref().borrow().cmp(key)
        }) {
            Ok(k) => Ok(Ok(k)),
            Err(k) => Ok(Err(k)),
        }
        */
        /*
         */
        let mut left = 0;
        let mut right = node.keys_len();
        while left < right {
            //let mid = left + (right - left) / 2;
            let mid = (left + right) / 2;
            //
            // SAFETY: `mid` is limited by `[left; right)` bound.
            let key_offset = unsafe { node.keys_get_unchecked(mid) };
            //let key_offset = node.keys[mid];
            //
            debug_assert!(key_offset != RecordOffset::new(0));
            let key_string = self.load_key_string(key_offset)?;
            //
            match key.cmp(key_string.as_ref().borrow()) {
                Ordering::Greater => left = mid + 1,
                Ordering::Less => right = mid,
                Ordering::Equal => {
                    return Ok(Ok(mid));
                }
            }
        }
        Ok(Err(left))
    }

    #[cfg(feature = "node_dm32")]
    fn load_node_keys_len(&mut self, node_offset: NodeOffset) -> Result<usize> {
        self.idx_file.read_node_keys_len(node_offset)
    }
    #[cfg(feature = "node_dm32")]
    fn load_node_keys_get(&mut self, node_offset: NodeOffset, idx: usize) -> Result<RecordOffset> {
        self.idx_file.read_node_keys_get(node_offset, idx)
    }
    #[cfg(feature = "node_dm32")]
    fn load_node_downs_get(&mut self, node_offset: NodeOffset, idx: usize) -> Result<NodeOffset> {
        self.idx_file.read_node_downs_get(node_offset, idx)
    }

    #[cfg(feature = "node_dm32")]
    fn keys_binary_search_offset<Q>(
        &mut self,
        node_offset: NodeOffset,
        key: &Q,
    ) -> Result<std::result::Result<usize, usize>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        /*
        match node.keys.binary_search_by(|&key_offset| {
            debug_assert!(!key_offset.is_zero());
            let key_string = self.load_key_string(key_offset).unwrap();
            key_string.as_ref().borrow().cmp(key)
        }) {
            Ok(k) => Ok(Ok(k)),
            Err(k) => Ok(Err(k)),
        }
        */
        /*
         */
        let mut left = 0;
        let mut right = self.load_node_keys_len(node_offset)?;
        while left < right {
            //let mid = left + (right - left) / 2;
            let mid = (left + right) / 2;
            //
            // SAFETY: `mid` is limited by `[left; right)` bound.
            let key_offset = self.load_node_keys_get(node_offset, mid)?;
            //let key_offset = node.keys[mid];
            //
            debug_assert!(key_offset != RecordOffset::new(0));
            let key_string = self.load_key_string(key_offset)?;
            //
            match key.cmp(key_string.as_ref().borrow()) {
                Ordering::Greater => left = mid + 1,
                Ordering::Less => right = mid,
                Ordering::Equal => {
                    return Ok(Ok(mid));
                }
            }
        }
        Ok(Err(left))
    }

    #[inline]
    fn write_node(&mut self, node: IdxNode) -> Result<IdxNode> {
        self.dirty = true;
        self.idx_file.write_node(node)
    }
    #[inline]
    fn write_new_node(&mut self, node: IdxNode) -> Result<IdxNode> {
        self.dirty = true;
        self.idx_file.write_new_node(node)
    }
}

// for debug
impl<KT: FileDbXxxInnerKT + std::fmt::Display + std::default::Default + std::cmp::PartialOrd>
    CheckFileDbMap for FileDbXxxInner<KT>
{
    /// convert the index node tree to graph string for debug.
    fn graph_string(&self) -> Result<String> {
        self.idx_file.graph_string()
    }
    /// convert the index node tree to graph string for debug.
    fn graph_string_with_key_string(&self) -> Result<String> {
        self.idx_file.graph_string_with_key_string(self)
    }
    /// check the index node tree is balanced
    fn is_balanced(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_balanced(&top_node)
    }
    /// check the index node tree is multi search tree
    fn is_mst_valid(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_mst_valid(&top_node, self)
    }
    /// check the index node except the root and leaves of the tree has branches of hm or more.
    fn is_dense(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_dense(&top_node)
    }
    /// get the depth of the index node
    fn depth_of_node_tree(&self) -> Result<u64> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.depth_of_node_tree(&top_node)
    }
    /// count of the free node
    fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        self.idx_file.count_of_free_node()
    }
    /// count of the free record
    fn count_of_free_record(&self) -> Result<CountOfPerSize> {
        self.dat_file.count_of_free_record()
    }
    /// count of the used record and the used node
    fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize)> {
        self.idx_file
            .count_of_used_node(|off| self.load_record_size(off))
    }
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    fn buf_stats(&self) -> Vec<(String, i64)> {
        let mut vec = self.dat_file.buf_stats();
        let mut vec2 = self.idx_file.buf_stats();
        vec.append(&mut vec2);
        vec
    }
    /// record size statistics
    fn record_size_stats(&self) -> Result<RecordSizeStats> {
        self.idx_file
            .record_size_stats(|off| self.load_record_size(off))
    }
    /// keys count statistics
    fn keys_count_stats(&self) -> Result<KeysCountStats> {
        self.idx_file.keys_count_stats()
    }
}

// insert: NEW
impl<KT: FileDbXxxInnerKT + Ord> FileDbXxxInner<KT> {
    fn insert_into_node_tree(
        &mut self,
        mut node_: IdxNode,
        key: &KT,
        value: &[u8],
    ) -> Result<IdxNode> {
        let r = {
            let node = node_.get_ref();
            if node.keys_is_empty() {
                let new_record = self.dat_file.add_record(key, value)?;
                return Ok(IdxNode::new_active(
                    new_record.offset,
                    NodeOffset::new(0),
                    NodeOffset::new(0),
                ));
            }
            self.keys_binary_search(&node, key)?
        };
        match r {
            Ok(k) => {
                let record_offset = unsafe { node_.get_ref().keys_get_unchecked(k) };
                //let record_offset = node.keys[k];
                debug_assert!(record_offset != RecordOffset::new(0));
                let new_record_offset = self.store_value_on_insert(record_offset, value)?;
                if record_offset != new_record_offset {
                    node_.get_mut().keys_set(k, new_record_offset);
                    return self.write_node(node_);
                }
                Ok(node_)
            }
            Err(k) => {
                let node_offset1 = unsafe { node_.get_ref().downs_get_unchecked(k) };
                //let node_offset1 = node.downs[k];
                let node2_ = if !node_offset1.is_zero() {
                    let node1_ = self.idx_file.read_node(node_offset1)?;
                    self.insert_into_node_tree(node1_, key, value)?
                } else {
                    let new_record = self.dat_file.add_record(key, value)?;
                    IdxNode::new_active(new_record.offset, NodeOffset::new(0), NodeOffset::new(0))
                };
                if node2_.is_active_on_insert() {
                    self.balance_on_insert(node_, k, &node2_)
                } else {
                    debug_assert!(!node2_.get_ref().offset().is_zero());
                    let node2_ = self.write_node(node2_)?;
                    node_.get_mut().downs_set(k, node2_.get_ref().offset());
                    self.write_node(node_)
                }
            }
        }
    }
    #[inline]
    fn store_value_on_insert(
        &mut self,
        record_offset: RecordOffset,
        value: &[u8],
    ) -> Result<RecordOffset> {
        let mut record = self.dat_file.read_record(record_offset)?;
        record.value = value.to_vec();
        let new_record = self.dat_file.write_record(record)?;
        Ok(new_record.offset)
    }
    #[inline]
    fn balance_on_insert(
        &mut self,
        mut node_: IdxNode,
        i: usize,
        active_node_: &IdxNode,
    ) -> Result<IdxNode> {
        debug_assert!(active_node_.get_ref().is_active_on_insert());
        //
        node_
            .get_mut()
            .keys_insert(i, active_node_.get_ref().keys_get(0));
        node_
            .get_mut()
            .downs_set(i, active_node_.get_ref().downs_get(1));
        node_
            .get_mut()
            .downs_insert(i, active_node_.get_ref().downs_get(0));
        //
        if node_.borrow().is_over_len() {
            self.split_on_insert(node_)
        } else {
            let node = self.write_node(node_)?;
            Ok(node)
        }
    }
    #[inline]
    fn split_on_insert(&mut self, mut node_: IdxNode) -> Result<IdxNode> {
        debug_assert!(node_.get_ref().keys_len() == idx::NODE_SLOTS_MAX as usize);
        debug_assert!(node_.get_ref().downs_len() == idx::NODE_SLOTS_MAX as usize + 1);
        debug_assert!(node_.get_ref().keys_len() >= idx::NODE_SLOTS_MAX_HALF as usize);
        debug_assert!(node_.get_ref().downs_len() >= idx::NODE_SLOTS_MAX_HALF as usize);
        let mut node1_ = IdxNode::new(NodeOffset::new(0));
        node1_
            .get_mut()
            .keys_extend_from_node(&node_.get_ref(), idx::NODE_SLOTS_MAX_HALF as usize);
        node1_
            .get_mut()
            .downs_extend_from_node(&node_.get_ref(), idx::NODE_SLOTS_MAX_HALF as usize);
        //
        node_
            .get_mut()
            .keys_resize(idx::NODE_SLOTS_MAX_HALF as usize);
        node_
            .get_mut()
            .downs_resize(idx::NODE_SLOTS_MAX_HALF as usize);
        //
        let key_offset1 = node_.get_mut().keys_pop().unwrap();
        let node1_ = self.write_new_node(node1_)?;
        let node_ = self.write_node(node_)?;
        let node_offset = node_.get_ref().offset();
        let node1_offset = node1_.get_ref().offset();
        Ok(IdxNode::new_active(key_offset1, node_offset, node1_offset))
    }
}

// delete: NEW
impl<KT: FileDbXxxInnerKT + Ord> FileDbXxxInner<KT> {
    fn delete_from_node_tree<Q>(&mut self, mut node_: IdxNode, key: &Q) -> Result<IdxNode>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        if node_.get_ref().keys_is_empty() {
            return Ok(node_);
        }
        let r = self.keys_binary_search(&node_.get_ref(), key)?;
        match r {
            Ok(k) => {
                let node_ = self.delete_at(node_, k)?;
                return Ok(node_);
            }
            Err(k) => {
                let node_offset1 = unsafe { node_.get_mut().downs_get_unchecked(k) };
                //let node_offset1 = node.downs[k];
                if !node_offset1.is_zero() {
                    let node1_ = self.idx_file.read_node(node_offset1)?;
                    let node1_ = self.delete_from_node_tree(node1_, key)?;
                    node_.get_mut().downs_set(k, node1_.get_ref().offset());
                    let node_ = self.write_node(node_)?;
                    if k == node_.get_ref().downs_len() - 1 {
                        let node_ = self.balance_right(node_, k)?;
                        return Ok(node_);
                    } else {
                        let node_ = self.balance_left(node_, k)?;
                        return Ok(node_);
                    }
                }
            }
        }
        Ok(node_)
    }
    fn delete_at(&mut self, mut node_: IdxNode, i: usize) -> Result<IdxNode> {
        let record_offset = node_.get_ref().keys_get(i);
        debug_assert!(
            record_offset != RecordOffset::new(0),
            "key_offset: {} != 0",
            record_offset
        );
        {
            #[cfg(feature = "key_cache")]
            self.clear_key_cache(record_offset);
            self.dat_file.delete_record(record_offset)?;
        }
        let node_offset1 = node_.get_ref().downs_get(i);
        if node_offset1.is_zero() {
            let _key_offset = node_.get_mut().keys_remove(i);
            let _node_offset = node_.get_mut().downs_remove(i);
            let new_node_ = self.write_node(node_)?;
            Ok(new_node_)
        } else {
            let node1_ = self.idx_file.read_node(node_offset1)?;
            let (record_offset, node1_) = self.delete_max(node1_)?;
            node_.get_mut().keys_set(i, record_offset);
            node_.get_mut().downs_set(i, node1_.get_ref().offset());
            let node_ = self.write_node(node_)?;
            self.balance_left(node_, i)
        }
    }
    fn delete_max(&mut self, mut node_: IdxNode) -> Result<(RecordOffset, IdxNode)> {
        let j = node_.get_ref().keys_len();
        let i = j - 1;
        let node_offset1 = node_.get_ref().downs_get(j);
        if node_offset1.is_zero() {
            node_.get_mut().downs_remove(j);
            let key_offset2 = node_.get_mut().keys_remove(i);
            let new_node_ = self.write_node(node_)?;
            Ok((key_offset2, new_node_))
        } else {
            let node1_ = self.idx_file.read_node(node_offset1)?;
            let (key_offset2, node1_) = self.delete_max(node1_)?;
            node_.get_mut().downs_set(j, node1_.get_ref().offset());
            let node_ = self.write_node(node_)?;
            let new_node_ = self.balance_right(node_, j)?;
            Ok((key_offset2, new_node_))
        }
    }
    fn balance_left(&mut self, mut node_: IdxNode, i: usize) -> Result<IdxNode> {
        let node_offset1 = node_.get_ref().downs_get(i);
        if node_offset1.is_zero() {
            return Ok(node_);
        }
        let mut node1_ = self.idx_file.read_node(node_offset1)?;
        if !node1_.is_active_on_delete() {
            return Ok(node_);
        }
        let j = i + 1;
        let key_offset2 = node_.get_ref().keys_get(i);
        let node_offset2 = node_.get_ref().downs_get(j);
        debug_assert!(!node_offset2.is_zero());
        if !node_offset2.is_zero() {
            let mut node2_ = self.idx_file.read_node(node_offset2)?;
            if node2_.get_ref().downs_len() == idx::NODE_SLOTS_MAX_HALF as usize {
                // unification
                node1_.get_mut().keys_push(key_offset2);
                //
                node1_.get_mut().keys_extend_from_node(&node2_.get_ref(), 0);
                node1_
                    .get_mut()
                    .downs_extend_from_node(&node2_.get_ref(), 0);
                self.idx_file.delete_node(node2_)?;
                //
                node_.get_mut().keys_remove(i);
                node_.get_mut().downs_remove(j);
                //
                let node1_ = self.write_node(node1_)?;
                node_.get_mut().downs_set(i, node1_.get_ref().offset());
            } else {
                let key_offset3 =
                    self.move_a_node_from_right_to_left(key_offset2, &mut node1_, &mut node2_);
                node_.get_mut().keys_set(i, key_offset3);
                let node2_ = self.write_node(node2_)?;
                let node1_ = self.write_node(node1_)?;
                node_.get_mut().downs_set(j, node2_.get_ref().offset());
                node_.get_mut().downs_set(i, node1_.get_ref().offset());
            }
            let new_node_ = self.write_node(node_)?;
            return Ok(new_node_);
        }
        Ok(node_)
    }
    fn balance_right(&mut self, mut node_: IdxNode, j: usize) -> Result<IdxNode> {
        let node_offset1 = node_.get_ref().downs_get(j);
        if node_offset1.is_zero() {
            return Ok(node_);
        }
        let mut node1_ = self.idx_file.read_node(node_offset1)?;
        if !node1_.is_active_on_delete() {
            return Ok(node_);
        }
        let i = j - 1;
        let key_offset2 = node_.get_ref().keys_get(i);
        let node_offset2 = node_.get_ref().downs_get(i);
        debug_assert!(!node_offset2.is_zero());
        if !node_offset2.is_zero() {
            let mut node2_ = self.idx_file.read_node(node_offset2)?;
            if node2_.get_ref().downs_len() == idx::NODE_SLOTS_MAX_HALF as usize {
                // unification
                node2_.get_mut().keys_push(key_offset2);
                //
                node2_.get_mut().keys_extend_from_node(&node1_.get_ref(), 0);
                node2_
                    .get_mut()
                    .downs_extend_from_node(&node1_.get_ref(), 0);
                self.idx_file.delete_node(node1_)?;
                //
                node_.get_mut().keys_remove(i);
                node_.get_mut().downs_remove(j);
                //
                let node2_ = self.write_node(node2_)?;
                node_.get_mut().downs_set(i, node2_.get_ref().offset());
            } else {
                let key_offset3 = self.move_left_right(key_offset2, &mut node2_, &mut node1_);
                node_.get_mut().keys_set(i, key_offset3);
                let node1_ = self.write_node(node1_)?;
                let node2_ = self.write_node(node2_)?;
                node_.get_mut().downs_set(j, node1_.get_ref().offset());
                node_.get_mut().downs_set(i, node2_.get_ref().offset());
            }
            let new_node_ = self.write_node(node_)?;
            return Ok(new_node_);
        }
        Ok(node_)
    }
    fn move_a_node_from_right_to_left(
        &mut self,
        record_offset: RecordOffset,
        node_l: &mut IdxNode,
        node_r: &mut IdxNode,
    ) -> RecordOffset {
        node_l.get_mut().keys_push(record_offset);
        node_l
            .get_mut()
            .downs_push(node_r.get_mut().downs_remove(0));
        node_r.get_mut().keys_remove(0)
    }
    fn move_left_right(
        &mut self,
        record_offset: RecordOffset,
        node_l: &mut IdxNode,
        node_r: &mut IdxNode,
    ) -> RecordOffset {
        let j = node_l.get_ref().keys_len();
        let i = j - 1;
        node_r.get_mut().keys_insert(0, record_offset);
        node_r
            .get_mut()
            .downs_insert(0, node_l.get_mut().downs_remove(j));
        node_l.get_mut().keys_remove(i)
    }
    #[inline]
    fn trim(&self, node_: IdxNode) -> Result<IdxNode> {
        if node_.get_ref().downs_len() == 1 {
            let node_offset1 = node_.get_ref().downs_get(0);
            if !node_offset1.is_zero() {
                let node1_ = self.idx_file.read_node(node_offset1)?;
                self.idx_file.delete_node(node_)?;
                return Ok(node1_);
            }
        }
        Ok(node_)
    }
}

// find: NEW
impl<KT: FileDbXxxInnerKT + Ord> FileDbXxxInner<KT> {
    #[cfg(not(feature = "node_dm32"))]
    fn find_in_node_tree<Q>(&mut self, node_: &mut IdxNode, key: &Q) -> Result<Option<Vec<u8>>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let r = {
            let node = node_.get_ref();
            if node.keys_is_empty() {
                return Ok(None);
            }
            self.keys_binary_search(&node, key)?
        };
        match r {
            Ok(k) => {
                let key_offset = unsafe { node_.get_ref().keys_get_unchecked(k) };
                //let key_offset = node_.get_ref().keys[k];
                debug_assert!(key_offset != RecordOffset::new(0));
                self.load_value(key_offset).map(Some)
            }
            Err(k) => {
                let node_offset1 = unsafe { node_.get_ref().downs_get_unchecked(k) };
                //let node_offset1 = node_.get_ref().downs[k];
                if !node_offset1.is_zero() {
                    let mut node1_ = self.idx_file.read_node(node_offset1)?;
                    self.find_in_node_tree(&mut node1_, key)
                } else {
                    Ok(None)
                }
            }
        }
    }
    #[cfg(feature = "node_dm32")]
    fn find_in_node_tree_offset<Q>(
        &mut self,
        node_offset: NodeOffset,
        key: &Q,
    ) -> Result<Option<Vec<u8>>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let r = {
            if self.load_node_keys_len(node_offset)? == 0 {
                return Ok(None);
            }
            self.keys_binary_search_offset(node_offset, key)?
        };
        match r {
            Ok(k) => {
                let key_offset = self.load_node_keys_get(node_offset, k)?;
                //let key_offset = node_.get_ref().keys[k];
                debug_assert!(key_offset != RecordOffset::new(0));
                self.load_value(key_offset).map(Some)
            }
            Err(k) => {
                let node_offset1 = self.load_node_downs_get(node_offset, k)?;
                //let node_offset1 = node_.get_ref().downs[k];
                if !node_offset1.is_zero() {
                    self.find_in_node_tree_offset(node_offset1, key)
                } else {
                    Ok(None)
                }
            }
        }
    }
    fn has_key_in_node_tree<Q>(&mut self, node_: &mut IdxNode, key: &Q) -> Result<bool>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let r = {
            let node = node_.get_ref();
            if node.keys_is_empty() {
                return Ok(false);
            }
            self.keys_binary_search(&node, key)?
        };
        match r {
            Ok(_k) => Ok(true),
            Err(k) => {
                let node_offset1 = unsafe { node_.get_ref().downs_get_unchecked(k) };
                //let node_offset1 = node_.get_ref().downs[k];
                if !node_offset1.is_zero() {
                    let mut node1_ = self.idx_file.read_node(node_offset1)?;
                    self.has_key_in_node_tree(&mut node1_, key)
                } else {
                    Ok(false)
                }
            }
        }
    }
}

impl<KT: FileDbXxxInnerKT + Ord> DbXxx<KT> for FileDbXxxInner<KT> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        #[cfg(not(feature = "node_dm32"))]
        {
            let mut top_node = self.idx_file.read_top_node()?;
            self.find_in_node_tree(&mut top_node, key)
        }
        #[cfg(feature = "node_dm32")]
        {
            self.flush()?;
            let top_node = self.idx_file.read_top_node()?;
            let node_offset = top_node.get_ref().offset();
            self.find_in_node_tree_offset(node_offset, key)
        }
    }
    fn put(&mut self, key: KT, value: &[u8]) -> Result<()>
    where
        KT: Ord,
    {
        let top_node = self.idx_file.read_top_node()?;
        let active_node = self.insert_into_node_tree(top_node, &key, value)?;
        let new_top_node = active_node.deactivate();
        self.idx_file.write_top_node(new_top_node)?;
        Ok(())
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let top_node = self.idx_file.read_top_node()?;
        let top_node_offset = top_node.get_ref().offset();
        let top_node = self.delete_from_node_tree(top_node, key)?;
        let new_top_node = self.trim(top_node)?;
        if top_node_offset != new_top_node.get_ref().offset() {
            self.idx_file.write_top_node(new_top_node)?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data
            self.dat_file.flush()?;
            self.idx_file.flush()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn sync_all(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data and meta
            self.dat_file.sync_all()?;
            self.idx_file.sync_all()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data
            self.dat_file.sync_data()?;
            self.idx_file.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn has_key<Q>(&mut self, key: &Q) -> Result<bool>
    where
        KT: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let mut top_node = self.idx_file.read_top_node()?;
        self.has_key_in_node_tree(&mut top_node, &(*key.borrow()))
    }
}
