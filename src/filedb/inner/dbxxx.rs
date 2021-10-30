use super::super::super::DbXxx;
use super::super::{CountOfPerSize, FileDbNode};
use super::kc::KeyCacheTrait;
use super::semtype::*;
use super::{dat, idx, kc};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Result;
use std::rc::Rc;

use super::super::RecordSizeStats;

pub trait FileDbXxxInnerKT {
    fn signature() -> [u8; 8];
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
    fn as_bytes(&self) -> Vec<u8>;
    fn from(bytes: &[u8]) -> Self;
}

#[derive(Debug)]
pub struct FileDbXxxInner<KT: FileDbXxxInnerKT> {
    parent: FileDbNode,
    mem: BTreeMap<String, (u64, Vec<u8>)>,
    dirty: bool,
    //
    dat_file: dat::DatFile,
    idx_file: idx::IdxFile,
    //
    key_cache: kc::KeyCache<KT>,
    //
    _phantom: std::marker::PhantomData<KT>,
}

impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    pub(crate) fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbXxxInner<KT>> {
        let path = {
            let rc = parent.0.upgrade().expect("FileDbNode is already disposed");
            let locked = rc.borrow();
            locked.path.clone()
        };
        //
        let dat_file = dat::DatFile::open(&path, ks_name, KT::signature())?;
        let idx_file = idx::IdxFile::open(&path, ks_name, KT::signature())?;
        Ok(Self {
            parent,
            dat_file,
            idx_file,
            mem: BTreeMap::new(),
            dirty: false,
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
    fn clear_key_cache(&mut self, record_offset: RecordOffset) {
        self.key_cache.delete(&record_offset);
    }
    fn _clear_key_cache_all(&mut self) {
        self.key_cache.clear();
    }
    pub fn load_key_string(&mut self, record_offset: RecordOffset) -> Result<Rc<KT>> {
        debug_assert!(record_offset != RecordOffset::new(0));
        let string = match self.key_cache.get(&record_offset) {
            Some(s) => s,
            None => {
                let vec = self.dat_file.read_record_key(record_offset)?.unwrap();
                self.key_cache.put(&record_offset, KT::from(&vec)).unwrap()
            }
        };
        Ok(string)
    }
    pub fn load_key_string_no_cache(&self, record_offset: RecordOffset) -> Result<KT> {
        debug_assert!(record_offset != RecordOffset::new(0));
        let vec = self.dat_file.read_record_key(record_offset)?.unwrap();
        Ok(KT::from(&vec))
    }
    fn load_value(&self, record_offset: RecordOffset) -> Result<Option<Vec<u8>>> {
        debug_assert!(record_offset != RecordOffset::new(0));
        Ok(self
            .dat_file
            .read_record(record_offset)?
            .map(|(_key, val)| val))
    }
    fn load_record_size(&self, record_offset: RecordOffset) -> Result<RecordSize> {
        self.dat_file.read_record_size(record_offset)
    }
    fn keys_binary_search(
        &mut self,
        node: &mut idx::IdxNode,
        key: &KT,
    ) -> Result<std::result::Result<usize, usize>> {
        let mut size = node.keys.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            //
            // SAFETY: `mid` is limited by `[left; right)` bound.
            let key_offset = unsafe { *node.keys.get_unchecked(mid) };
            //let key_offset = node.keys[mid];
            //
            debug_assert!(key_offset != RecordOffset::new(0));
            let key_string = self.load_key_string(key_offset)?;
            //
            let cmp = key.cmp(&key_string);
            match cmp {
                Ordering::Less => right = mid,
                Ordering::Greater => left = mid + 1,
                Ordering::Equal => {
                    return Ok(Ok(mid));
                }
            }
            //
            size = right - left;
        }
        Ok(Err(left))
    }
}

// for debug
impl<KT: FileDbXxxInnerKT + std::fmt::Display> FileDbXxxInner<KT> {
    // convert index to graph string for debug.
    pub fn to_graph_string(&self) -> Result<String> {
        self.idx_file.to_graph_string()
    }
    pub fn to_graph_string_with_key_string(&self) -> Result<String> {
        self.idx_file.to_graph_string_with_key_string(self)
        //.to_graph_string_with_key_string(self.dat_file.clone())
    }
    // check the index tree is balanced
    pub fn is_balanced(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_balanced(&top_node)
    }
    // check it is multi search tree
    pub fn is_mst_valid(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_mst_valid(&top_node, self.dat_file.clone())
    }
    // check the node except the root and leaves of the tree has branches of hm or more.
    pub fn is_dense(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_dense(&top_node)
    }
    // get depth of node tree
    pub fn depth_of_node_tree(&self) -> Result<u64> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.depth_of_node_tree(&top_node)
    }
    // count of free node
    pub fn count_of_free_node(&self) -> Result<CountOfPerSize> {
        self.idx_file.count_of_free_node()
    }
    // count of free record
    pub fn count_of_free_record(&self) -> Result<CountOfPerSize> {
        self.dat_file.count_of_free_record()
    }
    // count of used record and used node
    pub fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize)> {
        self.idx_file
            .count_of_used_node(|off| self.load_record_size(off))
    }
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        let mut vec = self.dat_file.buf_stats();
        let mut vec2 = self.idx_file.buf_stats();
        vec.append(&mut vec2);
        vec
    }
    /// record size statistics
    pub fn record_size_stats(&self) -> Result<RecordSizeStats> {
        self.idx_file
            .record_size_stats(|off| self.load_record_size(off))
    }
}

// insert: NEW
impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    fn insert_into_node_tree(
        &mut self,
        mut node: idx::IdxNode,
        key: &KT,
        value: &[u8],
    ) -> Result<idx::IdxNode> {
        if node.keys.is_empty() {
            let new_key_offset = self.dat_file.add_record(&key.as_bytes(), value)?;
            return Ok(idx::IdxNode::new_active(
                new_key_offset,
                NodeOffset::new(0),
                NodeOffset::new(0),
            ));
        }
        let r = self.keys_binary_search(&mut node, key)?;
        match r {
            Ok(k) => {
                let record_offset = node.keys[k];
                debug_assert!(record_offset != RecordOffset::new(0));
                let new_record_offset = self.store_value_on_insert(record_offset, value)?;
                if record_offset != new_record_offset {
                    node.keys[k] = new_record_offset;
                    self.dirty = true;
                    return self.idx_file.write_node(node);
                }
                Ok(node)
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                let node2 = if !node_offset1.is_zero() {
                    let node1 = self.idx_file.read_node(node_offset1)?;
                    self.insert_into_node_tree(node1, key, value)?
                } else {
                    let new_key_offset = self.dat_file.add_record(&key.as_bytes(), value)?;
                    idx::IdxNode::new_active(new_key_offset, NodeOffset::new(0), NodeOffset::new(0))
                };
                if node2.is_active_on_insert() {
                    self.balance_on_insert(node, k, &node2)
                } else {
                    debug_assert!(!node2.offset.is_zero());
                    let node2 = self.idx_file.write_node(node2)?;
                    node.downs[k] = node2.offset;
                    self.dirty = true;
                    self.idx_file.write_node(node)
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
        if let Some(r_key) = self.dat_file.read_record_key(record_offset)? {
            let new_key_offset = self.dat_file.write_record(record_offset, &r_key, value)?;
            Ok(new_key_offset)
        } else {
            panic!("dat_file.read_record({})", record_offset);
        }
    }
    #[inline]
    fn balance_on_insert(
        &mut self,
        mut node: idx::IdxNode,
        i: usize,
        active_node: &idx::IdxNode,
    ) -> Result<idx::IdxNode> {
        debug_assert!(active_node.is_active_on_insert());
        //
        node.keys.insert(i, active_node.keys[0]);
        node.downs[i] = active_node.downs[1];
        node.downs.insert(i, active_node.downs[0]);
        //
        if node.is_over_len() {
            self.split_on_insert(node)
        } else {
            let node = self.idx_file.write_node(node)?;
            Ok(node)
        }
    }
    #[inline]
    fn split_on_insert(&mut self, mut node: idx::IdxNode) -> Result<idx::IdxNode> {
        let mut node1 = idx::IdxNode::new(NodeOffset::new(0));
        let slice = &node.keys[idx::NODE_SLOTS_MAX_HALF as usize..node.keys.len()];
        node1.keys.extend_from_slice(slice);
        let slice = &node.downs[idx::NODE_SLOTS_MAX_HALF as usize..node.downs.len()];
        node1.downs.extend_from_slice(slice);
        //
        node.keys
            .resize(idx::NODE_SLOTS_MAX_HALF as usize, RecordOffset::new(0));
        node.downs
            .resize(idx::NODE_SLOTS_MAX_HALF as usize, NodeOffset::new(0));
        //
        let key_offset1 = node.keys.remove(idx::NODE_SLOTS_MAX_HALF as usize - 1);
        let node1 = self.idx_file.write_new_node(node1)?;
        let node = self.idx_file.write_node(node)?;
        Ok(idx::IdxNode::new_active(
            key_offset1,
            node.offset,
            node1.offset,
        ))
    }
}

// delete: NEW
impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    fn delete_from_node_tree(&mut self, mut node: idx::IdxNode, key: &KT) -> Result<idx::IdxNode> {
        if node.keys.is_empty() {
            return Ok(node);
        }
        let r = self.keys_binary_search(&mut node, key)?;
        match r {
            Ok(k) => {
                let node = self.delete_at(node, k)?;
                return Ok(node);
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                if !node_offset1.is_zero() {
                    let node1 = self.idx_file.read_node(node_offset1)?;
                    let node1 = self.delete_from_node_tree(node1, key)?;
                    node.downs[k] = node1.offset;
                    let node = self.idx_file.write_node(node)?;
                    if k == node.downs.len() - 1 {
                        let node = self.balance_right(node, k)?;
                        return Ok(node);
                    } else {
                        let node = self.balance_left(node, k)?;
                        return Ok(node);
                    }
                }
            }
        }
        Ok(node)
    }
    fn delete_at(&mut self, mut node: idx::IdxNode, i: usize) -> Result<idx::IdxNode> {
        let record_offset = node.keys[i];
        debug_assert!(
            record_offset != RecordOffset::new(0),
            "key_offset: {} != 0",
            record_offset
        );
        {
            self.clear_key_cache(record_offset);
            self.dat_file.delete_record(record_offset)?;
        }
        let node_offset1 = node.downs[i];
        if node_offset1.is_zero() {
            let _key_offset = node.keys.remove(i);
            let _node_offset = node.downs.remove(i);
            let new_node = self.idx_file.write_node(node)?;
            Ok(new_node)
        } else {
            let node1 = self.idx_file.read_node(node_offset1)?;
            let (record_offset, node1) = self.delete_max(node1)?;
            node.keys[i] = record_offset;
            node.downs[i] = node1.offset;
            let node = self.idx_file.write_node(node)?;
            self.balance_left(node, i)
        }
    }
    fn delete_max(&mut self, mut node: idx::IdxNode) -> Result<(RecordOffset, idx::IdxNode)> {
        let j = node.keys.len();
        let i = j - 1;
        let node_offset1 = node.downs[j];
        if node_offset1.is_zero() {
            node.downs.remove(j);
            let key_offset2 = node.keys.remove(i);
            let new_node = self.idx_file.write_node(node)?;
            Ok((key_offset2, new_node))
        } else {
            let node1 = self.idx_file.read_node(node_offset1)?;
            let (key_offset2, node1) = self.delete_max(node1)?;
            node.downs[j] = node1.offset;
            let node = self.idx_file.write_node(node)?;
            let new_node = self.balance_right(node, j)?;
            Ok((key_offset2, new_node))
        }
    }
    fn balance_left(&mut self, mut node: idx::IdxNode, i: usize) -> Result<idx::IdxNode> {
        let node_offset1 = node.downs[i];
        if node_offset1.is_zero() {
            return Ok(node);
        }
        let mut node1 = self.idx_file.read_node(node_offset1)?;
        if !node1.is_active_on_delete() {
            return Ok(node);
        }
        let j = i + 1;
        let key_offset2 = node.keys[i];
        let node_offset2 = node.downs[j];
        debug_assert!(!node_offset2.is_zero());
        if !node_offset2.is_zero() {
            let mut node2 = self.idx_file.read_node(node_offset2)?;
            if node2.downs.len() == idx::NODE_SLOTS_MAX_HALF as usize {
                // unification
                node1.keys.push(key_offset2);
                //
                node1.keys.extend_from_slice(&node2.keys[0..]);
                node1.downs.extend_from_slice(&node2.downs[0..]);
                self.idx_file.delete_node(node2)?;
                //
                node.keys.remove(i);
                node.downs.remove(j);
                //
                let node1 = self.idx_file.write_node(node1)?;
                node.downs[i] = node1.offset;
            } else {
                let key_offset3 =
                    self.move_a_node_from_right_to_left(key_offset2, &mut node1, &mut node2);
                node.keys[i] = key_offset3;
                let node2 = self.idx_file.write_node(node2)?;
                let node1 = self.idx_file.write_node(node1)?;
                node.downs[j] = node2.offset;
                node.downs[i] = node1.offset;
            }
            let new_node = self.idx_file.write_node(node)?;
            return Ok(new_node);
        }
        Ok(node)
    }
    fn balance_right(&mut self, mut node: idx::IdxNode, j: usize) -> Result<idx::IdxNode> {
        let node_offset1 = node.downs[j];
        if node_offset1.is_zero() {
            return Ok(node);
        }
        let mut node1 = self.idx_file.read_node(node_offset1)?;
        if !node1.is_active_on_delete() {
            return Ok(node);
        }
        let i = j - 1;
        let key_offset2 = node.keys[i];
        let node_offset2 = node.downs[i];
        debug_assert!(!node_offset2.is_zero());
        if !node_offset2.is_zero() {
            let mut node2 = self.idx_file.read_node(node_offset2)?;
            if node2.downs.len() == idx::NODE_SLOTS_MAX_HALF as usize {
                // unification
                node2.keys.push(key_offset2);
                //
                node2.keys.extend_from_slice(&node1.keys[0..]);
                node2.downs.extend_from_slice(&node1.downs[0..]);
                self.idx_file.delete_node(node1)?;
                //
                node.keys.remove(i);
                node.downs.remove(j);
                //
                let node2 = self.idx_file.write_node(node2)?;
                node.downs[i] = node2.offset;
            } else {
                let key_offset3 = self.move_left_right(key_offset2, &mut node2, &mut node1);
                node.keys[i] = key_offset3;
                let node1 = self.idx_file.write_node(node1)?;
                let node2 = self.idx_file.write_node(node2)?;
                node.downs[j] = node1.offset;
                node.downs[i] = node2.offset;
            }
            let new_node = self.idx_file.write_node(node)?;
            return Ok(new_node);
        }
        Ok(node)
    }
    fn move_a_node_from_right_to_left(
        &mut self,
        record_offset: RecordOffset,
        node_l: &mut idx::IdxNode,
        node_r: &mut idx::IdxNode,
    ) -> RecordOffset {
        node_l.keys.push(record_offset);
        node_l.downs.push(node_r.downs.remove(0));
        node_r.keys.remove(0)
    }
    fn move_left_right(
        &mut self,
        record_offset: RecordOffset,
        node_l: &mut idx::IdxNode,
        node_r: &mut idx::IdxNode,
    ) -> RecordOffset {
        let j = node_l.keys.len();
        let i = j - 1;
        node_r.keys.insert(0, record_offset);
        node_r.downs.insert(0, node_l.downs.remove(j));
        node_l.keys.remove(i)
    }
    fn trim(&self, node: idx::IdxNode) -> Result<idx::IdxNode> {
        if node.downs.len() == 1 {
            let node_offset1 = node.downs[0];
            if !node_offset1.is_zero() {
                let node1 = self.idx_file.read_node(node_offset1)?;
                self.idx_file.delete_node(node)?;
                return Ok(node1);
            }
        }
        Ok(node)
    }
}

// find: NEW
impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    fn find_in_node_tree(&mut self, node: &mut idx::IdxNode, key: &KT) -> Result<Option<Vec<u8>>> {
        if node.keys.is_empty() {
            return Ok(None);
        }
        let r = self.keys_binary_search(node, key)?;
        match r {
            Ok(k) => {
                let key_offset = node.keys[k];
                debug_assert!(key_offset != RecordOffset::new(0));
                self.load_value(key_offset)
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                if !node_offset1.is_zero() {
                    let mut node1 = self.idx_file.read_node(node_offset1)?;
                    self.find_in_node_tree(&mut node1, key)
                } else {
                    Ok(None)
                }
            }
        }
    }
    fn has_key_in_node_tree(&mut self, node: &mut idx::IdxNode, key: &KT) -> Result<bool> {
        if node.keys.is_empty() {
            return Ok(false);
        }
        let r = self.keys_binary_search(node, key)?;
        match r {
            Ok(_k) => Ok(true),
            Err(k) => {
                let node_offset1 = node.downs[k];
                if !node_offset1.is_zero() {
                    let mut node1 = self.idx_file.read_node(node_offset1)?;
                    self.has_key_in_node_tree(&mut node1, key)
                } else {
                    Ok(false)
                }
            }
        }
    }
}

impl<KT: FileDbXxxInnerKT> DbXxx<KT> for FileDbXxxInner<KT> {
    fn get(&mut self, key: &KT) -> Result<Option<Vec<u8>>> {
        let mut top_node = self.idx_file.read_top_node()?;
        self.find_in_node_tree(&mut top_node, key)
    }
    fn put(&mut self, key: &KT, value: &[u8]) -> Result<()> {
        let top_node = self.idx_file.read_top_node()?;
        let active_node = self.insert_into_node_tree(top_node, key, value)?;
        let new_top_node = active_node.deactivate();
        self.idx_file.write_top_node(new_top_node)?;
        Ok(())
    }
    fn delete(&mut self, key: &KT) -> Result<()> {
        let top_node = self.idx_file.read_top_node()?;
        let top_node_offset = top_node.offset;
        let top_node = self.delete_from_node_tree(top_node, key)?;
        let new_top_node = self.trim(top_node)?;
        if top_node_offset != new_top_node.offset {
            self.idx_file.write_top_node(new_top_node)?;
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
    fn has_key(&mut self, key: &KT) -> Result<bool> {
        let mut top_node = self.idx_file.read_top_node()?;
        self.has_key_in_node_tree(&mut top_node, key)
    }
}
