use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Result;
use std::rc::Rc;

use super::super::super::DbXxx;
use super::super::{FileDbNode, KeyType};
use super::{dat, idx, unu};

#[cfg(feature = "key_cache")]
use super::kc;
#[cfg(feature = "key_cache")]
use super::kc::KeyCacheTrait;

pub trait FileDbXxxInnerKT {
    fn as_bytes(&self) -> Vec<u8>;
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
    fn from(bytes: &[u8]) -> Self;
}

impl FileDbXxxInnerKT for String {
    fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        std::cmp::Ord::cmp(self, other)
    }
    fn from(bytes: &[u8]) -> Self {
        String::from_utf8_lossy(&bytes).to_string()
    }
}

impl FileDbXxxInnerKT for u64 {
    fn as_bytes(&self) -> Vec<u8> {
        //self.to_le_bytes().to_vec()
        super::vu64::encode(*self).as_ref().to_vec()
    }
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        std::cmp::Ord::cmp(self, other)
    }
    fn from(bytes: &[u8]) -> Self {
        //u64::from_le_bytes(bytes.try_into().unwrap())
        super::vu64::decode(bytes).unwrap()
    }
}

#[derive(Debug)]
pub struct FileDbXxxInner<KT: FileDbXxxInnerKT> {
    parent: FileDbNode,
    mem: BTreeMap<String, (u64, Vec<u8>)>,
    dirty: bool,
    //
    dat_file: dat::DatFile,
    idx_file: idx::IdxFile,
    unu_file: unu::UnuFile,
    //
    #[cfg(feature = "key_cache")]
    key_cache: kc::KeyCache<KT>,
    //
    _phantom: std::marker::PhantomData<KT>,
}

impl<KT: FileDbXxxInnerKT> FileDbXxxInner<KT> {
    pub fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbXxxInner<KT>> {
        let path = {
            let rc = parent.0.upgrade().expect("FileDbNode is already disposed");
            let locked = rc.borrow();
            locked.path.clone()
        };
        //
        let dat_file = dat::DatFile::open(&path, ks_name, KeyType::Str)?;
        let idx_file = idx::IdxFile::open(&path, ks_name, KeyType::Str)?;
        let unu_file = unu::UnuFile::open(&path, ks_name, KeyType::Str)?;
        Ok(Self {
            parent,
            dat_file,
            idx_file,
            unu_file,
            mem: BTreeMap::new(),
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
    fn clear_key_cache(&mut self, key_offset: u64) {
        self.key_cache.delete(&key_offset);
    }
    #[cfg(feature = "key_cache")]
    fn _clear_key_cache_all(&mut self) {
        self.key_cache.clear();
    }
    #[cfg(not(feature = "key_cache"))]
    pub fn load_key_string(&mut self, key_offset: u64) -> Result<String> {
        debug_assert!(key_offset != 0);
        let string = self
            .dat_file
            .read_record_key(key_offset)?
            .map(|key| String::from_utf8_lossy(&key).to_string())
            .unwrap();
        Ok(string)
    }
    #[cfg(feature = "key_cache")]
    pub fn load_key_string(&mut self, key_offset: u64) -> Result<Rc<KT>> {
        debug_assert!(key_offset != 0);
        let string = match self.key_cache.get(&key_offset) {
            Some(s) => s,
            None => {
                let vec = self
                    .dat_file
                    .read_record_key(key_offset)?
                    .unwrap();
                self.key_cache.put(&key_offset, KT::from(&vec)).unwrap()
            }
        };
        Ok(string)
    }
    pub fn load_key_string_no_cache(&self, key_offset: u64) -> Result<KT> {
        debug_assert!(key_offset != 0);
        let vec = self
            .dat_file
            .read_record_key(key_offset)?
            .unwrap();
        Ok(KT::from(&vec))
    }
    fn load_value(&self, key_offset: u64) -> Result<Option<Vec<u8>>> {
        debug_assert!(key_offset != 0);
        Ok(self
            .dat_file
            .read_record(key_offset)?
            .map(|(_key, val)| val))
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
            debug_assert!(key_offset != 0);
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
        self.idx_file
            .to_graph_string_with_key_string(self)
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
    pub fn count_of_free_node(&self) -> Result<Vec<(usize, u64)>> {
        self.idx_file.count_of_free_node()
    }
    // count of used node
    pub fn count_of_used_node(&self) -> Result<Vec<(usize, u64)>> {
        self.idx_file.count_of_used_node()
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
            return Ok(idx::IdxNode::new_active(new_key_offset, 0, 0));
        }
        let r = self.keys_binary_search(&mut node, key)?;
        match r {
            Ok(k) => {
                let key_offset = node.keys[k];
                debug_assert!(key_offset != 0);
                let new_key_offset = self.store_value_on_insert(key_offset, value)?;
                if key_offset != new_key_offset {
                    node.keys[k] = new_key_offset;
                    self.dirty = true;
                    return self.idx_file.write_node(node);
                }
                Ok(node)
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                let node2 = if node_offset1 != 0 {
                    let node1 = self.idx_file.read_node(node_offset1)?;
                    self.insert_into_node_tree(node1, key, value)?
                } else {
                    let new_key_offset = self.dat_file.add_record(&key.as_bytes(), value)?;
                    idx::IdxNode::new_active(new_key_offset, 0, 0)
                };
                if node2.is_active_on_insert() {
                    self.balance_on_insert(node, k, &node2)
                } else {
                    debug_assert!(node2.offset != 0);
                    let node2 = self.idx_file.write_node(node2)?;
                    node.downs[k] = node2.offset;
                    self.dirty = true;
                    self.idx_file.write_node(node)
                }
            }
        }
    }
    #[inline]
    fn store_value_on_insert(&mut self, key_offset: u64, value: &[u8]) -> Result<u64> {
        if let Some((r_key, r_val)) = self.dat_file.read_record(key_offset)? {
            if r_val.len() == value.len() {
                if r_val != value {
                    self.dat_file.write_record(key_offset, &r_key, value)?;
                    self.dirty = true;
                }
            } else {
                let _reserve_len = self.dat_file.delete_record(key_offset)?;
                self.unu_file.add_unu(key_offset)?;
                let new_key_offset = self.dat_file.add_record(&r_key, value)?;
                return Ok(new_key_offset);
            }
        } else {
            panic!("dat_file.read_record({})", key_offset);
        }
        Ok(key_offset)
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
        let mut node1 = idx::IdxNode::new(0);
        let slice = &node.keys[idx::NODE_SLOTS_MAX_HALF as usize..node.keys.len()];
        node1.keys.extend_from_slice(slice);
        let slice = &node.downs[idx::NODE_SLOTS_MAX_HALF as usize..node.downs.len()];
        node1.downs.extend_from_slice(slice);
        //
        node.keys.resize(idx::NODE_SLOTS_MAX_HALF as usize, 0);
        node.downs.resize(idx::NODE_SLOTS_MAX_HALF as usize, 0);
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
                if node_offset1 != 0 {
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
        let node_offset1 = node.downs[i];
        if node_offset1 == 0 {
            let _key_offset = node.keys.remove(i);
            let _node_offset = node.downs.remove(i);
            #[cfg(feature = "key_cache")]
            self.clear_key_cache(_key_offset);
            let new_node = self.idx_file.write_node(node)?;
            Ok(new_node)
        } else {
            let node1 = self.idx_file.read_node(node_offset1)?;
            let (key_offset, node1) = self.delete_max(node1)?;
            node.keys[i] = key_offset;
            node.downs[i] = node1.offset;
            let node = self.idx_file.write_node(node)?;
            self.balance_left(node, i)
        }
    }
    fn delete_max(&mut self, mut node: idx::IdxNode) -> Result<(u64, idx::IdxNode)> {
        let j = node.keys.len();
        let i = j - 1;
        let node_offset1 = node.downs[j];
        if node_offset1 == 0 {
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
        if node_offset1 == 0 {
            return Ok(node);
        }
        let mut node1 = self.idx_file.read_node(node_offset1)?;
        if !node1.is_active_on_delete() {
            return Ok(node);
        }
        let j = i + 1;
        let key_offset2 = node.keys[i];
        let node_offset2 = node.downs[j];
        debug_assert!(node_offset2 != 0);
        if node_offset2 != 0 {
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
        if node_offset1 == 0 {
            return Ok(node);
        }
        let mut node1 = self.idx_file.read_node(node_offset1)?;
        if !node1.is_active_on_delete() {
            return Ok(node);
        }
        let i = j - 1;
        let key_offset2 = node.keys[i];
        let node_offset2 = node.downs[i];
        debug_assert!(node_offset2 != 0);
        if node_offset2 != 0 {
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
        key_offset: u64,
        node_l: &mut idx::IdxNode,
        node_r: &mut idx::IdxNode,
    ) -> u64 {
        node_l.keys.push(key_offset);
        node_l.downs.push(node_r.downs.remove(0));
        node_r.keys.remove(0)
    }
    fn move_left_right(
        &mut self,
        key_offset: u64,
        node_l: &mut idx::IdxNode,
        node_r: &mut idx::IdxNode,
    ) -> u64 {
        let j = node_l.keys.len();
        let i = j - 1;
        node_r.keys.insert(0, key_offset);
        node_r.downs.insert(0, node_l.downs.remove(j));
        node_l.keys.remove(i)
    }
    fn trim(&self, node: idx::IdxNode) -> Result<idx::IdxNode> {
        if node.downs.len() == 1 {
            let node_offset1 = node.downs[0];
            if node_offset1 != 0 {
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
                debug_assert!(key_offset != 0);
                self.load_value(key_offset)
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                if node_offset1 != 0 {
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
                if node_offset1 != 0 {
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
            self.unu_file.sync_all()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data
            self.dat_file.sync_data()?;
            self.idx_file.sync_data()?;
            self.unu_file.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn has_key(&mut self, key: &KT) -> Result<bool> {
        let mut top_node = self.idx_file.read_top_node()?;
        self.has_key_in_node_tree(&mut top_node, key)
    }
}
