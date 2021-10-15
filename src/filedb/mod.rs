use super::{DbList, DbMap};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Result;
use std::path::{Path, PathBuf};
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

#[cfg(feature = "key_cache")]
pub mod kc;
#[cfg(feature = "key_cache")]
use kc::KeyCacheTrait;

pub mod buf;
pub mod vfile;

#[cfg(feature = "vf_vint64")]
pub mod vint64;

#[cfg(feature = "vf_leb128")]
pub mod leb128;

#[cfg(feature = "vf_sqlvli")]
pub mod sqlvli;

pub mod dat;
pub mod idx;
pub mod unu;

#[derive(Debug, Clone)]
pub struct FileDb(Rc<RefCell<FileDbInner>>);

#[derive(Debug, Clone)]
pub struct FileDbNode(Weak<RefCell<FileDbInner>>);

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
        if let Some(m) = self.0.borrow().db_maps.get(name) {
            return Ok(m.clone());
        }
        //
        let x = self.to_node();
        x.create_db_map(name)?;
        //
        match self.0.borrow().db_maps.get(name) {
            Some(m) => Ok(m.clone()),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_list(&self, name: &str) -> Result<FileDbList> {
        if let Some(m) = self.0.borrow().db_lists.get(name) {
            return Ok(m.clone());
        }
        //
        let x = self.to_node();
        x.create_db_list(name)?;
        //
        match self.0.borrow().db_lists.get(name) {
            Some(m) => Ok(m.clone()),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn is_dirty(&self) -> bool {
        self.0.borrow().is_dirty()
    }
    pub fn sync_all(&self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

impl FileDbNode {
    pub fn parent(&self) -> Option<Self> {
        let rc = self.0.upgrade().expect("FileDbNode is already dispose");
        let locked = rc.borrow();
        locked.parent.clone()
    }
    fn create_db_map(&self, name: &str) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let child: FileDbMap = FileDbMap::open(self.clone(), name)?;
        let mut locked = rc.borrow_mut();
        let _ = locked.db_maps.insert(name.to_string(), child);
        Ok(())
    }
    fn create_db_list(&self, name: &str) -> Result<()> {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let child: FileDbList = FileDbList::open(self.clone(), name)?;
        let mut locked = rc.borrow_mut();
        let _ = locked.db_lists.insert(name.to_string(), child);
        Ok(())
    }
    pub fn is_dirty(&self) -> bool {
        let rc = self.0.upgrade().expect("FileDbNode is already disposed");
        let r = rc.borrow().is_dirty();
        r
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
    pub fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbMap> {
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
        self.0.borrow().to_graph_string_with_key_string()
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
    pub fn count_of_free_node(&self) -> Result<Vec<(usize, u64)>> {
        self.0.borrow().count_of_free_node()
    }
    /// count of used node
    pub fn count_of_used_node(&self) -> Result<Vec<(usize, u64)>> {
        self.0.borrow().count_of_used_node()
    }
}

impl DbMap for FileDbMap {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        self.0.borrow_mut().get(key)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(key, value)
    }
    fn delete(&mut self, key: &str) -> Result<()> {
        self.0.borrow_mut().delete(key)
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
    fn has_key(&mut self, key: &str) -> Result<bool> {
        self.0.borrow_mut().has_key(key)
    }
}

impl FileDbList {
    pub fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbList> {
        Ok(Self(Rc::new(RefCell::new(FileDbListInner::open(
            parent, ks_name,
        )?))))
    }
    pub fn is_dirty(&self) -> bool {
        self.0.borrow().is_dirty()
    }
}

impl DbList for FileDbList {
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        self.0.borrow_mut().get(key)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(key, value)
    }
    fn delete(&mut self, key: u64) -> Result<()> {
        self.0.borrow_mut().delete(key)
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

//--

#[derive(Debug)]
pub struct FileDbInner {
    parent: Option<FileDbNode>,
    //
    db_maps: BTreeMap<String, FileDbMap>,
    db_lists: BTreeMap<String, FileDbList>,
    //
    path: PathBuf,
    dirty: bool,
}

impl FileDbInner {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<FileDbInner> {
        if !path.as_ref().is_dir() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(FileDbInner {
            parent: None,
            db_maps: BTreeMap::new(),
            db_lists: BTreeMap::new(),
            path: path.as_ref().to_path_buf(),
            dirty: false,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    pub fn sync_all(&self) -> Result<()> {
        if self.is_dirty() {
            // save all data
        }
        Ok(())
    }
    pub fn sync_data(&self) -> Result<()> {
        if self.is_dirty() {
            // save all data
        }
        Ok(())
    }
    /*<CHACHA>
    pub fn record_iter(&mut self) -> Result<RecordIter> {
        RecordIter::new(self.file.clone())
    }
    */
}

#[derive(Debug)]
pub struct FileDbMapInner {
    parent: FileDbNode,
    mem: BTreeMap<String, (u64, Vec<u8>)>,
    dirty: bool,
    //
    dat_file: dat::DatFile,
    idx_file: idx::IdxFile,
    unu_file: unu::UnuFile,
    //
    #[cfg(feature = "key_cache")]
    key_cache: kc::KeyCache,
}

impl FileDbMapInner {
    pub fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbMapInner> {
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
            #[cfg(feature = "key_cache")]
            key_cache: kc::KeyCache::new(),
            dirty: false,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

// for utils
impl FileDbMapInner {
    #[cfg(feature = "key_cache")]
    fn clear_key_cache(&mut self, key_offset: u64) {
        self.key_cache.delete(&key_offset);
    }
    #[cfg(feature = "key_cache")]
    fn _clear_key_cache_all(&mut self) {
        self.key_cache.clear();
    }
    #[cfg(not(feature = "key_cache"))]
    fn load_key_string(&mut self, key_offset: u64) -> Result<String> {
        debug_assert!(key_offset != 0);
        let string = self
            .dat_file
            .read_record_key(key_offset)?
            .map(|key| String::from_utf8_lossy(&key).to_string())
            .unwrap();
        Ok(string)
    }
    #[cfg(feature = "key_cache")]
    fn load_key_string(&mut self, key_offset: u64) -> Result<Rc<String>> {
        debug_assert!(key_offset != 0);
        let string = match self.key_cache.get(&key_offset) {
            Some(s) => s,
            None => {
                let string = self
                    .dat_file
                    .read_record_key(key_offset)?
                    .map(|key| String::from_utf8_lossy(&key).to_string())
                    .unwrap();
                self.key_cache.put(&key_offset, string).unwrap()
            }
        };
        Ok(string)
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
        key: &str,
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
impl FileDbMapInner {
    // convert index to graph string for debug.
    fn to_graph_string(&self) -> Result<String> {
        self.idx_file.to_graph_string()
    }
    fn to_graph_string_with_key_string(&self) -> Result<String> {
        self.idx_file
            .to_graph_string_with_key_string(self.dat_file.clone())
    }
    // check the index tree is balanced
    fn is_balanced(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_balanced(&top_node)
    }
    // check it is multi search tree
    fn is_mst_valid(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_mst_valid(&top_node, self.dat_file.clone())
    }
    // check the node except the root and leaves of the tree has branches of hm or more.
    fn is_dense(&self) -> Result<bool> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.is_dense(&top_node)
    }
    // get depth of node tree
    fn depth_of_node_tree(&self) -> Result<u64> {
        let top_node = self.idx_file.read_top_node()?;
        self.idx_file.depth_of_node_tree(&top_node)
    }
    // count of free node
    fn count_of_free_node(&self) -> Result<Vec<(usize, u64)>> {
        self.idx_file.count_of_free_node()
    }
    // count of used node
    fn count_of_used_node(&self) -> Result<Vec<(usize, u64)>> {
        self.idx_file.count_of_used_node()
    }
}

// insert: NEW
impl FileDbMapInner {
    fn insert_into_node_tree(
        &mut self,
        mut node: idx::IdxNode,
        key: &str,
        value: &[u8],
    ) -> Result<idx::IdxNode> {
        if node.keys.is_empty() {
            let new_key_offset = self.dat_file.add_record(key.as_bytes(), value)?;
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
                    let new_key_offset = self.dat_file.add_record(key.as_bytes(), value)?;
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
impl FileDbMapInner {
    fn delete_from_node_tree(&mut self, mut node: idx::IdxNode, key: &str) -> Result<idx::IdxNode> {
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
impl FileDbMapInner {
    fn find_in_node_tree(&mut self, node: &mut idx::IdxNode, key: &str) -> Result<Option<Vec<u8>>> {
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
    fn has_key_in_node_tree(&mut self, node: &mut idx::IdxNode, key: &str) -> Result<bool> {
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

impl DbMap for FileDbMapInner {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut top_node = self.idx_file.read_top_node()?;
        self.find_in_node_tree(&mut top_node, key)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let top_node = self.idx_file.read_top_node()?;
        let active_node = self.insert_into_node_tree(top_node, key, value)?;
        let new_top_node = active_node.deactivate();
        self.idx_file.write_top_node(new_top_node)?;
        Ok(())
    }
    fn delete(&mut self, key: &str) -> Result<()> {
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
    fn has_key(&mut self, key: &str) -> Result<bool> {
        let mut top_node = self.idx_file.read_top_node()?;
        self.has_key_in_node_tree(&mut top_node, key)
    }
}

#[derive(Debug)]
pub struct FileDbListInner {
    parent: FileDbNode,
    mem: BTreeMap<u64, (u64, Vec<u8>)>,
    dirty: bool,
    //
    dat_file: dat::DatFile,
    idx_file: idx::IdxFile,
    unu_file: unu::UnuFile,
}

impl FileDbListInner {
    pub fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbListInner> {
        let path = {
            let rc = parent.0.upgrade().expect("FileDbNode is already disposed");
            let locked = rc.borrow();
            locked.path.clone()
        };
        //
        let dat_file = dat::DatFile::open(&path, ks_name, KeyType::Int)?;
        let idx_file = idx::IdxFile::open(&path, ks_name, KeyType::Int)?;
        let unu_file = unu::UnuFile::open(&path, ks_name, KeyType::Int)?;
        Ok(Self {
            parent,
            dat_file,
            idx_file,
            unu_file,
            mem: BTreeMap::new(),
            dirty: false,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

impl DbList for FileDbListInner {
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let r = self.mem.get(&key).map(|val| val.1.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key, (0, value.to_vec()));
        Ok(())
    }
    fn delete(&mut self, key: u64) -> Result<()> {
        let _ = self.mem.remove(&key);
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
}

//--
#[cfg(test)]
mod debug {
    #[test]
    fn test_size_of() {
        use super::{FileDb, FileDbList, FileDbMap};
        use super::{FileDbInner, FileDbListInner, FileDbMapInner};
        //
        assert_eq!(std::mem::size_of::<FileDb>(), 8);
        assert_eq!(std::mem::size_of::<FileDbMap>(), 8);
        assert_eq!(std::mem::size_of::<FileDbList>(), 8);
        //
        assert_eq!(std::mem::size_of::<FileDbInner>(), 88);
        #[cfg(not(feature = "key_cache"))]
        assert_eq!(std::mem::size_of::<FileDbMapInner>(), 64);
        #[cfg(feature = "key_cache")]
        assert_eq!(std::mem::size_of::<FileDbMapInner>(), 88);
        assert_eq!(std::mem::size_of::<FileDbListInner>(), 64);
    }
}
