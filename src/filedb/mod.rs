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

pub mod buf;
//pub mod file_buffer;

pub mod dat;
pub mod idx;
pub mod rw;
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
}

impl DbMap for FileDbMap {
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.0.borrow().get(key)
    }
    fn put(&mut self, key: &str, value: &[u8]) {
        self.0.borrow_mut().put(key, value)
    }
    fn delete(&mut self, key: &str) {
        self.0.borrow_mut().delete(key)
    }
    fn sync_all(&mut self) {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) {
        self.0.borrow_mut().sync_data()
    }
    fn has_key(&self, key: &str) -> bool {
        self.0.borrow().has_key(key)
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
    fn get(&self, key: u64) -> Option<Vec<u8>> {
        self.0.borrow().get(key)
    }
    fn put(&mut self, key: u64, value: &[u8]) {
        self.0.borrow_mut().put(key, value)
    }
    fn delete(&mut self, key: u64) {
        self.0.borrow_mut().delete(key)
    }
    fn sync_all(&mut self) {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) {
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
            dirty: false,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

// for utils
impl FileDbMapInner {
    fn load_key_string(&self, key_offset: u64) -> Result<String> {
        assert!(key_offset != 0);
        Ok(self
            .dat_file
            .read_record_key(key_offset)?
            .map(|key| String::from_utf8_lossy(&key).to_string())
            .unwrap())
    }
    fn load_value(&self, key_offset: u64) -> Result<Option<Vec<u8>>> {
        assert!(key_offset != 0);
        Ok(self
            .dat_file
            .read_record(key_offset)?
            .map(|(_key, val)| val))
    }
    fn keys_binary_search(
        &self,
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
            assert!(key_offset != 0);
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
}

// insert: NEW
impl FileDbMapInner {
    fn insert_into_node_tree(
        &mut self,
        node: &mut idx::IdxNode,
        key: &str,
        value: &[u8],
    ) -> Result<idx::IdxNode> {
        if node.keys.is_empty() {
            let new_key_offset = self.dat_file.add_record(key.as_bytes(), value)?;
            return Ok(idx::IdxNode::new_active(new_key_offset, 0, 0));
        }
        let r = self.keys_binary_search(node, key)?;
        match r {
            Ok(k) => {
                let key_offset = node.keys[k];
                assert!(key_offset != 0);
                let new_key_offset = self.store_value_on_insert(key_offset, value)?;
                if key_offset != new_key_offset {
                    node.keys[k] = new_key_offset;
                    self.idx_file.write_node(node.clone())?;
                    self.dirty = true;
                }
                Ok(node.clone())
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                let node2 = if node_offset1 != 0 {
                    let mut node1 = self.idx_file.read_node(node_offset1)?;
                    self.insert_into_node_tree(&mut node1, key, value)?
                } else {
                    let new_key_offset = self.dat_file.add_record(key.as_bytes(), value)?;
                    idx::IdxNode::new_active(new_key_offset, 0, 0)
                };
                if node2.is_active_on_insert() {
                    self.balance_on_insert(node, k, &node2)
                } else {
                    assert!(node2.offset != 0);
                    self.idx_file.write_node(node2.clone())?;
                    node.downs[k] = node2.offset;
                    self.idx_file.write_node(node.clone())?;
                    self.dirty = true;
                    Ok(node.clone())
                }
            }
        }
    }
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
    fn balance_on_insert(
        &mut self,
        node: &mut idx::IdxNode,
        i: usize,
        active_node: &idx::IdxNode,
    ) -> Result<idx::IdxNode> {
        assert!(active_node.is_active_on_insert());
        //
        node.keys.insert(i, active_node.keys[0]);
        node.downs[i] = active_node.downs[1];
        node.downs.insert(i, active_node.downs[0]);
        //
        if node.is_over_len() {
            self.split_on_insert(node)
        } else {
            Ok(node.clone())
        }
    }
    fn split_on_insert(&mut self, node: &mut idx::IdxNode) -> Result<idx::IdxNode> {
        let mut node1 = idx::IdxNode::new(0);
        for i in idx::NODE_SLOTS_MAX_HALF as usize..node.keys.len() {
            node1.keys.push(node.keys[i]);
            node.keys[i] = 0;
        }
        for i in idx::NODE_SLOTS_MAX_HALF as usize..node.downs.len() {
            node1.downs.push(node.downs[i]);
            node.downs[i] = 0;
        }
        //
        let key_offset1 = node.keys.remove(idx::NODE_SLOTS_MAX_HALF as usize - 1);
        let node1 = self.idx_file.write_new_node(node1)?;
        let node = self.idx_file.write_node(node.clone())?;
        Ok(idx::IdxNode::new_active(
            key_offset1,
            node.offset,
            node1.offset,
        ))
    }
}

// delete: NEW
impl FileDbMapInner {
    fn delete_from_node_tree(&mut self, node: &mut idx::IdxNode, key: &str) -> Result<()> {
        if node.keys.is_empty() {
            return Ok(());
        }
        let r = self.keys_binary_search(node, key)?;
        match r {
            Ok(k) => {
                self.delete_at(node, k)?;
            }
            Err(k) => {
                let node_offset1 = node.downs[k];
                if node_offset1 != 0 {
                    let mut node1 = self.idx_file.read_node(node_offset1)?;
                    self.delete_from_node_tree(&mut node1, key)?;
                    if k == node.downs.len() - 1 {
                        self.balance_right(node, k)?;
                    } else {
                        self.balance_left(node, k)?;
                    }
                }
            }
        }
        Ok(())
    }
    fn delete_at(&mut self, node: &mut idx::IdxNode, i: usize) -> Result<()> {
        let node_offset1 = node.downs[i];
        if node_offset1 == 0 {
            node.keys.remove(i);
            node.downs.remove(i);
            self.idx_file.write_node(node.clone())?;
            Ok(())
        } else {
            let mut node1 = self.idx_file.read_node(node_offset1)?;
            let key_offset = self.delete_max(&mut node1)?;
            node.keys[i] = key_offset;
            self.balance_left(node, i)
        }
    }
    fn delete_max(&mut self, node: &mut idx::IdxNode) -> Result<u64> {
        let j = node.keys.len();
        let i = j - 1;
        let node_offset1 = node.downs[j];
        if node_offset1 == 0 {
            node.downs.remove(j);
            let key_offset2 = node.keys.remove(i);
            self.idx_file.write_node(node.clone())?;
            Ok(key_offset2)
        } else {
            let mut node1 = self.idx_file.read_node(node_offset1)?;
            let key_offset2 = self.delete_max(&mut node1)?;
            self.balance_right(node, j)?;
            Ok(key_offset2)
        }
    }
    fn balance_left(&mut self, node: &mut idx::IdxNode, i: usize) -> Result<()> {
        let node_offset1 = node.downs[i];
        if node_offset1 != 0 {
            let mut node1 = self.idx_file.read_node(node_offset1)?;
            if !node1.is_active_on_delete() {
                return Ok(());
            }
            let j = i + 1;
            let key_offset2 = node.keys[i];
            let node_offset2 = node.downs[j];
            let mut node2 = self.idx_file.read_node(node_offset2)?;
            if node2.downs.len() == idx::NODE_SLOTS_MAX_HALF as usize {
                node1.keys.push(key_offset2);
                //
                for k in 0..node2.keys.len() {
                    node1.keys.push(node2.keys[k]);
                }
                for k in 0..node2.downs.len() {
                    node1.downs.push(node2.downs[k]);
                }
                //
                node.keys.remove(i);
                node.downs.remove(j);
                self.idx_file.write_node(node1.clone())?;
                self.idx_file.write_node(node.clone())?;
                return Ok(());
            }
            let key_offset3 = self.move_right_left(key_offset2, &mut node1, &mut node2);
            node.keys[i] = key_offset3;
            self.idx_file.write_node(node2.clone())?;
            self.idx_file.write_node(node1.clone())?;
            self.idx_file.write_node(node.clone())?;
        }
        Ok(())
    }
    fn balance_right(&mut self, node: &mut idx::IdxNode, j: usize) -> Result<()> {
        let node_offset1 = node.downs[j];
        if node_offset1 != 0 {
            let mut node1 = self.idx_file.read_node(node_offset1)?;
            if !node1.is_active_on_delete() {
                return Ok(());
            }
            let i = j - 1;
            let key_offset2 = node.keys[i];
            let node_offset2 = node.downs[i];
            let mut node2 = self.idx_file.read_node(node_offset2)?;
            if node2.downs.len() == idx::NODE_SLOTS_MAX_HALF as usize {
                node2.keys.push(key_offset2);
                //
                for k in 0..node1.keys.len() {
                    node2.keys.push(node1.keys[k]);
                }
                for k in 0..node1.downs.len() {
                    node2.downs.push(node1.downs[k]);
                }
                //
                node.keys.remove(i);
                node.downs.remove(j);
                self.idx_file.write_node(node2.clone())?;
                self.idx_file.write_node(node.clone())?;
                return Ok(());
            }
            let key_offset3 = self.move_left_right(key_offset2, &mut node2, &mut node1);
            node.keys[i] = key_offset3;
            self.idx_file.write_node(node2.clone())?;
            self.idx_file.write_node(node1.clone())?;
            self.idx_file.write_node(node.clone())?;
        }
        Ok(())
    }
    fn move_right_left(
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
    fn trim(&self, node: &mut idx::IdxNode) -> Result<idx::IdxNode> {
        if node.downs.len() == 1 {
            let node_offset1 = node.downs[0];
            if node_offset1 != 0 {
                let node1 = self.idx_file.read_node(node_offset1)?;
                Ok(node1)
            } else {
                Ok(node.clone())
            }
        } else {
            Ok(node.clone())
        }
    }
}

// find: NEW
impl FileDbMapInner {
    fn find_in_node_tree(&self, node: &mut idx::IdxNode, key: &str) -> Result<Option<Vec<u8>>> {
        if node.keys.is_empty() {
            return Ok(None);
        }
        let r = self.keys_binary_search(node, key)?;
        match r {
            Ok(k) => {
                let key_offset = node.keys[k];
                assert!(key_offset != 0);
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
    fn has_key_in_node_tree(&self, node: &mut idx::IdxNode, key: &str) -> Result<bool> {
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
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut top_node = self
            .idx_file
            .read_top_node()
            .expect("can not read top node");
        self.find_in_node_tree(&mut top_node, key).unwrap()
    }
    fn put(&mut self, key: &str, value: &[u8]) {
        let mut top_node = self
            .idx_file
            .read_top_node()
            .expect("can not read top node");
        let active_node = self
            .insert_into_node_tree(&mut top_node, key, value)
            .unwrap();
        let new_top_node = active_node.deactivate();
        self.idx_file.write_top_node(new_top_node).unwrap();
    }
    fn sync_all(&mut self) {
        if self.is_dirty() {
            // save all data and meta
            if let Err(err) = self.dat_file.sync_all() {
                panic!("can not sync dat file: {}", err);
            }
            if let Err(err) = self.idx_file.sync_all() {
                panic!("can not sync idx file: {}", err);
            }
            if let Err(err) = self.unu_file.sync_all() {
                panic!("can not sync unu file: {}", err);
            }
            self.dirty = false;
        }
    }
    fn sync_data(&mut self) {
        if self.is_dirty() {
            // save all data
            if let Err(err) = self.dat_file.sync_data() {
                panic!("can not sync dat file: {}", err);
            }
            if let Err(err) = self.idx_file.sync_data() {
                panic!("can not sync idx file: {}", err);
            }
            if let Err(err) = self.unu_file.sync_data() {
                panic!("can not sync unu file: {}", err);
            }
            self.dirty = false;
        }
    }
    fn delete(&mut self, key: &str) {
        let mut top_node = self
            .idx_file
            .read_top_node()
            .expect("can not read top node");
        self.delete_from_node_tree(&mut top_node, key).unwrap();
        let new_top_node = self.trim(&mut top_node).unwrap();
        if top_node.offset != new_top_node.offset {
            self.idx_file.write_top_node(new_top_node).unwrap();
        }
    }
    fn has_key(&self, key: &str) -> bool {
        let mut top_node = self
            .idx_file
            .read_top_node()
            .expect("can not read top node");
        self.has_key_in_node_tree(&mut top_node, key).unwrap()
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
    fn get(&self, key: u64) -> Option<Vec<u8>> {
        self.mem.get(&key).map(|val| val.1.to_vec())
    }
    fn put(&mut self, key: u64, value: &[u8]) {
        let _ = self.mem.insert(key, (0, value.to_vec()));
    }
    fn delete(&mut self, key: u64) {
        let _ = self.mem.remove(&key);
    }
    fn sync_all(&mut self) {
        if self.is_dirty() {
            // save all data and meta
            if let Err(err) = self.dat_file.sync_all() {
                panic!("can not sync dat file: {}", err);
            }
            if let Err(err) = self.idx_file.sync_all() {
                panic!("can not sync idx file: {}", err);
            }
            if let Err(err) = self.unu_file.sync_all() {
                panic!("can not sync unu file: {}", err);
            }
            self.dirty = false;
        }
    }
    fn sync_data(&mut self) {
        if self.is_dirty() {
            // save all data
            if let Err(err) = self.dat_file.sync_data() {
                panic!("can not sync dat file: {}", err);
            }
            if let Err(err) = self.idx_file.sync_data() {
                panic!("can not sync idx file: {}", err);
            }
            if let Err(err) = self.unu_file.sync_data() {
                panic!("can not sync unu file: {}", err);
            }
            self.dirty = false;
        }
    }
}

//--
/// An iterator over the entries of a ``
pub struct RecordIter {
    file: rw::RawFile,
    pos: u64,
    len: u64,
}

impl RecordIter {
    pub fn new(file: rw::RawFile) -> Result<Self> {
        let len = file.length()?;
        Ok(Self {
            file,
            pos: 512,
            len,
        })
    }
}

impl<'a> Iterator for RecordIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.len {
            return None;
        }
        let r = self.file.seek_to_offset(self.pos);
        if let Err(err) = r {
            return Some(Err(err));
        }
        let rrr = self.file.read_record();
        if rrr.is_err() {
            return Some(rrr);
        }
        let r = self.file.position();
        match r {
            Ok(pos) => self.pos = pos,
            Err(err) => return Some(Err(err)),
        }
        Some(rrr)
    }
}

//--
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
        assert_eq!(std::mem::size_of::<FileDbMapInner>(), 64);
        assert_eq!(std::mem::size_of::<FileDbListInner>(), 64);
    }
}
