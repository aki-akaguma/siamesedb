use super::idx::{NODE_SLOTS_MAX, NODE_SLOTS_MAX_HALF};
use super::semtype::*;
use super::vfile::VarFile;
use std::cell::{Ref, RefCell, RefMut};
use std::convert::TryInto;
use std::io::Result;
use std::rc::Rc;

#[derive(Debug, Default, Clone)]
pub struct IdxNode(Rc<RefCell<TreeNode>>);

impl IdxNode {
    pub fn new(offset: NodeOffset) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::new(offset))))
    }
    pub fn with_node_size(offset: NodeOffset, size: NodeSize) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::with_node_size(
            offset, size,
        ))))
    }
    pub fn new_active(
        record_offset: RecordOffset,
        l_node_offset: NodeOffset,
        r_node_offset: NodeOffset,
    ) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::new_active(
            record_offset,
            l_node_offset,
            r_node_offset,
        ))))
    }
    //
    pub fn get_mut(&mut self) -> RefMut<TreeNode> {
        RefCell::borrow_mut(&self.0)
    }
    pub fn get_ref(&self) -> Ref<TreeNode> {
        RefCell::borrow(&self.0)
    }
    //
    pub fn is_over_len(&self) -> bool {
        let locked = RefCell::borrow(&self.0);
        locked.is_over_len()
    }
    pub fn deactivate(&self) -> Self {
        Self(Rc::new(RefCell::new(RefCell::borrow(&self.0).deactivate())))
    }
    pub fn is_active_on_insert(&self) -> bool {
        let locked = RefCell::borrow(&self.0);
        locked.is_active_on_insert()
    }
    pub fn is_active_on_delete(&self) -> bool {
        let locked = RefCell::borrow(&self.0);
        locked.is_active_on_delete()
    }
    pub(crate) fn idx_write_node_one(&self, file: &mut VarFile) -> Result<()> {
        let locked = RefCell::borrow(&self.0);
        locked.idx_write_node_one(file)
    }
}

#[derive(Debug, Default, Clone)]
pub struct TreeNode {
    /// active node flag is used insert operation. this not store into file.
    is_active: bool,
    /// offset of IdxNode in idx file.
    offset: NodeOffset,
    /// size in bytes of IdxNode in idx file.
    size: NodeSize,
    /// key slot: offset of key-value record in dat file.
    keys: Vec<RecordOffset>,
    /// down slot: offset of next IdxNode in idx file.
    downs: Vec<NodeOffset>,
}

impl TreeNode {
    pub fn new(offset: NodeOffset) -> Self {
        Self::with_node_size(offset, NodeSize::new(0))
    }
    pub fn with_node_size(offset: NodeOffset, size: NodeSize) -> Self {
        Self {
            offset,
            size,
            keys: Vec::with_capacity((NODE_SLOTS_MAX as usize) - 1),
            downs: Vec::with_capacity(NODE_SLOTS_MAX as usize),
            ..Default::default()
        }
    }
    pub fn new_active(
        record_offset: RecordOffset,
        l_node_offset: NodeOffset,
        r_node_offset: NodeOffset,
    ) -> Self {
        let mut r = Self {
            is_active: true,
            ..Default::default()
        };
        r.keys.push(record_offset);
        r.downs.push(l_node_offset);
        r.downs.push(r_node_offset);
        r
    }
}

impl TreeNode {
    pub fn offset(&self) -> NodeOffset {
        self.offset
    }
    pub fn set_offset(&mut self, offset: NodeOffset) {
        self.offset = offset;
    }
    pub fn size(&self) -> NodeSize {
        self.size
    }
    pub fn set_size(&mut self, size: NodeSize) {
        self.size = size;
    }
}

// keys
impl TreeNode {
    pub fn keys_is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn keys_len(&self) -> usize {
        self.keys.len()
    }
    pub fn keys_get(&self, idx: usize) -> RecordOffset {
        self.keys[idx]
    }
    pub unsafe fn keys_get_unchecked(&self, idx: usize) -> RecordOffset {
        *self.keys.get_unchecked(idx)
    }
    pub fn keys_set(&mut self, idx: usize, val: RecordOffset) {
        self.keys[idx] = val;
    }
    pub fn keys_pop(&mut self) -> Option<RecordOffset> {
        self.keys.pop()
    }
    pub fn keys_push(&mut self, val: RecordOffset) {
        self.keys.push(val);
    }
    pub fn keys_insert(&mut self, idx: usize, val: RecordOffset) {
        self.keys.insert(idx, val);
    }
    pub fn keys_extend_from_node(&mut self, other: &TreeNode, st: usize) {
        self.keys.extend_from_slice(&other.keys[st..])
    }
    pub fn keys_remove(&mut self, idx: usize) -> RecordOffset {
        self.keys.remove(idx)
    }
    pub fn keys_resize(&mut self, new_size: usize) {
        self.keys.resize(new_size, RecordOffset::new(0));
    }
}

// downs
impl TreeNode {
    pub fn downs_is_empty(&self) -> bool {
        self.downs.is_empty()
    }
    pub fn downs_len(&self) -> usize {
        self.downs.len()
    }
    pub fn downs_get(&self, idx: usize) -> NodeOffset {
        self.downs[idx]
    }
    pub unsafe fn downs_get_unchecked(&self, idx: usize) -> NodeOffset {
        *self.downs.get_unchecked(idx)
    }
    pub fn downs_set(&mut self, idx: usize, val: NodeOffset) {
        self.downs[idx] = val;
    }
    pub fn _downs_pop(&mut self) -> Option<NodeOffset> {
        self.downs.pop()
    }
    pub fn downs_push(&mut self, val: NodeOffset) {
        self.downs.push(val);
    }
    pub fn downs_insert(&mut self, idx: usize, val: NodeOffset) {
        self.downs.insert(idx, val);
    }
    pub fn downs_extend_from_node(&mut self, other: &TreeNode, st: usize) {
        self.downs.extend_from_slice(&other.downs[st..])
    }
    pub fn downs_remove(&mut self, idx: usize) -> NodeOffset {
        self.downs.remove(idx)
    }
    pub fn downs_resize(&mut self, new_size: usize) {
        self.downs.resize(new_size, NodeOffset::new(0));
    }
}

impl TreeNode {
    pub fn is_over_len(&self) -> bool {
        if self.keys.len() < NODE_SLOTS_MAX as usize && self.downs.len() <= NODE_SLOTS_MAX as usize
        {
            return false;
        }
        true
    }
    /// convert active node to normal node
    pub fn deactivate(&self) -> Self {
        if self.is_active {
            let mut r = Self::new(NodeOffset::new(0));
            r.keys.push(self.keys[0]);
            r.downs.push(self.downs[0]);
            r.downs.push(self.downs[1]);
            r
        } else {
            self.clone()
        }
    }
    pub fn is_active_on_insert(&self) -> bool {
        self.is_active
    }
    pub fn is_active_on_delete(&self) -> bool {
        self.downs.len() < NODE_SLOTS_MAX_HALF as usize
    }
    //
    pub fn encoded_node_size(&self) -> usize {
        let mut sum_size = 0usize;
        //
        let keys_count: u16 = self.keys.len().try_into().unwrap();
        #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
        {
            sum_size += 2;
        }
        #[cfg(feature = "vf_vu64")]
        {
            sum_size += vu64::encoded_len(keys_count as u64) as usize;
        }
        //
        for i in 0..(keys_count as usize) {
            debug_assert!(!self.keys[i].is_zero());
            let _offset = self.keys[i];
            #[cfg(feature = "vf_u32u32")]
            {
                sum_size += 4;
            }
            #[cfg(feature = "vf_u64u64")]
            {
                sum_size += 8;
            }
            #[cfg(feature = "vf_vu64")]
            {
                sum_size += vu64::encoded_len(_offset.as_value()) as usize;
            }
        }
        for i in 0..((keys_count as usize) + 1) {
            debug_assert!(
                keys_count == 0 || i < self.downs.len(),
                "i: {} < self.downs.len(): {}, keys_count: {}",
                i,
                self.downs.len(),
                keys_count
            );
            let _offset = if i < self.downs.len() {
                self.downs[i]
            } else {
                NodeOffset::new(0)
            };
            #[cfg(feature = "vf_u32u32")]
            {
                sum_size += 4;
            }
            #[cfg(feature = "vf_u64u64")]
            {
                sum_size += 8;
            }
            #[cfg(feature = "vf_vu64")]
            {
                sum_size += vu64::encoded_len(_offset.as_value()) as usize;
            }
        }
        //
        sum_size
    }
    //
    pub(crate) fn idx_write_node_one(&self, file: &mut VarFile) -> Result<()> {
        debug_assert!(!self.offset.is_zero());
        //debug_assert!(self.offset.as_value() == IDX_HEADER_SZ || !self.size.is_zero());
        //
        file.seek_from_start(self.offset)?;
        file.write_zero(self.size)?;
        //
        let _start_pos = file.seek_from_start(self.offset)?;
        file.write_node_size(self.size)?;
        let keys_count = self.keys.len();
        //
        file.write_keys_count(KeysCount::new(keys_count.try_into().unwrap()))?;
        debug_assert!(
            keys_count < NODE_SLOTS_MAX as usize,
            "keys_count: {} < NODE_SLOTS_MAX as usize - 1",
            keys_count
        );
        debug_assert!(keys_count == 0 || self.downs.len() == keys_count + 1);
        //
        for i in 0..keys_count {
            let offset = self.keys[i];
            debug_assert!(!offset.is_zero());
            file.write_record_offset(offset)?;
        }
        for i in 0..(keys_count + 1) {
            let offset = if i < self.downs.len() {
                self.downs[i]
            } else {
                NodeOffset::new(0)
            };
            debug_assert!((offset.as_value() & 0x0F) == 0);
            file.write_node_offset(offset)?;
        }
        //
        let _current_pos = file.seek_position()?;
        debug_assert!(
            _start_pos + self.size >= _current_pos,
            "_start_pos: {} + self.size: {} >= _current_pos: {}",
            _start_pos,
            self.size,
            _current_pos,
        );
        //
        Ok(())
    }
}