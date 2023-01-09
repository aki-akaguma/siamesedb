use super::idx::{NODE_SLOTS_MAX, NODE_SLOTS_MAX_HALF};
use super::semtype::*;
use super::vfile::VarFile;
use rabuf::{SmallRead, SmallWrite};
use std::cell::{Ref, RefCell, RefMut};
use std::io::Result;
use std::rc::Rc;

#[cfg(feature = "siamese_debug")]
use std::convert::TryInto;

#[derive(Debug, Default, Clone)]
pub struct IdxNode(Rc<RefCell<TreeNode>>);

impl IdxNode {
    #[inline]
    pub fn new(offset: NodePieceOffset) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::new(offset))))
    }
    #[inline]
    pub fn new_empty() -> Self {
        Self(Rc::new(RefCell::new(TreeNode::new_empty())))
    }
    #[inline]
    pub fn _with_node_size(offset: NodePieceOffset, size: NodePieceSize) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::with_node_size(
            offset, size,
        ))))
    }
    #[inline]
    pub fn with_node_size_vec_1(
        offset: NodePieceOffset,
        size: NodePieceSize,
        keys: Vec<KeyPieceOffset>,
        downs: Vec<NodePieceOffset>,
    ) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::with_node_size_vec_1(
            offset, size, keys, downs,
        ))))
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn with_node_size_vec_2(
        offset: NodePieceOffset,
        size: NodePieceSize,
        keys: Vec<KeyPieceOffset>,
        downs: Vec<NodePieceOffset>,
        short_keys: Vec<Option<Vec<u8>>>,
    ) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::with_node_size_vec_2(
            offset, size, keys, downs, short_keys,
        ))))
    }
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn new_active(
        piece_offset: KeyPieceOffset,
        l_node_offset: NodePieceOffset,
        r_node_offset: NodePieceOffset,
    ) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::new_active(
            piece_offset,
            l_node_offset,
            r_node_offset,
        ))))
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn new_active(
        piece_offset: KeyPieceOffset,
        l_node_offset: NodePieceOffset,
        r_node_offset: NodePieceOffset,
        short_key: Option<Vec<u8>>,
    ) -> Self {
        Self(Rc::new(RefCell::new(TreeNode::new_active(
            piece_offset,
            l_node_offset,
            r_node_offset,
            short_key,
        ))))
    }
    //
    #[inline]
    pub fn get_mut(&mut self) -> RefMut<TreeNode> {
        RefCell::borrow_mut(&self.0)
    }
    #[inline]
    pub fn get_ref(&self) -> Ref<TreeNode> {
        RefCell::borrow(&self.0)
    }
    //
    #[inline]
    pub fn is_over_len(&self) -> bool {
        let locked = RefCell::borrow(&self.0);
        locked.is_over_len()
    }
    #[inline]
    pub fn deactivate(&self) -> Self {
        Self(Rc::new(RefCell::new(RefCell::borrow(&self.0).deactivate())))
    }
    #[inline]
    pub fn is_active_on_insert(&self) -> bool {
        let locked = RefCell::borrow(&self.0);
        locked.is_active_on_insert()
    }
    #[inline]
    pub fn is_active_on_delete(&self) -> bool {
        let locked = RefCell::borrow(&self.0);
        locked.is_active_on_delete()
    }
    #[inline]
    pub(crate) fn idx_write_node_one(&self, file: &mut VarFile) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.idx_write_node_one(file)
    }
}

#[derive(Debug, Default, Clone)]
pub struct TreeNode {
    /// active node flag is used insert operation.
    /// active nodes have not yet been saved to idx-file.
    is_active: bool,
    /// dirty flag, if this is true, tr must be saved.
    is_dirty: bool,
    /// offset of IdxNode in idx-file.
    offset: NodePieceOffset,
    /// size in bytes of IdxNode in idx-file.
    size: NodePieceSize,
    /// key slot: offset of key-value piece in idx-file.
    keys: Vec<KeyPieceOffset>,
    /// down slot: offset of next IdxNode in idx-file.
    downs: Vec<NodePieceOffset>,
    ///
    #[cfg(feature = "tr_has_short_key")]
    short_keys: Vec<Option<Vec<u8>>>,
}

impl TreeNode {
    #[inline]
    pub fn create_empty_keys_vec() -> Vec<KeyPieceOffset> {
        Vec::with_capacity((NODE_SLOTS_MAX as usize) - 1)
    }
    #[inline]
    pub fn create_empty_downs_vec() -> Vec<NodePieceOffset> {
        Vec::with_capacity(NODE_SLOTS_MAX as usize)
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn create_empty_short_keys_vec() -> Vec<Option<Vec<u8>>> {
        Vec::with_capacity((NODE_SLOTS_MAX as usize) - 1)
    }
    #[inline]
    pub fn new_empty() -> Self {
        let offset = NodePieceOffset::new(0);
        let size = NodePieceSize::new(0);
        let keys = Self::create_empty_keys_vec();
        let downs = Self::create_empty_downs_vec();
        #[cfg(feature = "tr_has_short_key")]
        let short_keys = Self::create_empty_short_keys_vec();
        //
        Self {
            is_dirty: false,
            offset,
            size,
            keys,
            downs,
            #[cfg(feature = "tr_has_short_key")]
            short_keys,
            ..Default::default()
        }
    }
    #[inline]
    pub fn new(offset: NodePieceOffset) -> Self {
        Self::with_node_size(offset, NodePieceSize::new(0))
    }
    #[inline]
    pub fn with_node_size(offset: NodePieceOffset, size: NodePieceSize) -> Self {
        let keys = Self::create_empty_keys_vec();
        let mut downs = Self::create_empty_downs_vec();
        downs.push(NodePieceOffset::new(0));
        #[cfg(feature = "tr_has_short_key")]
        let short_keys = Self::create_empty_short_keys_vec();
        //
        Self {
            is_dirty: true,
            offset,
            size,
            keys,
            downs,
            #[cfg(feature = "tr_has_short_key")]
            short_keys,
            ..Default::default()
        }
    }
    #[inline]
    pub fn with_node_size_vec_1(
        offset: NodePieceOffset,
        size: NodePieceSize,
        keys: Vec<KeyPieceOffset>,
        downs: Vec<NodePieceOffset>,
    ) -> Self {
        #[cfg(feature = "tr_has_short_key")]
        let short_keys = Self::create_empty_short_keys_vec();
        Self {
            is_dirty: false,
            offset,
            size,
            keys,
            downs,
            #[cfg(feature = "tr_has_short_key")]
            short_keys,
            ..Default::default()
        }
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn with_node_size_vec_2(
        offset: NodePieceOffset,
        size: NodePieceSize,
        keys: Vec<KeyPieceOffset>,
        downs: Vec<NodePieceOffset>,
        short_keys: Vec<Option<Vec<u8>>>,
    ) -> Self {
        Self {
            is_dirty: false,
            offset,
            size,
            keys,
            downs,
            short_keys,
            ..Default::default()
        }
    }
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn new_active(
        piece_offset: KeyPieceOffset,
        l_node_offset: NodePieceOffset,
        r_node_offset: NodePieceOffset,
    ) -> Self {
        let mut r = Self {
            is_active: true,
            is_dirty: true,
            keys: Vec::with_capacity(1),
            downs: Vec::with_capacity(2),
            ..Default::default()
        };
        r.keys.push(piece_offset);
        r.downs.push(l_node_offset);
        r.downs.push(r_node_offset);
        r
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn new_active(
        piece_offset: KeyPieceOffset,
        l_node_offset: NodePieceOffset,
        r_node_offset: NodePieceOffset,
        short_key: Option<Vec<u8>>,
    ) -> Self {
        let mut r = Self {
            is_active: true,
            is_dirty: true,
            keys: Vec::with_capacity(1),
            downs: Vec::with_capacity(2),
            short_keys: Vec::with_capacity(1),
            ..Default::default()
        };
        r.keys.push(piece_offset);
        r.downs.push(l_node_offset);
        r.downs.push(r_node_offset);
        r.short_keys.push(short_key);
        r
    }
    /// convert active node to normal node
    pub fn deactivate(&self) -> Self {
        if self.is_active {
            let mut keys = Self::create_empty_keys_vec();
            let mut downs = Self::create_empty_downs_vec();
            #[cfg(feature = "tr_has_short_key")]
            let mut short_keys = Self::create_empty_short_keys_vec();
            //
            #[cfg(feature = "siamese_debug")]
            #[cfg(not(feature = "tr_has_short_key"))]
            let (key_0, down_0, down_1) = { (self.keys[0], self.downs[0], self.downs[1]) };
            #[cfg(feature = "siamese_debug")]
            #[cfg(feature = "tr_has_short_key")]
            let (key_0, down_0, down_1, short_key_0) = {
                (
                    self.keys[0],
                    self.downs[0],
                    self.downs[1],
                    self.short_keys[0].clone(),
                )
            };
            #[cfg(not(feature = "siamese_debug"))]
            #[cfg(not(feature = "tr_has_short_key"))]
            let (key_0, down_0, down_1) = unsafe {
                let keys_ptr = self.keys.as_ptr();
                let downs_ptr = self.downs.as_ptr();
                (*keys_ptr, *downs_ptr, *(downs_ptr.add(1)))
            };
            #[cfg(not(feature = "siamese_debug"))]
            #[cfg(feature = "tr_has_short_key")]
            let (key_0, down_0, down_1, short_key_0) = unsafe {
                let keys_ptr = self.keys.as_ptr();
                let downs_ptr = self.downs.as_ptr();
                let short_keys_ptr = self.short_keys.as_ptr();
                (
                    *keys_ptr,
                    *downs_ptr,
                    *(downs_ptr.add(1)),
                    (*short_keys_ptr).clone(),
                )
            };
            //
            keys.push(key_0);
            downs.push(down_0);
            downs.push(down_1);
            #[cfg(feature = "tr_has_short_key")]
            short_keys.push(short_key_0);
            //
            Self {
                is_dirty: true,
                offset: NodePieceOffset::new(0),
                size: NodePieceSize::new(0),
                keys,
                downs,
                #[cfg(feature = "tr_has_short_key")]
                short_keys,
                ..Default::default()
            }
        } else {
            self.clone()
        }
    }
}

impl TreeNode {
    #[inline]
    pub fn offset(&self) -> NodePieceOffset {
        self.offset
    }
    #[inline]
    pub fn set_offset(&mut self, offset: NodePieceOffset) {
        self.offset = offset;
    }
    #[inline]
    pub fn size(&self) -> NodePieceSize {
        self.size
    }
    #[inline]
    pub fn set_size(&mut self, size: NodePieceSize) {
        self.size = size;
    }
}

// keys
impl TreeNode {
    #[inline]
    pub fn keys(&self) -> &[KeyPieceOffset] {
        &self.keys
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn short_keys(&self) -> &[Option<Vec<u8>>] {
        &self.short_keys
    }
    //
    #[inline]
    pub fn keys_is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    #[inline]
    pub fn keys_len(&self) -> usize {
        self.keys.len()
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn keys_get(&self, idx: usize) -> KeyPieceOffset {
        self.keys[idx]
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn keys_get(&self, idx: usize) -> (KeyPieceOffset, Option<Vec<u8>>) {
        (self.keys[idx], self.short_keys[idx].clone())
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[cfg(not(feature = "siamese_debug"))]
    #[inline]
    pub unsafe fn keys_get_unchecked(&self, idx: usize) -> KeyPieceOffset {
        *self.keys.as_ptr().add(idx)
    }
    #[cfg(feature = "tr_has_short_key")]
    #[cfg(not(feature = "siamese_debug"))]
    #[inline]
    pub unsafe fn keys_get_unchecked(&self, idx: usize) -> (KeyPieceOffset, Option<Vec<u8>>) {
        (
            *self.keys.as_ptr().add(idx),
            (*self.short_keys.as_ptr().add(idx)).clone(),
        )
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn keys_set(&mut self, idx: usize, val: KeyPieceOffset) {
        #[cfg(feature = "siamese_debug")]
        {
            if self.keys[idx] != val {
                self.keys[idx] = val;
                self.is_dirty = true;
            }
        }
        #[cfg(not(feature = "siamese_debug"))]
        unsafe {
            let keys_ptr = self.keys.as_mut_ptr().add(idx);
            if *keys_ptr != val {
                *keys_ptr = val;
                self.is_dirty = true;
            }
        }
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn keys_set(&mut self, idx: usize, val: KeyPieceOffset, short_key: Option<Vec<u8>>) {
        #[cfg(feature = "siamese_debug")]
        {
            if self.keys[idx] != val {
                self.keys[idx] = val;
                self.is_dirty = true;
            }
            if self.short_keys[idx] != short_key {
                self.short_keys[idx] = short_key;
                self.is_dirty = true;
            }
        }
        #[cfg(not(feature = "siamese_debug"))]
        unsafe {
            let keys_ptr = self.keys.as_mut_ptr().add(idx);
            if *keys_ptr != val {
                *keys_ptr = val;
                self.is_dirty = true;
            }
            let short_keys_ptr = self.short_keys.as_mut_ptr().add(idx);
            if *short_keys_ptr != short_key {
                *short_keys_ptr = short_key;
                self.is_dirty = true;
            }
        }
    }
    //
    #[inline]
    pub fn _keys_pop(&mut self) -> Option<KeyPieceOffset> {
        self.is_dirty = true;
        self.keys.pop()
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn keys_push(&mut self, val: KeyPieceOffset) {
        self.is_dirty = true;
        self.keys.push(val);
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn keys_push(&mut self, val: KeyPieceOffset, short_key: Option<Vec<u8>>) {
        self.is_dirty = true;
        self.keys.push(val);
        self.short_keys.push(short_key);
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn keys_insert(&mut self, idx: usize, val: KeyPieceOffset) {
        self.is_dirty = true;
        self.keys.insert(idx, val);
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn keys_insert(&mut self, idx: usize, val: KeyPieceOffset, short_key: Option<Vec<u8>>) {
        self.is_dirty = true;
        self.keys.insert(idx, val);
        self.short_keys.insert(idx, short_key);
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn keys_remove(&mut self, idx: usize) -> KeyPieceOffset {
        self.is_dirty = true;
        self.keys.remove(idx)
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn keys_remove(&mut self, idx: usize) -> (KeyPieceOffset, Option<Vec<u8>>) {
        self.is_dirty = true;
        let key_offset = self.keys.remove(idx);
        let short_key = self.short_keys.remove(idx);
        (key_offset, short_key)
    }
}

// downs
impl TreeNode {
    #[inline]
    pub fn downs_is_empty(&self) -> bool {
        self.downs.is_empty()
    }
    #[inline]
    pub fn downs_len(&self) -> usize {
        self.downs.len()
    }
    #[inline]
    pub fn downs_get(&self, idx: usize) -> NodePieceOffset {
        self.downs[idx]
    }
    #[cfg(not(feature = "siamese_debug"))]
    #[inline]
    pub unsafe fn downs_get_unchecked(&self, idx: usize) -> NodePieceOffset {
        *self.downs.as_ptr().add(idx)
        //*self.downs.get_unchecked(idx)
    }
    #[inline]
    pub fn downs_set(&mut self, idx: usize, val: NodePieceOffset) {
        #[cfg(feature = "siamese_debug")]
        {
            if self.downs[idx] != val {
                self.downs[idx] = val;
                self.is_dirty = true;
            }
        }
        #[cfg(not(feature = "siamese_debug"))]
        unsafe {
            let downs_ptr = self.downs.as_mut_ptr().add(idx);
            if *downs_ptr != val {
                *downs_ptr = val;
                self.is_dirty = true;
            }
        }
    }
    #[inline]
    pub fn _downs_pop(&mut self) -> Option<NodePieceOffset> {
        self.is_dirty = true;
        self.downs.pop()
    }
    #[inline]
    pub fn downs_push(&mut self, val: NodePieceOffset) {
        self.is_dirty = true;
        self.downs.push(val);
    }
    #[inline]
    pub fn downs_insert(&mut self, idx: usize, val: NodePieceOffset) {
        self.is_dirty = true;
        self.downs.insert(idx, val);
    }
    #[inline]
    pub fn downs_remove(&mut self, idx: usize) -> NodePieceOffset {
        self.is_dirty = true;
        self.downs.remove(idx)
    }
}

// keys & downs
impl TreeNode {
    #[inline]
    pub fn keys_downs_extend_from_node(&mut self, other: &TreeNode, st: usize) {
        debug_assert!(self.keys.len() == self.downs.len());
        self.is_dirty = true;
        #[cfg(not(feature = "tr_has_short_key"))]
        #[cfg(feature = "siamese_debug")]
        let (okeys_slice, odowns_slice) = { (&other.keys[st..], &other.downs[st..]) };
        #[cfg(not(feature = "tr_has_short_key"))]
        #[cfg(not(feature = "siamese_debug"))]
        let (okeys_slice, odowns_slice) = unsafe {
            (
                std::slice::from_raw_parts(other.keys.as_ptr().add(st), other.keys.len() - st),
                std::slice::from_raw_parts(other.downs.as_ptr().add(st), other.downs.len() - st),
            )
        };
        #[cfg(feature = "tr_has_short_key")]
        #[cfg(feature = "siamese_debug")]
        let (okeys_slice, odowns_slice, oshort_keys_slice) = {
            (
                &other.keys[st..],
                &other.downs[st..],
                &other.short_keys[st..],
            )
        };
        #[cfg(feature = "tr_has_short_key")]
        #[cfg(not(feature = "siamese_debug"))]
        let (okeys_slice, odowns_slice, oshort_keys_slice) = unsafe {
            (
                std::slice::from_raw_parts(other.keys.as_ptr().add(st), other.keys.len() - st),
                std::slice::from_raw_parts(other.downs.as_ptr().add(st), other.downs.len() - st),
                std::slice::from_raw_parts(
                    other.short_keys.as_ptr().add(st),
                    other.short_keys.len() - st,
                ),
            )
        };
        //
        self.keys.extend_from_slice(okeys_slice);
        self.downs.extend_from_slice(odowns_slice);
        #[cfg(feature = "tr_has_short_key")]
        self.short_keys.extend_from_slice(oshort_keys_slice);
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    pub fn keys_downs_resize(&mut self, new_downs_size: usize) -> KeyPieceOffset {
        debug_assert!(self.keys.len() + 1 == self.downs.len());
        #[cfg(feature = "siamese_debug")]
        let key_offset = self.keys[new_downs_size - 1];
        #[cfg(not(feature = "siamese_debug"))]
        let key_offset = unsafe { *(self.keys.as_ptr().add(new_downs_size - 1)) };
        //
        if self.keys.len() != new_downs_size - 1 {
            self.keys.resize(new_downs_size - 1, KeyPieceOffset::new(0));
            self.is_dirty = true;
        }
        if self.downs.len() != new_downs_size {
            self.downs.resize(new_downs_size, NodePieceOffset::new(0));
            self.is_dirty = true;
        }
        key_offset
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    pub fn keys_downs_resize(
        &mut self,
        new_downs_size: usize,
    ) -> (KeyPieceOffset, Option<Vec<u8>>) {
        debug_assert!(self.keys.len() + 1 == self.downs.len());
        #[cfg(feature = "siamese_debug")]
        let key_offset = self.keys[new_downs_size - 1];
        #[cfg(not(feature = "siamese_debug"))]
        let key_offset = unsafe { *(self.keys.as_ptr().add(new_downs_size - 1)) };
        //
        #[cfg(feature = "siamese_debug")]
        let short_key = self.short_keys[new_downs_size - 1].clone();
        #[cfg(not(feature = "siamese_debug"))]
        let short_key = unsafe { &*(self.short_keys.as_ptr().add(new_downs_size - 1)) }.clone();
        //
        if self.keys.len() != new_downs_size - 1 {
            self.keys.resize(new_downs_size - 1, KeyPieceOffset::new(0));
            self.is_dirty = true;
        }
        if self.downs.len() != new_downs_size {
            self.downs.resize(new_downs_size, NodePieceOffset::new(0));
            self.is_dirty = true;
        }
        if self.short_keys.len() != new_downs_size - 1 {
            self.short_keys.resize(new_downs_size - 1, None);
            self.is_dirty = true;
        }
        (key_offset, short_key)
    }
}

impl TreeNode {
    #[inline]
    pub fn is_over_len(&self) -> bool {
        if self.keys.len() < NODE_SLOTS_MAX as usize && self.downs.len() <= NODE_SLOTS_MAX as usize
        {
            return false;
        }
        true
    }
    #[inline]
    pub fn is_active_on_insert(&self) -> bool {
        self.is_active
    }
    #[inline]
    pub fn is_active_on_delete(&self) -> bool {
        self.downs.len() < NODE_SLOTS_MAX_HALF as usize
    }
    #[inline]
    pub fn is_leaf(&self) -> bool {
        #[cfg(feature = "siamese_debug")]
        for x in self.downs.iter() {
            if !x.is_zero() {
                return false;
            }
        }
        #[cfg(not(feature = "siamese_debug"))]
        unsafe {
            let mut len = self.downs.len();
            let mut ptr = self.downs.as_ptr();
            while len > 0 {
                if !(*ptr).is_zero() {
                    return false;
                }
                ptr = ptr.add(1);
                len -= 1;
            }
        }
        true
    }
    //
    #[inline(never)]
    pub fn encoded_node_size(&self) -> usize {
        let mut sum_size = 0usize;
        // node or leaf + padding
        sum_size += 2;
        //
        #[cfg(feature = "siamese_debug")]
        let keys_count: u16 = self.keys.len().try_into().unwrap();
        #[cfg(not(feature = "siamese_debug"))]
        let keys_count: u16 = self.keys.len() as u16;
        //
        #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
        {
            sum_size += 2;
        }
        #[cfg(not(any(feature = "vf_u32u32", feature = "vf_u64u64")))]
        #[cfg(feature = "vf_vu64")]
        {
            sum_size += vu64::encoded_len(keys_count as u64) as usize;
        }
        //
        #[cfg(any(feature = "vf_u32u32", feature = "vf_node_u32"))]
        {
            sum_size += 4 * keys_count as usize;
        }
        #[cfg(any(feature = "vf_u64u64", feature = "vf_node_u64"))]
        {
            sum_size += 8 * keys_count as usize;
        }
        #[cfg(not(any(
            feature = "vf_u32u32",
            feature = "vf_u64u64",
            feature = "vf_node_u32",
            feature = "vf_node_u64"
        )))]
        #[cfg(feature = "vf_vu64")]
        for i in 0..(keys_count as usize) {
            #[cfg(feature = "siamese_debug")]
            let _offset = self.keys[i];
            #[cfg(not(feature = "siamese_debug"))]
            let _offset = unsafe { *self.keys.as_ptr().add(i) };
            //let _offset = unsafe { *self.keys.get_unchecked(i) };
            debug_assert!(!_offset.is_zero());
            //
            sum_size += vu64::encoded_len(_offset.as_value() / 8) as usize;
        }
        let is_leaf = self.is_leaf();
        if !is_leaf {
            #[cfg(any(feature = "vf_u32u32", feature = "vf_node_u32"))]
            {
                sum_size += 4 * (keys_count as usize + 1);
            }
            #[cfg(any(feature = "vf_u64u64", feature = "vf_node_u64"))]
            {
                sum_size += 8 * (keys_count as usize + 1);
            }
            #[cfg(not(any(
                feature = "vf_u32u32",
                feature = "vf_u64u64",
                feature = "vf_node_u32",
                feature = "vf_node_u64"
            )))]
            #[cfg(feature = "vf_vu64")]
            for i in 0..((keys_count as usize) + 1) {
                debug_assert!(
                    keys_count == 0 || i < self.downs.len(),
                    "i: {} < self.downs.len(): {}, keys_count: {}",
                    i,
                    self.downs.len(),
                    keys_count
                );
                let _offset = if i < (keys_count as usize) + 1 {
                    #[cfg(feature = "siamese_debug")]
                    let _offset = self.downs[i];
                    #[cfg(not(feature = "siamese_debug"))]
                    let _offset = unsafe { *self.downs.as_ptr().add(i) };
                    //let _offset = unsafe { *self.downs.get_unchecked(i) };
                    _offset
                } else {
                    NodePieceOffset::new(0)
                };
                sum_size += vu64::encoded_len(_offset.as_value() / 8) as usize;
            }
        }
        //
        #[cfg(feature = "tr_has_short_key")]
        for i in 0..(keys_count as usize) {
            #[cfg(feature = "siamese_debug")]
            let short_key = &self.short_keys[i];
            #[cfg(not(feature = "siamese_debug"))]
            let short_key = unsafe { &*self.short_keys.as_ptr().add(i) };
            //
            sum_size += 1;
            if let Some(vec) = short_key {
                let short_key_size = vec.len();
                sum_size += short_key_size;
            }
        }
        //
        sum_size
    }
    //
    pub(crate) fn idx_write_node_one(&mut self, file: &mut VarFile) -> Result<()> {
        if !self.is_dirty {
            let _phantom = std::marker::PhantomData::<i32>;
            return Ok(());
        }
        debug_assert!(!self.offset.is_zero());
        //debug_assert!(self.offset.as_value() == IDX_HEADER_SZ || !self.size.is_zero());
        //
        let _start_pos = file.seek_from_start(self.offset)?;
        file.write_node_size(self.size)?;
        //
        let is_leaf = u8::from(self.is_leaf());
        file.write_u16_le(is_leaf as u16)?;
        //
        let keys_count = self.keys.len();
        file.write_keys_count(KeysCount::new(keys_count as u16))?;
        debug_assert!(
            keys_count < NODE_SLOTS_MAX as usize,
            "keys_count: {} < NODE_SLOTS_MAX as usize - 1",
            keys_count
        );
        //debug_assert!(keys_count == 0 || self.downs.len() == keys_count + 1);
        debug_assert!(
            self.downs.len() == keys_count + 1,
            "self.downs.len():{} == (keys_count + 1):{}",
            self.downs.len(),
            keys_count + 1
        );
        //
        #[cfg(not(all(feature = "vf_node_u64", feature = "idx_straight")))]
        {
            for i in 0..keys_count {
                #[cfg(feature = "siamese_debug")]
                let offset = self.keys[i];
                #[cfg(not(feature = "siamese_debug"))]
                let offset = unsafe { *self.keys.as_ptr().add(i) };
                //let offset = unsafe { *self.keys.get_unchecked(i) };
                debug_assert!(!offset.is_zero());
                //
                #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                file.write_piece_offset(offset)?;
                #[cfg(feature = "vf_node_u32")]
                file.write_piece_offset_u32(offset)?;
                #[cfg(feature = "vf_node_u64")]
                file.write_piece_offset_u64(offset)?;
            }
            //
            if is_leaf == 0u8 {
                for i in 0..(keys_count + 1) {
                    let offset = if i < self.downs.len() {
                        #[cfg(feature = "siamese_debug")]
                        let offset = self.downs[i];
                        #[cfg(not(feature = "siamese_debug"))]
                        let offset = unsafe { *self.downs.as_ptr().add(i) };
                        //let offset = unsafe { *self.downs.get_unchecked(i) };
                        offset
                    } else {
                        NodePieceOffset::new(0)
                    };
                    debug_assert!((offset.as_value() & 0x0F) == 0);
                    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                    file.write_node_offset(offset)?;
                    #[cfg(feature = "vf_node_u32")]
                    file.write_node_offset_u32(offset)?;
                    #[cfg(feature = "vf_node_u64")]
                    file.write_node_offset_u64(offset)?;
                }
            }
        }
        #[cfg(all(feature = "vf_node_u64", feature = "idx_straight"))]
        {
            if keys_count == 0 {
                if is_leaf == 0u8 {
                    debug_assert!(
                        self.downs.len() == 1,
                        "self.downs.len():{}",
                        self.downs.len()
                    );
                    file.write_piece_offset_u64_slice(&self.downs)?;
                }
            } else if is_leaf == 0u8 {
                debug_assert!(
                    self.keys.len() + 1 == self.downs.len(),
                    "self.keys.len():{} + 1 == self.downs.len():{}",
                    self.keys.len(),
                    self.downs.len()
                );
                file.write_piece_offset_and_node_offset_u64_slice(&self.keys, &self.downs)?;
            } else {
                file.write_piece_offset_u64_slice(&self.keys)?;
            }
        }
        //
        #[cfg(feature = "tr_has_short_key")]
        {
            for i in 0..keys_count {
                #[cfg(feature = "siamese_debug")]
                let short_key = &self.short_keys[i];
                #[cfg(not(feature = "siamese_debug"))]
                let short_key = unsafe { &*self.short_keys.as_ptr().add(i) };
                //
                if let Some(vec) = short_key {
                    let short_key_size = vec.len();
                    debug_assert!(short_key_size < 0x80);
                    file.write_u8(short_key_size as u8 | 0x80)?;
                    file.write_all_small(&vec)?;
                } else {
                    file.write_u8(0x00)?;
                }
            }
        }
        //
        let _current_pos = file.seek_position()?;
        debug_assert!(
            _start_pos + self.size >= _current_pos,
            "_start_pos: {} + self.size: {} >= _current_pos: {}, have keys: {}",
            _start_pos,
            self.size,
            _current_pos,
            keys_count,
        );
        //
        file.write_zero_to_offset(self.offset + self.size)?;
        //
        self.is_dirty = false;
        //
        #[cfg(debug_assertions)]
        {
            let _current_pos = file.seek_position()?;
            debug_assert!(
                _start_pos + self.size == _current_pos,
                "_start_pos: {} + self.size: {} == _current_pos: {}",
                _start_pos,
                self.size,
                _current_pos,
            );
        }
        //
        Ok(())
    }
}

impl IdxNode {
    //
    pub(crate) fn idx_read_node_one(
        file: &mut VarFile,
        offset: NodePieceOffset,
    ) -> Result<(IdxNode, NodePieceSize)> {
        debug_assert!(!offset.is_zero());
        debug_assert!((offset.as_value() & 0x0F) == 0);
        //
        let _start_pos = file.seek_from_start(offset)?;
        let node_size = file.read_node_size()?;
        debug_assert!(
            !node_size.is_zero(),
            "!node_size.is_zero(), offset: {}",
            offset
        );
        let is_leaf = file.read_u16_le()?;
        let keys_count = file.read_keys_count()?;
        debug_assert!(
            keys_count.as_value() < NODE_SLOTS_MAX,
            "keys_count: {} < NODE_SLOTS_MAX",
            keys_count
        );
        let keys_count: usize = keys_count.into();
        //
        let mut keys = TreeNode::create_empty_keys_vec();
        let mut downs = TreeNode::create_empty_downs_vec();
        #[cfg(feature = "tr_has_short_key")]
        let mut short_keys = TreeNode::create_empty_short_keys_vec();
        //
        #[cfg(not(feature = "idx_straight"))]
        {
            keys.resize(keys_count, KeyPieceOffset::new(0));
            for _i in 0..keys_count {
                #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                let piece_offset = file
                    .read_piece_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                #[cfg(feature = "vf_node_u32")]
                let piece_offset = file
                    .read_piece_offset_u32()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                #[cfg(feature = "vf_node_u64")]
                let piece_offset = file
                    .read_piece_offset_u64()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                debug_assert!(!piece_offset.is_zero());
                #[cfg(feature = "siamese_debug")]
                {
                    keys[_i] = piece_offset;
                }
                #[cfg(not(feature = "siamese_debug"))]
                {
                    unsafe { *(keys.as_mut_ptr().add(_i)) = piece_offset };
                    //*unsafe { keys.get_unchecked_mut(_i) } = piece_offset;
                }
            }
            downs.resize(keys_count + 1, NodePieceOffset::new(0));
            if is_leaf == 0 {
                for _i in 0..(keys_count + 1) {
                    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                    let node_offset = file
                        .read_node_offset()
                        .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                    #[cfg(feature = "vf_node_u32")]
                    let node_offset = file
                        .read_node_offset_u32()
                        .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                    #[cfg(feature = "vf_node_u64")]
                    let node_offset = file
                        .read_node_offset_u64()
                        .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                    debug_assert!(
                        (node_offset.as_value() & 0x0F) == 0,
                        "(node_offset.as_value(): {} & 0x0F) == 0, offset: {}",
                        node_offset,
                        offset.as_value()
                    );
                    #[cfg(feature = "siamese_debug")]
                    {
                        downs[_i] = node_offset;
                    }
                    #[cfg(not(feature = "siamese_debug"))]
                    {
                        unsafe { *(downs.as_mut_ptr().add(_i)) = node_offset };
                        //*unsafe { downs.get_unchecked_mut(_i) } = node_offset;
                    }
                }
            }
        }
        #[cfg(feature = "idx_straight")]
        {
            keys.resize(keys_count, KeyPieceOffset::new(0));
            #[cfg(not(feature = "siamese_debug"))]
            let keys_ptr = keys.as_mut_ptr();
            for _i in 0..keys_count {
                #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                let piece_offset = file
                    .read_piece_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                #[cfg(feature = "vf_node_u32")]
                let piece_offset = file
                    .read_piece_offset_u32()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                #[cfg(feature = "vf_node_u64")]
                let piece_offset = file
                    .read_piece_offset_u64()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                debug_assert!(!piece_offset.is_zero());
                #[cfg(feature = "siamese_debug")]
                {
                    keys[_i] = piece_offset;
                }
                #[cfg(not(feature = "siamese_debug"))]
                {
                    unsafe {
                        *(keys_ptr.add(_i)) = piece_offset;
                    }
                }
            }
            downs.resize(keys_count + 1, NodePieceOffset::new(0));
            if is_leaf == 0 {
                #[cfg(not(feature = "siamese_debug"))]
                let downs_ptr = downs.as_mut_ptr();
                for _i in 0..(keys_count + 1) {
                    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                    let node_offset = file
                        .read_node_offset()
                        .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                    #[cfg(feature = "vf_node_u32")]
                    let node_offset = file
                        .read_node_offset_u32()
                        .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                    #[cfg(feature = "vf_node_u64")]
                    let node_offset = file
                        .read_node_offset_u64()
                        .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                    debug_assert!(
                        (node_offset.as_value() & 0x0F) == 0,
                        "(node_offset.as_value(): {} & 0x0F) == 0, offset: {}",
                        node_offset,
                        offset.as_value()
                    );
                    #[cfg(feature = "siamese_debug")]
                    {
                        downs[_i] = node_offset;
                    }
                    #[cfg(not(feature = "siamese_debug"))]
                    {
                        unsafe {
                            *downs_ptr.add(_i) = node_offset;
                        }
                    }
                }
            }
        }
        /*
        #[cfg(feature = "idx_straight")]
        {
            if keys_count == 0 {
                if is_leaf == 0 {
                    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
                    let node_offset = file
                        .read_node_offset()?;
                    #[cfg(feature = "vf_node_u32")]
                    let node_offset = file
                        .read_node_offset_u32()?;
                    #[cfg(feature = "vf_node_u64")]
                    let node_offset = file
                        .read_node_offset_u64()?;
                    debug_assert!(
                        (node_offset.as_value() & 0x0F) == 0,
                        "(node_offset.as_value(): {} & 0x0F) == 0, offset: {}",
                        node_offset,
                        offset.as_value()
                    );
                    downs.push(node_offset);
                }
            } else {
                keys.resize(keys_count, KeyPieceOffset::new(0));
                //let dest = &mut keys[0..];
                let maybe_slice = file.read_exact_maybeslice(keys_count)?;
                let dest = unsafe { std::mem::transmute(keys.as_mut_ptr()) };
                let src = unsafe { std::mem::transmute((&maybe_slice).as_ptr()) };
                let count = keys_count * 8;
                unsafe { std::ptr::copy_nonoverlapping::<u8>(src, dest, count) };
                //
                downs.resize(keys_count + 1, NodePieceOffset::new(0));
                if is_leaf == 0 {
                    //let dest = &mut downs[0..];
                    let maybe_slice = file.read_exact_maybeslice(keys_count + 1)?;
                    let dest = unsafe { std::mem::transmute(downs.as_mut_ptr()) };
                    let src = unsafe { std::mem::transmute((&maybe_slice).as_ptr()) };
                    let count = (keys_count + 1) * 8;
                    unsafe { std::ptr::copy_nonoverlapping::<u8>(src, dest, count) };
                }
            }
        }
        */
        //
        #[cfg(feature = "tr_has_short_key")]
        {
            short_keys.resize(keys_count, None);
            for _i in 0..keys_count {
                let short_key_size = file.read_u8()?;
                if short_key_size != 0 {
                    let short_key_size = short_key_size & 0x7F;
                    let short_key_string = file.read_exact_maybeslice(short_key_size as usize)?;
                    let vec = short_key_string.to_vec();
                    #[cfg(feature = "siamese_debug")]
                    {
                        short_keys[_i] = Some(vec);
                    }
                    #[cfg(not(feature = "siamese_debug"))]
                    {
                        unsafe { *(short_keys.as_mut_ptr().add(_i)) = Some(vec) };
                    }
                }
            }
        }
        //
        debug_assert!(_start_pos + node_size >= file.seek_position()?);
        //
        #[cfg(not(feature = "tr_has_short_key"))]
        let node_ = IdxNode::with_node_size_vec_1(offset, node_size, keys, downs);
        #[cfg(feature = "tr_has_short_key")]
        let node_ = IdxNode::with_node_size_vec_2(offset, node_size, keys, downs, short_keys);
        //
        Ok((node_, node_size))
    }
}
