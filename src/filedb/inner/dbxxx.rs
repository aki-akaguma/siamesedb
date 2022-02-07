use super::super::super::{DbMapKeyType, DbXxxBase, DbXxxObjectSafe};
use super::super::{
    CheckFileDbMap, CountOfPerSize, FileDbParams, KeysCountStats, LengthStats, RecordSizeStats,
};
use super::semtype::*;
use super::tr::IdxNode;
use super::{idx, key, val};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::io::Result;
use std::path::Path;
use std::rc::Rc;

#[cfg(all(
    feature = "idx_find_uu",
    any(feature = "vf_node_u32", feature = "vf_node_u64")
))]
use rabuf::SmallRead;

#[cfg(feature = "htx")]
use super::htx;

#[derive(Debug)]
pub struct FileDbXxxInner<KT: DbMapKeyType> {
    dirty: bool,
    //
    key_file: key::KeyFile<KT>,
    val_file: val::ValueFile,
    idx_file: idx::IdxFile,
    #[cfg(feature = "htx")]
    htx_file: htx::HtxFile,
    //
    _phantom: std::marker::PhantomData<KT>,
}

impl<KT: DbMapKeyType> FileDbXxxInner<KT> {
    pub(crate) fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        params: FileDbParams,
    ) -> Result<FileDbXxxInner<KT>> {
        let key_file = key::KeyFile::open_with_params(&path, ks_name, KT::signature(), &params)?;
        let val_file = val::ValueFile::open_with_params(&path, ks_name, KT::signature(), &params)?;
        let idx_file = idx::IdxFile::open_with_params(&path, ks_name, KT::signature(), &params)?;
        #[cfg(feature = "htx")]
        let htx_file = htx::HtxFile::open_with_params(&path, ks_name, KT::signature(), &params)?;
        //
        Ok(Self {
            key_file,
            val_file,
            idx_file,
            #[cfg(feature = "htx")]
            htx_file,
            dirty: false,
            _phantom: std::marker::PhantomData,
        })
    }
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

// for utils
impl<KT: DbMapKeyType> FileDbXxxInner<KT> {
    #[inline]
    pub(crate) fn load_key_data(&self, piece_offset: KeyPieceOffset) -> Result<KT> {
        debug_assert!(!piece_offset.is_zero());
        self.key_file.read_piece_only_key(piece_offset)
    }
    #[inline]
    fn load_key_piece_size(&self, piece_offset: KeyPieceOffset) -> Result<KeyPieceSize> {
        self.key_file.read_piece_only_size(piece_offset)
    }
    #[inline]
    fn load_key_length(&self, piece_offset: KeyPieceOffset) -> Result<KeyLength> {
        self.key_file.read_piece_only_key_length(piece_offset)
    }
    //
    #[inline]
    fn load_value(&self, piece_offset: KeyPieceOffset) -> Result<Vec<u8>> {
        debug_assert!(!piece_offset.is_zero());
        let value_offset = self.key_file.read_piece_only_value_offset(piece_offset)?;
        self.val_file.read_piece_only_value(value_offset)
    }
    #[inline]
    fn load_value_piece_size(&self, piece_offset: KeyPieceOffset) -> Result<ValuePieceSize> {
        let value_offset = self.key_file.read_piece_only_value_offset(piece_offset)?;
        self.val_file.read_piece_only_size(value_offset)
    }
    #[inline]
    fn load_value_length(&self, piece_offset: KeyPieceOffset) -> Result<ValueLength> {
        let value_offset = self.key_file.read_piece_only_value_offset(piece_offset)?;
        self.val_file.read_piece_only_value_length(value_offset)
    }

    #[cfg(all(
        feature = "idx_find_uu",
        any(feature = "vf_node_u32", feature = "vf_node_u64")
    ))]
    fn keys_binary_search_uu_kt(
        &mut self,
        node_offset: NodePieceOffset,
        key_kt: &KT,
    ) -> Result<std::result::Result<KeyPieceOffset, NodePieceOffset>> {
        #[cfg(feature = "vf_node_u32")]
        const OFFSET_BYTE_SIZE: u32 = 4;
        #[cfg(feature = "vf_node_u64")]
        const OFFSET_BYTE_SIZE: u32 = 8;
        //
        let mut locked_key = self.key_file.0.borrow_mut();
        let mut locked_idx = self.idx_file.0.borrow_mut();
        //
        let _ = locked_idx.0.seek_from_start(node_offset)?;
        let _ = locked_idx.0.read_node_size()?;
        let is_leaf = locked_idx.0.read_u16_le()?;
        let keys_count = locked_idx.0.read_keys_count()?;
        if keys_count.is_zero() {
            return Ok(Err(NodePieceOffset::new(0)));
        }
        let keys_start: NodePieceOffset = locked_idx.0.seek_position()?;
        let keys_count = keys_count.as_value() as u32;
        //
        let mut left = 0;
        let mut right = keys_count;
        while left < right {
            let mid = (left + right) / 2;
            //
            // SAFETY: `mid` is limited by `[left; right)` bound.
            //let key_offset = node.keys[mid];
            let _ = locked_idx
                .0
                .seek_from_start(keys_start + NodePieceSize::new(OFFSET_BYTE_SIZE * mid))?;
            #[cfg(feature = "vf_node_u32")]
            let key_offset: KeyPieceOffset = locked_idx.0.read_piece_offset_u32()?;
            #[cfg(feature = "vf_node_u64")]
            let key_offset: KeyPieceOffset = locked_idx.0.read_piece_offset_u64()?;
            //
            debug_assert!(!key_offset.is_zero());
            let key_string = locked_key.read_piece_only_key_maybeslice(key_offset)?;
            match key_kt.cmp_u8(&key_string) {
                Ordering::Greater => left = mid + 1,
                Ordering::Equal => {
                    return Ok(Ok(key_offset));
                }
                Ordering::Less => right = mid,
            }
        }
        if is_leaf == 0 {
            let _ = locked_idx.0.seek_from_start(
                keys_start + NodePieceSize::new(OFFSET_BYTE_SIZE * (keys_count + left)),
            )?;
            #[cfg(feature = "vf_node_u32")]
            let node_offset = locked_idx.0.read_node_offset_u32()?;
            #[cfg(feature = "vf_node_u64")]
            let node_offset = locked_idx.0.read_node_offset_u64()?;
            Ok(Err(node_offset))
        } else {
            Ok(Err(NodePieceOffset::new(0)))
        }
    }
    //
    #[cfg(not(feature = "tr_has_short_key"))]
    fn keys_binary_search_kt(
        &mut self,
        keys: &[KeyPieceOffset],
        key_kt: &KT,
    ) -> Result<std::result::Result<usize, usize>> {
        let mut locked_key = self.key_file.0.borrow_mut();
        //
        let keys_count = keys.len();
        //
        let mut left = 0;
        let mut right = keys_count;
        while left < right {
            let mid = (left + right) / 2;
            //
            // SAFETY: `mid` is limited by `[left; right)` bound.
            //let key_offset = node.keys[mid];
            let key_offset = unsafe { *keys.get_unchecked(mid) };
            //
            debug_assert!(!key_offset.is_zero());
            let key_string = locked_key.read_piece_only_key_maybeslice(key_offset)?;
            match key_kt.cmp_u8(&key_string) {
                Ordering::Greater => left = mid + 1,
                Ordering::Equal => {
                    return Ok(Ok(mid));
                }
                Ordering::Less => right = mid,
            }
        }
        Ok(Err(left))
    }
    #[cfg(feature = "tr_has_short_key")]
    fn keys_binary_search_kt(
        &mut self,
        keys: &[KeyPieceOffset],
        short_keys: &[Option<Vec<u8>>],
        key_kt: &KT,
    ) -> Result<std::result::Result<usize, usize>> {
        let mut locked_key = self.key_file.0.borrow_mut();
        //
        let keys_count = keys.len();
        //
        let mut left = 0;
        let mut right = keys_count;
        while left < right {
            let mid = (left + right) / 2;
            //
            #[cfg(feature = "siamese_debug")]
            let short_key = &short_keys[mid];
            #[cfg(not(feature = "siamese_debug"))]
            let short_key = unsafe { &*short_keys.get_unchecked(mid) };
            //
            if let Some(key_string_vec) = short_key {
                match key_kt.cmp_u8(&key_string_vec) {
                    Ordering::Greater => left = mid + 1,
                    Ordering::Equal => {
                        return Ok(Ok(mid));
                    }
                    Ordering::Less => right = mid,
                }
            } else {
                // SAFETY: `mid` is limited by `[left; right)` bound.
                #[cfg(feature = "siamese_debug")]
                let key_offset = keys[mid];
                #[cfg(not(feature = "siamese_debug"))]
                let key_offset = unsafe { *keys.get_unchecked(mid) };
                //
                debug_assert!(!key_offset.is_zero());
                let key_string = locked_key.read_piece_only_key_maybeslice(key_offset)?;
                match key_kt.cmp_u8(&key_string) {
                    Ordering::Greater => left = mid + 1,
                    Ordering::Equal => {
                        return Ok(Ok(mid));
                    }
                    Ordering::Less => right = mid,
                }
            }
        }
        Ok(Err(left))
    }
    //
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
impl<KT: DbMapKeyType + std::fmt::Display> CheckFileDbMap for FileDbXxxInner<KT> {
    #[cfg(feature = "htx")]
    fn ht_size_and_count(&self) -> Result<(u64, u64)> {
        self.htx_file.ht_size_and_count()
    }
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
    /// count of the free key piece
    fn count_of_free_key_piece(&self) -> Result<CountOfPerSize> {
        self.key_file.count_of_free_key_piece()
    }
    /// count of the free value piece
    fn count_of_free_value_piece(&self) -> Result<CountOfPerSize> {
        self.val_file.count_of_free_value_piece()
    }
    /// count of the used piece and the used node
    fn count_of_used_node(&self) -> Result<(CountOfPerSize, CountOfPerSize, CountOfPerSize)> {
        self.idx_file.count_of_used_node(|off| {
            let ks = self.load_key_piece_size(off);
            if let Err(err) = ks {
                Err(err)
            } else {
                let vs = self.load_value_piece_size(off);
                if let Err(err) = vs {
                    Err(err)
                } else {
                    Ok((ks.unwrap(), vs.unwrap()))
                }
            }
        })
    }
    /// buffer statistics
    #[cfg(feature = "buf_stats")]
    fn buf_stats(&self) -> Vec<(String, i64)> {
        let mut vec = self.dat_file.buf_stats();
        let mut vec2 = self.idx_file.buf_stats();
        vec.append(&mut vec2);
        vec
    }
    /// piece size statistics
    fn key_piece_size_stats(&self) -> Result<RecordSizeStats<Key>> {
        self.idx_file
            .piece_size_stats(|off| self.load_key_piece_size(off))
    }
    fn value_piece_size_stats(&self) -> Result<RecordSizeStats<Value>> {
        self.idx_file
            .piece_size_stats(|off| self.load_value_piece_size(off))
    }
    /// keys count statistics
    fn keys_count_stats(&self) -> Result<KeysCountStats> {
        self.idx_file.keys_count_stats()
    }
    /// key length statistics
    fn key_length_stats(&self) -> Result<LengthStats<Key>> {
        self.idx_file
            .length_stats::<Key, _>(|off| self.load_key_length(off))
    }
    /// value length statistics
    fn value_length_stats(&self) -> Result<LengthStats<Value>> {
        self.idx_file
            .length_stats::<Value, _>(|off| self.load_value_length(off))
    }
    #[cfg(feature = "htx")]
    fn htx_filling_rate_per_mill(&self) -> Result<(u64, u32)> {
        self.htx_file.htx_filling_rate_per_mill()
    }
}

// insert: NEW
impl<KT: DbMapKeyType> FileDbXxxInner<KT> {
    fn insert_into_node_tree_kt(
        &mut self,
        mut node_: IdxNode,
        key_kt: &KT,
        value: &[u8],
    ) -> Result<IdxNode> {
        let r = {
            let node = node_.get_ref();
            if node.keys_is_empty() {
                return self.keys_is_empty_on_insert_(key_kt, value);
            }
            #[cfg(not(feature = "tr_has_short_key"))]
            let r = self.keys_binary_search_kt(node.keys(), key_kt)?;
            #[cfg(feature = "tr_has_short_key")]
            let r = self.keys_binary_search_kt(node.keys(), node.short_keys(), key_kt)?;
            //
            r
        };
        match r {
            Ok(k) => {
                #[cfg(not(feature = "tr_has_short_key"))]
                #[cfg(feature = "siamese_debug")]
                let key_offset = node_.get_ref().keys_get(k);
                #[cfg(not(feature = "tr_has_short_key"))]
                #[cfg(not(feature = "siamese_debug"))]
                let key_offset = unsafe { node_.get_ref().keys_get_unchecked(k) };
                //
                #[cfg(feature = "tr_has_short_key")]
                #[cfg(feature = "siamese_debug")]
                let (key_offset, short_key) = node_.get_ref().keys_get(k);
                #[cfg(feature = "tr_has_short_key")]
                #[cfg(not(feature = "siamese_debug"))]
                let (key_offset, short_key) = unsafe { node_.get_ref().keys_get_unchecked(k) };
                //
                debug_assert!(!key_offset.is_zero());
                let new_key_offset = self.store_value_on_insert(key_offset, value)?;
                if key_offset == new_key_offset {
                    Ok(node_)
                } else {
                    #[cfg(feature = "htx")]
                    {
                        let hash = key_kt.hash_value();
                        self.htx_file.write_key_piece_offset(hash, new_key_offset)?;
                    }
                    #[cfg(not(feature = "tr_has_short_key"))]
                    node_.get_mut().keys_set(k, new_key_offset);
                    #[cfg(feature = "tr_has_short_key")]
                    node_
                        .get_mut()
                        .keys_set(k, new_key_offset, short_key.map(|o| o.to_vec()));
                    //
                    self.write_node(node_)
                }
            }
            Err(k) => {
                #[cfg(feature = "siamese_debug")]
                let node_offset1 = node_.get_ref().downs_get(k);
                #[cfg(not(feature = "siamese_debug"))]
                let node_offset1 = unsafe { node_.get_ref().downs_get_unchecked(k) };
                //
                let node2_ = if !node_offset1.is_zero() {
                    let node1_ = self.idx_file.read_node(node_offset1)?;
                    self.insert_into_node_tree_kt(node1_, key_kt, value)?
                } else {
                    let new_val_piece = self.val_file.add_value_piece(value)?;
                    let new_key_piece =
                        self.key_file.add_key_piece(key_kt, new_val_piece.offset)?;
                    #[cfg(feature = "htx")]
                    {
                        let hash = key_kt.hash_value();
                        self.htx_file
                            .write_key_piece_offset(hash, new_key_piece.offset)?;
                    }
                    //
                    #[cfg(not(feature = "tr_has_short_key"))]
                    let new_active_node = IdxNode::new_active(
                        new_key_piece.offset,
                        NodePieceOffset::new(0),
                        NodePieceOffset::new(0),
                    );
                    #[cfg(feature = "tr_has_short_key")]
                    let new_active_node = IdxNode::new_active(
                        new_key_piece.offset,
                        NodePieceOffset::new(0),
                        NodePieceOffset::new(0),
                        key_kt.as_short_bytes().map(|o| o.to_vec()),
                    );
                    //
                    new_active_node
                };
                if !node2_.is_active_on_insert() {
                    debug_assert!(!node2_.get_ref().offset().is_zero());
                    let node2_ = self.write_node(node2_)?;
                    node_.get_mut().downs_set(k, node2_.get_ref().offset());
                    self.write_node(node_)
                } else {
                    self.balance_on_insert(node_, k, &node2_)
                }
            }
        }
    }
    fn keys_is_empty_on_insert_(&mut self, key_kt: &KT, value: &[u8]) -> Result<IdxNode> {
        let new_val_piece = self.val_file.add_value_piece(value)?;
        let new_key_piece = self.key_file.add_key_piece(key_kt, new_val_piece.offset)?;
        #[cfg(feature = "htx")]
        {
            let off = new_key_piece.offset;
            let hash = new_key_piece.hash_value();
            self.htx_file.write_key_piece_offset(hash, off)?;
        }
        //
        #[cfg(not(feature = "tr_has_short_key"))]
        let new_active_node = IdxNode::new_active(
            new_key_piece.offset,
            NodePieceOffset::new(0),
            NodePieceOffset::new(0),
        );
        #[cfg(feature = "tr_has_short_key")]
        let new_active_node = IdxNode::new_active(
            new_key_piece.offset,
            NodePieceOffset::new(0),
            NodePieceOffset::new(0),
            key_kt.as_short_bytes().map(|o| o.to_vec()),
        );
        //
        Ok(new_active_node)
    }
    #[inline]
    fn store_value_on_insert(
        &mut self,
        piece_offset: KeyPieceOffset,
        value: &[u8],
    ) -> Result<KeyPieceOffset> {
        let mut key_piece = self.key_file.read_piece(piece_offset)?;
        let mut val_piece = self.val_file.read_piece(key_piece.value_offset)?;
        val_piece.value = value.to_vec();
        let new_value_piece = self.val_file.write_piece(val_piece)?;
        let new_key_piece = if key_piece.value_offset == new_value_piece.offset {
            key_piece
        } else {
            key_piece.value_offset = new_value_piece.offset;
            self.key_file.write_piece(key_piece)?
        };
        Ok(new_key_piece.offset)
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
        {
            let mut node = node_.get_mut();
            let active_node = active_node_.get_ref();
            //
            #[cfg(not(feature = "tr_has_short_key"))]
            #[cfg(feature = "siamese_debug")]
            let key_0 = active_node.keys_get(0);
            #[cfg(not(feature = "tr_has_short_key"))]
            #[cfg(not(feature = "siamese_debug"))]
            let key_0 = unsafe { active_node.keys_get_unchecked(0) };
            //
            #[cfg(feature = "tr_has_short_key")]
            #[cfg(feature = "siamese_debug")]
            let (key_0, short_key_0) = active_node.keys_get(0);
            #[cfg(feature = "tr_has_short_key")]
            #[cfg(not(feature = "siamese_debug"))]
            let (key_0, short_key_0) = unsafe { active_node.keys_get_unchecked(0) };
            //
            #[cfg(feature = "siamese_debug")]
            let down_0 = active_node.downs_get(0);
            #[cfg(not(feature = "siamese_debug"))]
            let down_0 = unsafe { active_node.downs_get_unchecked(0) };
            //
            #[cfg(feature = "siamese_debug")]
            let down_1 = active_node.downs_get(1);
            #[cfg(not(feature = "siamese_debug"))]
            let down_1 = unsafe { active_node.downs_get_unchecked(1) };
            //
            node.downs_set(i, down_1);
            node.downs_insert(i, down_0);
            #[cfg(not(feature = "tr_has_short_key"))]
            node.keys_insert(i, key_0);
            #[cfg(feature = "tr_has_short_key")]
            node.keys_insert(i, key_0, short_key_0.map(|o| o.to_vec()));
        }
        //
        if !node_.borrow().is_over_len() {
            let node = self.write_node(node_)?;
            Ok(node)
        } else {
            self.split_on_insert(node_)
        }
    }
    #[inline]
    fn split_on_insert(&mut self, mut node_: IdxNode) -> Result<IdxNode> {
        debug_assert!(node_.get_ref().keys_len() == idx::NODE_SLOTS_MAX as usize);
        debug_assert!(node_.get_ref().downs_len() == idx::NODE_SLOTS_MAX as usize + 1);
        debug_assert!(node_.get_ref().keys_len() >= idx::NODE_SLOTS_MAX_HALF as usize);
        debug_assert!(node_.get_ref().downs_len() >= idx::NODE_SLOTS_MAX_HALF as usize);
        let mut node1_ = IdxNode::new_empty();
        {
            let mut node1 = node1_.get_mut();
            let node = node_.get_ref();
            node1.keys_downs_extend_from_node(&node, idx::NODE_SLOTS_MAX_HALF as usize);
        }
        #[cfg(not(feature = "tr_has_short_key"))]
        let key_offset1 = node_
            .get_mut()
            .keys_downs_resize(idx::NODE_SLOTS_MAX_HALF as usize);
        #[cfg(feature = "tr_has_short_key")]
        let (key_offset1, short_key1) = node_
            .get_mut()
            .keys_downs_resize(idx::NODE_SLOTS_MAX_HALF as usize);
        //
        let node1_ = self.write_new_node(node1_)?;
        let node_ = self.write_node(node_)?;
        let node_offset = node_.get_ref().offset();
        let node1_offset = node1_.get_ref().offset();
        //
        #[cfg(not(feature = "tr_has_short_key"))]
        let new_active_node = IdxNode::new_active(key_offset1, node_offset, node1_offset);
        #[cfg(feature = "tr_has_short_key")]
        let new_active_node =
            IdxNode::new_active(key_offset1, node_offset, node1_offset, short_key1);
        //
        Ok(new_active_node)
    }
}

// delete: NEW
impl<KT: DbMapKeyType> FileDbXxxInner<KT> {
    fn delete_from_node_tree_kt(
        &mut self,
        mut node_: IdxNode,
        key_kt: &KT,
    ) -> Result<(IdxNode, Option<Vec<u8>>)> {
        let r = {
            if node_.get_ref().keys_is_empty() {
                return Ok((node_, None));
            }
            let node = node_.get_ref();
            #[cfg(not(feature = "tr_has_short_key"))]
            let r = self.keys_binary_search_kt(node.keys(), key_kt)?;
            #[cfg(feature = "tr_has_short_key")]
            let r = self.keys_binary_search_kt(node.keys(), node.short_keys(), key_kt)?;
            r
        };
        match r {
            Ok(k) => {
                let (node_, val) = self.delete_at(node_, k)?;
                return Ok((node_, val));
            }
            Err(k) => {
                #[cfg(feature = "siamese_debug")]
                let node_offset1 = node_.get_ref().downs_get(k);
                #[cfg(not(feature = "siamese_debug"))]
                let node_offset1 = unsafe { node_.get_ref().downs_get_unchecked(k) };
                //
                if !node_offset1.is_zero() {
                    let node1_ = self.idx_file.read_node(node_offset1)?;
                    let (node1_, val) = self.delete_from_node_tree_kt(node1_, key_kt)?;
                    node_.get_mut().downs_set(k, node1_.get_ref().offset());
                    let node_ = self.write_node(node_)?;
                    if k == node_.get_ref().downs_len() - 1 {
                        let node_ = self.balance_right(node_, k)?;
                        return Ok((node_, val));
                    } else {
                        let node_ = self.balance_left(node_, k)?;
                        return Ok((node_, val));
                    }
                }
            }
        }
        Ok((node_, None))
    }
    #[inline]
    fn delete_at(&mut self, mut node_: IdxNode, i: usize) -> Result<(IdxNode, Option<Vec<u8>>)> {
        #[cfg(not(feature = "tr_has_short_key"))]
        let key_offset = node_.get_ref().keys_get(i);
        #[cfg(feature = "tr_has_short_key")]
        let (key_offset, _short_key) = node_.get_ref().keys_get(i);
        debug_assert!(!key_offset.is_zero(), "key_offset: {} != 0", key_offset);
        //
        let opt_value = {
            let key_piece = self.key_file.read_piece(key_offset)?;
            #[cfg(feature = "htx")]
            {
                let hash = key_piece.key.hash_value();
                self.htx_file
                    .write_key_piece_offset(hash, KeyPieceOffset::new(0))?;
            }
            let value = self
                .val_file
                .read_piece_only_value(key_piece.value_offset)?;
            self.val_file.delete_piece(key_piece.value_offset)?;
            self.key_file.delete_piece(key_offset)?;
            Some(value)
        };
        let node_offset1 = node_.get_ref().downs_get(i);
        if node_offset1.is_zero() {
            let _key_offset = node_.get_mut().keys_remove(i);
            let _node_offset = node_.get_mut().downs_remove(i);
            let new_node_ = self.write_node(node_)?;
            Ok((new_node_, opt_value))
        } else {
            let node1_ = self.idx_file.read_node(node_offset1)?;
            #[cfg(not(feature = "tr_has_short_key"))]
            let (key_offset, node1_) = self.delete_max(node1_)?;
            #[cfg(feature = "tr_has_short_key")]
            let (key_offset, short_key, node1_) = self.delete_max(node1_)?;
            //
            #[cfg(not(feature = "tr_has_short_key"))]
            node_.get_mut().keys_set(i, key_offset);
            #[cfg(feature = "tr_has_short_key")]
            node_.get_mut().keys_set(i, key_offset, short_key);
            //
            node_.get_mut().downs_set(i, node1_.get_ref().offset());
            let node_ = self.write_node(node_)?;
            let new_node_ = self.balance_left(node_, i)?;
            Ok((new_node_, opt_value))
        }
    }
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    fn delete_max(&mut self, mut node_: IdxNode) -> Result<(KeyPieceOffset, IdxNode)> {
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
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    fn delete_max(
        &mut self,
        mut node_: IdxNode,
    ) -> Result<(KeyPieceOffset, Option<Vec<u8>>, IdxNode)> {
        let j = node_.get_ref().keys_len();
        let i = j - 1;
        let node_offset1 = node_.get_ref().downs_get(j);
        if node_offset1.is_zero() {
            node_.get_mut().downs_remove(j);
            let (key_offset2, short_key2) = node_.get_mut().keys_remove(i);
            let new_node_ = self.write_node(node_)?;
            Ok((key_offset2, short_key2, new_node_))
        } else {
            let node1_ = self.idx_file.read_node(node_offset1)?;
            let (key_offset2, short_key2, node1_) = self.delete_max(node1_)?;
            node_.get_mut().downs_set(j, node1_.get_ref().offset());
            let node_ = self.write_node(node_)?;
            let new_node_ = self.balance_right(node_, j)?;
            Ok((key_offset2, short_key2, new_node_))
        }
    }
    #[inline]
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
        //
        #[cfg(not(feature = "tr_has_short_key"))]
        let key_offset2 = node_.get_ref().keys_get(i);
        #[cfg(feature = "tr_has_short_key")]
        let (key_offset2, short_key2) = node_.get_ref().keys_get(i);
        debug_assert!(!key_offset2.is_zero(), "key_offset2: {} != 0", key_offset2);
        //
        let node_offset2 = node_.get_ref().downs_get(j);
        debug_assert!(!node_offset2.is_zero());
        if !node_offset2.is_zero() {
            let mut node2_ = self.idx_file.read_node(node_offset2)?;
            if node2_.get_ref().downs_len() == idx::NODE_SLOTS_MAX_HALF as usize {
                // unification
                #[cfg(not(feature = "tr_has_short_key"))]
                node1_.get_mut().keys_push(key_offset2);
                #[cfg(feature = "tr_has_short_key")]
                node1_
                    .get_mut()
                    .keys_push(key_offset2, short_key2.map(|o| o.to_vec()));
                //
                node1_
                    .get_mut()
                    .keys_downs_extend_from_node(&node2_.get_ref(), 0);
                self.idx_file.delete_node(node2_)?;
                //
                node_.get_mut().keys_remove(i);
                node_.get_mut().downs_remove(j);
                //
                let node1_ = self.write_node(node1_)?;
                node_.get_mut().downs_set(i, node1_.get_ref().offset());
            } else {
                #[cfg(not(feature = "tr_has_short_key"))]
                let key_offset3 =
                    self.move_a_node_from_right_to_left(key_offset2, &mut node1_, &mut node2_);
                #[cfg(feature = "tr_has_short_key")]
                let (key_offset3, short_key3) = self.move_a_node_from_right_to_left(
                    key_offset2,
                    short_key2.map(|o| o.to_vec()),
                    &mut node1_,
                    &mut node2_,
                );
                //
                #[cfg(not(feature = "tr_has_short_key"))]
                node_.get_mut().keys_set(i, key_offset3);
                #[cfg(feature = "tr_has_short_key")]
                node_.get_mut().keys_set(i, key_offset3, short_key3);
                //
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
    #[inline]
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
        #[cfg(not(feature = "tr_has_short_key"))]
        let key_offset2 = node_.get_ref().keys_get(i);
        #[cfg(feature = "tr_has_short_key")]
        let (key_offset2, short_key2) = node_.get_ref().keys_get(i);
        debug_assert!(!key_offset2.is_zero(), "key_offset2: {} != 0", key_offset2);
        //
        let node_offset2 = node_.get_ref().downs_get(i);
        debug_assert!(!node_offset2.is_zero());
        if !node_offset2.is_zero() {
            let mut node2_ = self.idx_file.read_node(node_offset2)?;
            if node2_.get_ref().downs_len() == idx::NODE_SLOTS_MAX_HALF as usize {
                // unification
                #[cfg(not(feature = "tr_has_short_key"))]
                node2_.get_mut().keys_push(key_offset2);
                #[cfg(feature = "tr_has_short_key")]
                node2_
                    .get_mut()
                    .keys_push(key_offset2, short_key2.map(|o| o.to_vec()));
                //
                node2_
                    .get_mut()
                    .keys_downs_extend_from_node(&node1_.get_ref(), 0);
                self.idx_file.delete_node(node1_)?;
                //
                node_.get_mut().keys_remove(i);
                node_.get_mut().downs_remove(j);
                //
                let node2_ = self.write_node(node2_)?;
                node_.get_mut().downs_set(i, node2_.get_ref().offset());
            } else {
                #[cfg(not(feature = "tr_has_short_key"))]
                let key_offset3 = self.move_left_right(key_offset2, &mut node2_, &mut node1_);
                #[cfg(feature = "tr_has_short_key")]
                let (key_offset3, short_key3) = self.move_left_right(
                    key_offset2,
                    short_key2.map(|o| o.to_vec()),
                    &mut node2_,
                    &mut node1_,
                );
                //
                #[cfg(not(feature = "tr_has_short_key"))]
                node_.get_mut().keys_set(i, key_offset3);
                #[cfg(feature = "tr_has_short_key")]
                node_.get_mut().keys_set(i, key_offset3, short_key3);
                //
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
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    fn move_a_node_from_right_to_left(
        &mut self,
        key_offset: KeyPieceOffset,
        node_l: &mut IdxNode,
        node_r: &mut IdxNode,
    ) -> KeyPieceOffset {
        node_l.get_mut().keys_push(key_offset);
        node_l
            .get_mut()
            .downs_push(node_r.get_mut().downs_remove(0));
        node_r.get_mut().keys_remove(0)
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    fn move_a_node_from_right_to_left(
        &mut self,
        key_offset: KeyPieceOffset,
        short_key: Option<Vec<u8>>,
        node_l: &mut IdxNode,
        node_r: &mut IdxNode,
    ) -> (KeyPieceOffset, Option<Vec<u8>>) {
        node_l.get_mut().keys_push(key_offset, short_key);
        node_l
            .get_mut()
            .downs_push(node_r.get_mut().downs_remove(0));
        node_r.get_mut().keys_remove(0)
    }
    #[cfg(not(feature = "tr_has_short_key"))]
    #[inline]
    fn move_left_right(
        &mut self,
        key_offset: KeyPieceOffset,
        node_l: &mut IdxNode,
        node_r: &mut IdxNode,
    ) -> KeyPieceOffset {
        let j = node_l.get_ref().keys_len();
        let i = j - 1;
        node_r.get_mut().keys_insert(0, key_offset);
        node_r
            .get_mut()
            .downs_insert(0, node_l.get_mut().downs_remove(j));
        node_l.get_mut().keys_remove(i)
    }
    #[cfg(feature = "tr_has_short_key")]
    #[inline]
    fn move_left_right(
        &mut self,
        key_offset: KeyPieceOffset,
        short_key: Option<Vec<u8>>,
        node_l: &mut IdxNode,
        node_r: &mut IdxNode,
    ) -> (KeyPieceOffset, Option<Vec<u8>>) {
        let j = node_l.get_ref().keys_len();
        let i = j - 1;
        node_r.get_mut().keys_insert(0, key_offset, short_key);
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
impl<KT: DbMapKeyType> FileDbXxxInner<KT> {
    #[cfg(all(
        feature = "idx_find_uu",
        any(feature = "vf_node_u32", feature = "vf_node_u64")
    ))]
    fn find_in_node_tree_uu_kt(
        &mut self,
        node_offset: NodePieceOffset,
        key_kt: &KT,
    ) -> Result<Option<Vec<u8>>> {
        let r = self.keys_binary_search_uu_kt(node_offset, key_kt)?;
        match r {
            Ok(key_offset) => {
                debug_assert!(!key_offset.is_zero());
                self.load_value(key_offset).map(Some)
            }
            Err(node_offset) => {
                if !node_offset.is_zero() {
                    self.find_in_node_tree_uu_kt(node_offset, key_kt)
                } else {
                    Ok(None)
                }
            }
        }
    }
    #[cfg(not(all(
        feature = "idx_find_uu",
        any(feature = "vf_node_u32", feature = "vf_node_u64")
    )))]
    fn find_in_node_tree_kt(&mut self, node_: IdxNode, key_kt: &KT) -> Result<Option<Vec<u8>>> {
        let r = {
            let node = node_.get_ref();
            #[cfg(not(feature = "tr_has_short_key"))]
            let r = self.keys_binary_search_kt(node.keys(), key_kt)?;
            #[cfg(feature = "tr_has_short_key")]
            let r = self.keys_binary_search_kt(node.keys(), node.short_keys(), key_kt)?;
            //
            r
        };
        match r {
            Ok(k) => {
                #[cfg(not(feature = "tr_has_short_key"))]
                #[cfg(feature = "siamese_debug")]
                let key_offset = node_.get_ref().keys_get(k);
                #[cfg(not(feature = "tr_has_short_key"))]
                #[cfg(not(feature = "siamese_debug"))]
                let key_offset = unsafe { node_.get_ref().keys_get_unchecked(k) };
                //
                #[cfg(feature = "tr_has_short_key")]
                #[cfg(feature = "siamese_debug")]
                let (key_offset, _short_key) = node_.get_ref().keys_get(k);
                #[cfg(feature = "tr_has_short_key")]
                #[cfg(not(feature = "siamese_debug"))]
                let (key_offset, _short_key) = unsafe { node_.get_ref().keys_get_unchecked(k) };
                //
                debug_assert!(!key_offset.is_zero());
                self.load_value(key_offset).map(Some)
            }
            Err(k) => {
                #[cfg(feature = "siamese_debug")]
                let node_offset = node_.get_ref().downs_get(k);
                #[cfg(not(feature = "siamese_debug"))]
                let node_offset = unsafe { node_.get_ref().downs_get_unchecked(k) };
                //
                if !node_offset.is_zero() {
                    let node1_ = self.idx_file.read_node(node_offset)?;
                    self.find_in_node_tree_kt(node1_, key_kt)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

// impl trait: DbXxxBase
impl<KT: DbMapKeyType> DbXxxBase for FileDbXxxInner<KT> {
    #[inline]
    fn read_fill_buffer(&mut self) -> Result<()> {
        self.val_file.read_fill_buffer()?;
        self.key_file.read_fill_buffer()?;
        self.idx_file.read_fill_buffer()?;
        #[cfg(feature = "htx")]
        self.htx_file.read_fill_buffer()?;
        Ok(())
    }
    #[inline]
    fn flush(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data
            self.val_file.flush()?;
            self.key_file.flush()?;
            self.idx_file.flush()?;
            #[cfg(feature = "htx")]
            self.htx_file.flush()?;
            self.dirty = false;
        }
        Ok(())
    }
    #[inline]
    fn sync_all(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data and meta
            self.val_file.sync_all()?;
            self.key_file.sync_all()?;
            self.idx_file.sync_all()?;
            #[cfg(feature = "htx")]
            self.htx_file.sync_all()?;
            self.dirty = false;
        }
        Ok(())
    }
    #[inline]
    fn sync_data(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data
            self.val_file.sync_data()?;
            self.key_file.sync_data()?;
            self.idx_file.sync_data()?;
            #[cfg(feature = "htx")]
            self.htx_file.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }
}

// impl trait: DbXxxObjectSafe<KT>
impl<KT: DbMapKeyType> DbXxxObjectSafe<KT> for FileDbXxxInner<KT> {
    #[cfg(all(
        feature = "idx_find_uu",
        any(feature = "vf_node_u32", feature = "vf_node_u64")
    ))]
    #[inline]
    fn get_kt(&mut self, key_kt: &KT) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "htx")]
        {
            let hash = key_kt.hash_value();
            let key_offset = self.htx_file.read_key_piece_offset(hash)?;
            if !key_offset.is_zero() {
                let flg = {
                    let mut locked_key = self.key_file.0.borrow_mut();
                    let key_string = locked_key.read_piece_only_key_maybeslice(key_offset)?;
                    match key_kt.cmp_u8(&key_string) {
                        Ordering::Equal => true,
                        Ordering::Greater => false,
                        Ordering::Less => false,
                    }
                };
                if flg {
                    #[cfg(feature = "htx_print_hits")]
                    self.htx_file.set_hits();
                    return self.load_value(key_offset).map(Some);
                } else {
                    #[cfg(feature = "htx_print_hits")]
                    self.htx_file.set_miss();
                }
            }
        }
        #[cfg(feature = "node_cache")]
        {
            let mut locked_idx = RefCell::borrow_mut(&self.idx_file.0);
            locked_idx.flush_node_cache_clear()?
        }
        let node_offset = {
            let mut locked_idx = self.idx_file.0.borrow_mut();
            locked_idx.0.read_top_node_offset()?
        };
        self.find_in_node_tree_uu_kt(node_offset, key_kt)
    }
    #[cfg(not(all(
        feature = "idx_find_uu",
        any(feature = "vf_node_u32", feature = "vf_node_u64")
    )))]
    #[inline]
    fn get_kt(&mut self, key_kt: &KT) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "htx")]
        {
            let hash = key_kt.hash_value();
            let key_offset = self.htx_file.read_key_piece_offset(hash)?;
            if !key_offset.is_zero() {
                let flg = {
                    let mut locked_key = self.key_file.0.borrow_mut();
                    let key_string = locked_key.read_piece_only_key_maybeslice(key_offset)?;
                    match key_kt.cmp_u8(&key_string) {
                        Ordering::Equal => true,
                        Ordering::Greater => false,
                        Ordering::Less => false,
                    }
                };
                if flg {
                    #[cfg(feature = "htx_print_hits")]
                    self.htx_file.set_hits();
                    return self.load_value(key_offset).map(Some);
                } else {
                    #[cfg(feature = "htx_print_hits")]
                    self.htx_file.set_miss();
                }
            }
        }
        let top_node = self.idx_file.read_top_node()?;
        self.find_in_node_tree_kt(top_node, key_kt)
    }
    #[inline]
    fn put_kt(&mut self, key_kt: &KT, value: &[u8]) -> Result<()> {
        #[cfg(feature = "htx")]
        {
            let hash = key_kt.hash_value();
            let key_offset = self.htx_file.read_key_piece_offset(hash)?;
            if !key_offset.is_zero() {
                let flg = {
                    let mut locked_key = self.key_file.0.borrow_mut();
                    let key_string = locked_key.read_piece_only_key_maybeslice(key_offset)?;
                    match key_kt.cmp_u8(&key_string) {
                        Ordering::Equal => true,
                        Ordering::Greater => false,
                        Ordering::Less => false,
                    }
                };
                if flg {
                    #[cfg(feature = "htx_print_hits")]
                    self.htx_file.set_hits();
                    //
                    let new_piece_offset = self.store_value_on_insert(key_offset, value)?;
                    assert!(key_offset == new_piece_offset);
                    return Ok(());
                } else {
                    #[cfg(feature = "htx_print_hits")]
                    self.htx_file.set_miss();
                }
            }
        }
        let top_node = self.idx_file.read_top_node()?;
        let active_node = self.insert_into_node_tree_kt(top_node, key_kt, value)?;
        let new_top_node = active_node.deactivate();
        self.idx_file.write_top_node(new_top_node)?;
        Ok(())
    }
    #[inline]
    fn del_kt(&mut self, key_kt: &KT) -> Result<Option<Vec<u8>>> {
        let top_node = self.idx_file.read_top_node()?;
        let top_node_offset = top_node.get_ref().offset();
        let (top_node, opt_val) = self.delete_from_node_tree_kt(top_node, key_kt)?;
        let new_top_node = self.trim(top_node)?;
        if top_node_offset != new_top_node.get_ref().offset() {
            self.idx_file.write_top_node(new_top_node)?;
        }
        Ok(opt_val)
    }
}

// for Iterator
//
#[derive(Debug)]
pub struct DbXxxIterMut<KT: DbMapKeyType> {
    db_map: Rc<RefCell<FileDbXxxInner<KT>>>,
    /// node depth of top node to leaf node.
    depth_nodes: Vec<(IdxNode, i32, i32)>,
}

impl<KT: DbMapKeyType> DbXxxIterMut<KT> {
    pub fn new(db_map: Rc<RefCell<FileDbXxxInner<KT>>>) -> Result<Self> {
        let depth_nodes = {
            let db_map_inner = RefCell::borrow(&db_map);
            let top_node = db_map_inner.idx_file.read_top_node()?;
            let mut depth_nodes = vec![(top_node.clone(), 0, 0)];
            let mut node = top_node;
            //
            loop {
                let node_offset = node.get_ref().downs_get(0);
                if node_offset.is_zero() {
                    break;
                }
                let down_node = db_map_inner.idx_file.read_node(node_offset).unwrap();
                depth_nodes.push((down_node.clone(), 0, 0));
                node = down_node;
            }
            depth_nodes
        };
        //
        Ok(Self {
            db_map,
            depth_nodes,
        })
    }
    fn next_piece_offset(&mut self) -> Option<KeyPieceOffset> {
        if self.depth_nodes.is_empty() {
            return None;
        }
        /*
        {
            let depth = self.depth_nodes.len();
            let (_, keys_idx, downs_idx) = self.depth_nodes.last_mut().unwrap();
            eprintln!("CHECK 001: {}, {}, {}", depth, *keys_idx, *downs_idx);
        }
        */
        //
        let (key_offset, sw) = {
            let (idx_node, keys_idx, downs_idx) = self.depth_nodes.last_mut().unwrap();
            #[cfg(not(feature = "tr_has_short_key"))]
            let key_offset = if *keys_idx < *downs_idx {
                if *keys_idx < idx_node.get_ref().keys_len().try_into().unwrap() {
                    idx_node.get_ref().keys_get((*keys_idx).try_into().unwrap())
                } else {
                    return None;
                }
            } else {
                let node_offset = idx_node
                    .get_ref()
                    .downs_get((*downs_idx).try_into().unwrap());
                if node_offset.is_zero() {
                    idx_node.get_ref().keys_get((*keys_idx).try_into().unwrap())
                } else {
                    {
                        let db_map_inner = RefCell::borrow(&self.db_map);
                        let down_node = db_map_inner.idx_file.read_node(node_offset).unwrap();
                        self.depth_nodes.push((down_node, 0, 0));
                    }
                    return self.next_piece_offset();
                }
            };
            #[cfg(feature = "tr_has_short_key")]
            let (key_offset, _short_key) = if *keys_idx < *downs_idx {
                if *keys_idx < idx_node.get_ref().keys_len().try_into().unwrap() {
                    idx_node.get_ref().keys_get((*keys_idx).try_into().unwrap())
                } else {
                    return None;
                }
            } else {
                let node_offset = idx_node
                    .get_ref()
                    .downs_get((*downs_idx).try_into().unwrap());
                if node_offset.is_zero() {
                    idx_node.get_ref().keys_get((*keys_idx).try_into().unwrap())
                } else {
                    {
                        let db_map_inner = RefCell::borrow(&self.db_map);
                        let down_node = db_map_inner.idx_file.read_node(node_offset).unwrap();
                        self.depth_nodes.push((down_node, 0, 0));
                    }
                    return self.next_piece_offset();
                }
            };
            debug_assert!(!key_offset.is_zero());
            //
            *keys_idx += 1;
            if *keys_idx >= idx_node.get_ref().keys_len().try_into().unwrap() {
                //eprintln!("CHECK 002");
                if *downs_idx < idx_node.get_ref().downs_len().try_into().unwrap() {
                    let node_offset = idx_node
                        .get_ref()
                        .downs_get((*downs_idx).try_into().unwrap());
                    if !node_offset.is_zero() {
                        //eprintln!("CHECK 002.1");
                        let db_map_inner = RefCell::borrow(&self.db_map);
                        let down_node = db_map_inner.idx_file.read_node(node_offset).unwrap();
                        self.depth_nodes.push((down_node, 0, 0));
                        (key_offset, 1)
                    } else {
                        //eprintln!("CHECK 002.2");
                        (key_offset, 2)
                    }
                } else {
                    //eprintln!("CHECK 002.3");
                    let (_, _, _) = self.depth_nodes.pop().unwrap();
                    let (_, _keys_idx, downs_idx) = self.depth_nodes.last_mut().unwrap();
                    *downs_idx += 1;
                    (key_offset, 3)
                }
            } else {
                (key_offset, 0)
            }
        };
        if sw == 2 {
            loop {
                if self.depth_nodes.is_empty() {
                    break;
                }
                let (_, _, _) = self.depth_nodes.pop().unwrap();
                if self.depth_nodes.is_empty() {
                    break;
                }
                let (idx_node, _keys_idx, downs_idx) = self.depth_nodes.last_mut().unwrap();
                *downs_idx += 1;
                if *downs_idx < idx_node.get_ref().downs_len().try_into().unwrap() {
                    break;
                }
            }
        }
        //
        Some(key_offset)
    }
}

// impl trait: Iterator
impl<KT: DbMapKeyType> Iterator for DbXxxIterMut<KT> {
    type Item = (KT, Vec<u8>);
    fn next(&mut self) -> Option<(KT, Vec<u8>)> {
        if let Some(key_offset) = self.next_piece_offset() {
            let db_map_inner = RefCell::borrow_mut(&self.db_map);
            let key = db_map_inner.load_key_data(key_offset).unwrap();
            let value_vec = db_map_inner.load_value(key_offset).unwrap();
            Some((key, value_vec))
        } else {
            None
        }
    }
}

//
#[derive(Debug)]
pub struct DbXxxIter<KT: DbMapKeyType> {
    iter: DbXxxIterMut<KT>,
}

impl<KT: DbMapKeyType> DbXxxIter<KT> {
    #[inline]
    pub fn new(db_map: Rc<RefCell<FileDbXxxInner<KT>>>) -> Result<Self> {
        Ok(Self {
            iter: DbXxxIterMut::new(db_map)?,
        })
    }
}

// impl trait: Iterator
impl<KT: DbMapKeyType> Iterator for DbXxxIter<KT> {
    type Item = (KT, Vec<u8>);
    #[inline]
    fn next(&mut self) -> Option<(KT, Vec<u8>)> {
        self.iter.next()
    }
}

//
#[derive(Debug)]
pub struct DbXxxIntoIter<KT: DbMapKeyType> {
    iter: DbXxxIterMut<KT>,
}

impl<KT: DbMapKeyType> DbXxxIntoIter<KT> {
    #[inline]
    pub fn new(db_map: Rc<RefCell<FileDbXxxInner<KT>>>) -> Result<Self> {
        Ok(Self {
            iter: DbXxxIterMut::new(db_map)?,
        })
    }
}

// impl trait: Iterator
impl<KT: DbMapKeyType> Iterator for DbXxxIntoIter<KT> {
    type Item = (KT, Vec<u8>);
    #[inline]
    fn next(&mut self) -> Option<(KT, Vec<u8>)> {
        self.iter.next()
    }
}
