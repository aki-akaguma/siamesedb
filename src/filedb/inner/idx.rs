use super::super::super::DbXxxKeyType;
use super::super::{
    CountOfPerSize, FileBufSizeParam, FileDbParams, KeysCountStats, RecordSizeStats,
};
use super::dbxxx::FileDbXxxInner;
use super::semtype::*;
use super::tr::IdxNode;
use super::vfile::VarFile;
use std::cell::RefCell;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Result, Write};
use std::path::Path;
use std::rc::Rc;

type HeaderSignature = [u8; 8];

//const CHUNK_SIZE: u32 = 16 * 4 * 1024;
const CHUNK_SIZE: u32 = 1024 * 1024;
//const CHUNK_SIZE: u32 = 16 * 1024 * 1024;
const IDX_HEADER_SZ: u64 = 128;
const IDX_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'1', 0u8];
const IDX_HEADER_TOP_NODE_OFFSET: u64 = 16;

#[cfg(not(feature = "node_cache"))]
use std::marker::PhantomData;

#[cfg(feature = "node_cache")]
use super::nc::NodeCache;

#[cfg(not(feature = "node_cache"))]
#[derive(Debug)]
struct VarFileNodeCache(VarFile, PhantomData<i32>);

#[cfg(feature = "node_cache")]
#[derive(Debug)]
struct VarFileNodeCache(VarFile, NodeCache);

#[derive(Debug, Clone)]
pub struct IdxFile(Rc<RefCell<VarFileNodeCache>>);

impl IdxFile {
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        sig2: HeaderSignature,
        params: &FileDbParams,
    ) -> Result<Self> {
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.idx", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = match params.idx_buf_size {
            FileBufSizeParam::Size(val) => {
                let idx_buf_chunk_size = CHUNK_SIZE;
                let idx_buf_num_chunks = val / idx_buf_chunk_size;
                VarFile::with_capacity(
                    std_file,
                    idx_buf_chunk_size,
                    idx_buf_num_chunks.try_into().unwrap(),
                )?
            }
            FileBufSizeParam::PerMille(val) => VarFile::with_per_mille(std_file, CHUNK_SIZE, val)?,
            FileBufSizeParam::Auto => VarFile::new(std_file)?,
        };
        let file_length: NodeOffset = file.seek_to_end()?;
        //
        #[cfg(not(feature = "node_cache"))]
        let mut file_nc = VarFileNodeCache(file, PhantomData);
        #[cfg(feature = "node_cache")]
        let mut file_nc = VarFileNodeCache(file, NodeCache::new());
        //
        if file_length.is_zero() {
            file_nc.0.write_idxf_init_header(sig2)?;
            // writing top node
            let top_node = IdxNode::new(NodeOffset::new(IDX_HEADER_SZ));
            let _new_node_ = file_nc.write_node(top_node, true)?;
            debug_assert!(_new_node_.get_ref().offset() == NodeOffset::new(IDX_HEADER_SZ));
        } else {
            file_nc.0.check_idxf_header(sig2)?;
        }
        //
        Ok(Self(Rc::new(RefCell::new(file_nc))))
    }
    #[inline]
    pub fn read_fill_buffer(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.0.read_fill_buffer()
    }
    #[inline]
    pub fn flush(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        #[cfg(feature = "node_cache")]
        locked.flush_node_cache()?;
        locked.0.flush()
    }
    #[inline]
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        #[cfg(feature = "node_cache")]
        locked.flush_node_cache_clear()?;
        locked.0.sync_all()
    }
    #[inline]
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        #[cfg(feature = "node_cache")]
        locked.flush_node_cache_clear()?;
        locked.0.sync_data()
    }
    #[cfg(feature = "buf_stats")]
    #[inline]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        let locked = RefCell::borrow(&self.0);
        locked.0.buf_stats()
    }
    //
    #[inline]
    pub fn read_top_node(&self) -> Result<IdxNode> {
        let offset = {
            let mut locked = RefCell::borrow_mut(&self.0);
            locked.0.read_top_node_offset()?
        };
        self.read_node(offset)
    }
    pub fn write_top_node(&self, node: IdxNode) -> Result<IdxNode> {
        if node.get_ref().offset().is_zero() {
            let new_top_node = self.write_new_node(node)?;
            {
                let mut locked = RefCell::borrow_mut(&self.0);
                locked
                    .0
                    .write_top_node_offset(new_top_node.get_ref().offset())?;
            }
            Ok(new_top_node)
        } else {
            let top_node_offset = {
                let mut locked = RefCell::borrow_mut(&self.0);
                locked.0.read_top_node_offset()?
            };
            let new_top_node = self.write_node(node)?;
            if new_top_node.get_ref().offset() != top_node_offset {
                let mut locked = RefCell::borrow_mut(&self.0);
                locked
                    .0
                    .write_top_node_offset(new_top_node.get_ref().offset())?;
            }
            Ok(new_top_node)
        }
    }
    //
    #[inline]
    pub fn read_node(&self, offset: NodeOffset) -> Result<IdxNode> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.read_node(offset)
    }
    #[inline]
    pub fn write_node(&self, node: IdxNode) -> Result<IdxNode> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.write_node(node, false)
    }
    #[inline]
    pub fn write_new_node(&self, mut node: IdxNode) -> Result<IdxNode> {
        node.get_mut().set_offset({
            let mut locked = RefCell::borrow_mut(&self.0);
            locked.0.seek_to_end()?
        });
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.write_node(node, true)
    }
    #[inline]
    pub fn delete_node(&self, node: IdxNode) -> Result<NodeSize> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.delete_node(node)
    }
    #[inline]
    pub fn _read_node_only_keys_count(&self, offset: NodeOffset) -> Result<KeysCount> {
        //let mut locked = RefCell::borrow_mut(&self.0);
        //let idx_node = locked.read_node(offset)?;
        let idx_node = self.read_node(offset)?;
        let keys_len = idx_node.get_ref().keys_len();
        Ok(KeysCount::new(keys_len.try_into().unwrap()))
    }
}

// for debug
impl IdxFile {
    pub fn graph_string(&self) -> Result<String> {
        let top_node = self.read_top_node()?;
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.graph_string("", &top_node)
    }
    pub fn graph_string_with_key_string<KT>(&self, dbxxx: &FileDbXxxInner<KT>) -> Result<String>
    where
        KT: DbXxxKeyType + std::fmt::Display,
    {
        let top_node = self.read_top_node()?;
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.graph_string_with_key_string("", &top_node, dbxxx)
    }
    // check the index tree is balanced
    pub fn is_balanced(&self, node: &IdxNode) -> Result<bool> {
        let node_offset = node.get_ref().downs_get(0);
        let h = if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_balanced(&node1)? {
                return Ok(false);
            }
            self.height(&node1)?
        } else {
            0
        };
        for i in 1..node.get_ref().downs_len() {
            let node_offset = node.get_ref().downs_get(i);
            let hh = if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_balanced(&node1)? {
                    return Ok(false);
                }
                self.height(&node1)?
            } else {
                0
            };
            if h != hh {
                return Ok(false);
            }
        }
        Ok(true)
    }
    // return height of node tree
    fn height(&self, node: &IdxNode) -> Result<u32> {
        let node_offset = node.get_ref().downs_get(0);
        let mut mx = if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            self.height(&node1)?
        } else {
            0
        };
        for i in 1..node.get_ref().downs_len() {
            let node_offset = node.get_ref().downs_get(i);
            let h = if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                self.height(&node1)?
            } else {
                0
            };
            if h > mx {
                mx = h;
            }
        }
        Ok(1 + mx)
    }
    //
    pub fn is_mst_valid<KT>(&self, node: &IdxNode, dbxxx: &FileDbXxxInner<KT>) -> Result<bool>
    where
        KT: DbXxxKeyType + std::fmt::Display + std::default::Default + std::cmp::PartialOrd,
    {
        if node.get_ref().keys_is_empty() {
            return Ok(true);
        }
        let record_offset = node.get_ref().keys_get(0);
        let key_string = if !record_offset.is_zero() {
            dbxxx.load_key_string_no_cache(record_offset)?
        } else {
            Default::default()
        };
        let node_offset = node.get_ref().downs_get(0);
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_small(&key_string, &node1, dbxxx)? {
                return Ok(false);
            }
            if !self.is_mst_valid(&node1, dbxxx)? {
                return Ok(false);
            }
        }
        //
        for i in 1..node.get_ref().keys_len() {
            let key_offset1 = node.get_ref().keys_get(i - 1);
            let key_offset2 = node.get_ref().keys_get(i);
            let node_offset = node.get_ref().downs_get(i);
            let key_string1 = if !key_offset1.is_zero() {
                dbxxx.load_key_string_no_cache(key_offset1)?
            } else {
                Default::default()
            };
            let key_string2 = if !key_offset2.is_zero() {
                dbxxx.load_key_string_no_cache(key_offset2)?
            } else {
                Default::default()
            };
            if key_string1 >= key_string2 {
                return Ok(false);
            }
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_between(&key_string1, &key_string2, &node1, dbxxx)? {
                    return Ok(false);
                }
                if !self.is_mst_valid(&node1, dbxxx)? {
                    return Ok(false);
                }
            }
        }
        //
        let record_offset = node.get_ref().keys_get(node.get_ref().keys_len() - 1);
        let node_offset = node.get_ref().downs_get(node.get_ref().keys_len());
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !record_offset.is_zero() {
                let key_string = dbxxx.load_key_string_no_cache(record_offset)?;
                if !self.is_large(&key_string, &node1, dbxxx)? {
                    return Ok(false);
                }
            }
            if !self.is_mst_valid(&node1, dbxxx)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    //
    fn is_small<KT>(&self, key: &KT, node: &IdxNode, dbxxx: &FileDbXxxInner<KT>) -> Result<bool>
    where
        KT: DbXxxKeyType + std::fmt::Display + std::default::Default + std::cmp::PartialOrd,
    {
        for i in 0..node.get_ref().keys_len() {
            let node_offset = node.get_ref().downs_get(i);
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_small(key, &node1, dbxxx)? {
                    return Ok(false);
                }
            }
            let record_offset = node.get_ref().keys_get(i);
            if !record_offset.is_zero() {
                let key_string1 = dbxxx.load_key_string_no_cache(record_offset)?;
                if key <= &key_string1 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.get_ref().downs_get(node.get_ref().keys_len());
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_small(key, &node1, dbxxx)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    fn is_between<KT>(
        &self,
        key1: &KT,
        key2: &KT,
        node: &IdxNode,
        dbxxx: &FileDbXxxInner<KT>,
    ) -> Result<bool>
    where
        KT: DbXxxKeyType + std::fmt::Display + std::default::Default + std::cmp::PartialOrd,
    {
        for i in 0..node.get_ref().keys_len() {
            let node_offset = node.get_ref().downs_get(i);
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_between(key1, key2, &node1, dbxxx)? {
                    return Ok(false);
                }
            }
            let record_offset11 = node.get_ref().keys_get(i);
            if !record_offset11.is_zero() {
                let ket_string11 = dbxxx.load_key_string_no_cache(record_offset11)?;
                if key1 >= &ket_string11 {
                    return Ok(false);
                }
                if key2 <= &ket_string11 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.get_ref().downs_get(node.get_ref().keys_len());
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_between(key1, key2, &node1, dbxxx)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    fn is_large<KT>(&self, key: &KT, node: &IdxNode, dbxxx: &FileDbXxxInner<KT>) -> Result<bool>
    where
        KT: DbXxxKeyType + std::fmt::Display + std::default::Default + std::cmp::PartialOrd,
    {
        for i in 0..node.get_ref().keys_len() {
            let node_offset = node.get_ref().downs_get(i);
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_large(key, &node1, dbxxx)? {
                    return Ok(false);
                }
            }
            let record_offset = node.get_ref().keys_get(i);
            if !record_offset.is_zero() {
                let ket_string1 = dbxxx.load_key_string_no_cache(record_offset)?;
                if key >= &ket_string1 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.get_ref().downs_get(node.get_ref().keys_len());
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_large(key, &node1, dbxxx)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    //
    pub fn is_dense(&self, top_node: &IdxNode) -> Result<bool> {
        if top_node.get_ref().downs_is_empty() {
            return Ok(true);
        }
        let n = top_node.get_ref().downs_len();
        if n > NODE_SLOTS_MAX as usize {
            return Ok(false);
        }
        if n == 1 && !top_node.get_ref().downs_get(0).is_zero() {
            return Ok(false);
        }
        for i in 0..n {
            let node_offset = top_node.get_ref().downs_get(i);
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_dense_half(&node1)? {
                    return Ok(false);
                }
            }
        }
        //
        Ok(true)
    }
    fn is_dense_half(&self, node: &IdxNode) -> Result<bool> {
        let n = node.get_ref().downs_len();
        if n < NODE_SLOTS_MAX_HALF as usize || n > NODE_SLOTS_MAX as usize {
            return Ok(false);
        }
        for i in 0..n {
            let node_offset = node.get_ref().downs_get(i);
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_dense_half(&node1)? {
                    return Ok(false);
                }
            }
        }
        //
        Ok(true)
    }
    pub fn depth_of_node_tree(&self, node: &IdxNode) -> Result<u64> {
        let mut cnt = 1;
        if !node.get_ref().downs_is_empty() {
            let node_offset = node.get_ref().downs_get(0);
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                cnt += self.depth_of_node_tree(&node1)?;
            }
        }
        //
        Ok(cnt)
    }
    pub fn count_of_free_node(&self) -> Result<Vec<(u32, u64)>> {
        let sz_ary = NODE_SIZE_ARY;
        //
        let mut vec = Vec::new();
        let mut locked = RefCell::borrow_mut(&self.0);
        for node_size in sz_ary {
            let cnt = locked.0.count_of_free_node_list(NodeSize::new(node_size))?;
            vec.push((node_size, cnt));
        }
        Ok(vec)
    }
    pub fn count_of_used_node<F>(
        &self,
        read_record_size_func: F,
    ) -> Result<(CountOfPerSize, CountOfPerSize)>
    where
        F: Fn(RecordOffset) -> Result<RecordSize> + std::marker::Copy,
    {
        let mut node_vec = Vec::new();
        for node_size in NODE_SIZE_ARY {
            let cnt = 0;
            node_vec.push((node_size, cnt));
        }
        //
        let mut record_vec = Vec::new();
        for record_size in super::dat::REC_SIZE_ARY {
            let cnt = 0;
            record_vec.push((record_size, cnt));
        }
        //
        let top_node = self.read_top_node()?;
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.count_of_used_node(
            &top_node,
            &mut node_vec,
            &mut record_vec,
            read_record_size_func,
        )?;
        //
        Ok((record_vec, node_vec))
    }
    pub fn record_size_stats<F>(&self, read_record_size_func: F) -> Result<RecordSizeStats>
    where
        F: Fn(RecordOffset) -> Result<RecordSize> + std::marker::Copy,
    {
        let mut record_size_stats = RecordSizeStats::default();
        //
        let top_node = self.read_top_node()?;
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.idx_record_size_stats(&top_node, &mut record_size_stats, read_record_size_func)?;
        //
        Ok(record_size_stats)
    }
    pub fn keys_count_stats(&self) -> Result<KeysCountStats> {
        let mut keys_count_stats = KeysCountStats::default();
        //
        let top_node = self.read_top_node()?;
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.idx_keys_count_stats(&top_node, &mut keys_count_stats)?;
        //
        Ok(keys_count_stats)
    }
}

/**
write initiale header to file.

## header map

The db index header size is 128 bytes.

```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 4     | signature1  | [b's', b'h', b'a', b'm']  |
| 4      | 4     | signature1  | [b'd', b'b', b'1', 0u8]   |
| 8      | 8     | signature2  | 8 bytes type signature    |
| 16     | 8     | top node    | offset of top node        |
| 24     | 8     | free1 off   | offset of free 1st list   |
| 32     | 8     | free2 off   | offset of free 2ndlist    |
| 40     | 8     | free3 off   | offset of free 3rd list   |
| 48     | 8     | free4 off   | offset of free 4th list   |
| 56     | 8     | free5 off   | offset of free 5th list   |
| 64     | 8     | free6 off   | offset of free 6th list   |
| 72     | 8     | free7 off   | offset of free 7th list   |
| 80     | 8     | free8 off   | offset of free 8th list   |
| 88     | 40    | reserve1    |                           |
+--------+-------+-------------+---------------------------+
```

- signature1: always fixed 8 bytes
- signature2: 8 bytes type signature

*/

impl VarFile {
    fn write_idxf_init_header(&mut self, signature2: HeaderSignature) -> Result<()> {
        self.seek_from_start(NodeOffset::new(0))?;
        // signature1
        self.write_all(&IDX_HEADER_SIGNATURE)?;
        // signature2
        self.write_all(&signature2)?;
        // root offset
        self.write_u64_le(IDX_HEADER_SZ)?;
        // free1 .. rserve1
        self.write_all(&[0u8; 104])?;
        //
        Ok(())
    }
    fn check_idxf_header(&mut self, signature2: HeaderSignature) -> Result<()> {
        self.seek_from_start(NodeOffset::new(0))?;
        // signature1
        let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let _sz = self.read_exact(&mut sig1)?;
        assert!(!(sig1 != IDX_HEADER_SIGNATURE), "invalid header signature1");
        // signature2
        let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let _sz = self.read_exact(&mut sig2)?;
        assert!(
            !(sig2 != signature2),
            "invalid header signature2, type signature: {:?}",
            sig2
        );
        // top node offset
        let _top_node_offset = self.read_u64_le()?;
        assert!(!(_top_node_offset == 0), "invalid root offset");
        //
        Ok(())
    }
    fn read_top_node_offset(&mut self) -> Result<NodeOffset> {
        self.seek_from_start(NodeOffset::new(IDX_HEADER_TOP_NODE_OFFSET))?;
        self.read_u64_le().map(NodeOffset::new)
    }
    fn write_top_node_offset(&mut self, offset: NodeOffset) -> Result<()> {
        self.seek_from_start(NodeOffset::new(IDX_HEADER_TOP_NODE_OFFSET))?;
        self.write_u64_le(offset.into())?;
        Ok(())
    }
}

const NODE_SIZE_FREE_OFFSET_1ST: u64 = 24;

const NODE_SIZE_FREE_OFFSET: [u64; 8] = [
    NODE_SIZE_FREE_OFFSET_1ST,
    NODE_SIZE_FREE_OFFSET_1ST + 8,
    NODE_SIZE_FREE_OFFSET_1ST + 8 * 2,
    NODE_SIZE_FREE_OFFSET_1ST + 8 * 3,
    NODE_SIZE_FREE_OFFSET_1ST + 8 * 4,
    NODE_SIZE_FREE_OFFSET_1ST + 8 * 5,
    NODE_SIZE_FREE_OFFSET_1ST + 8 * 6,
    NODE_SIZE_FREE_OFFSET_1ST + 8 * 7,
];

impl NodeSize {
    fn free_node_list_offset_of_header(&self) -> u64 {
        let node_size = self.as_value();
        debug_assert!(node_size > 0, "node_size: {} > 0", node_size);
        match NODE_SIZE_ARY[..(NODE_SIZE_ARY.len() - 1)].binary_search(&node_size) {
            Ok(k) => {
                return NODE_SIZE_FREE_OFFSET[k];
            }
            Err(_k) => {}
        }
        debug_assert!(
            node_size > NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2],
            "node_size: {} > NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2]: {}",
            node_size,
            NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2]
        );
        NODE_SIZE_FREE_OFFSET[NODE_SIZE_FREE_OFFSET.len() - 1]
    }
    fn is_large_node_size(&self) -> bool {
        let node_size = self.as_value();
        node_size >= NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 1]
    }
    fn roundup(&self) -> NodeSize {
        let node_size = self.as_value();
        debug_assert!(node_size > 0, "node_size: {} > 0", node_size);
        match NODE_SIZE_ARY[..(NODE_SIZE_ARY.len() - 1)].binary_search(&node_size) {
            Ok(k) => {
                let n_sz = NODE_SIZE_ARY[k];
                return NodeSize::new(n_sz);
            }
            Err(k) => {
                if k < NODE_SIZE_ARY.len() - 1 {
                    let n_sz = NODE_SIZE_ARY[k];
                    return NodeSize::new(n_sz);
                }
            }
        }
        NodeSize::new(((node_size + 63) / 64) * 64)
    }
    fn can_down(&self, need: NodeSize) -> bool {
        let node_size = self.as_value();
        let need_size = need.as_value();
        debug_assert!(node_size > 0, "node_size: {} > 0", node_size);
        match NODE_SIZE_ARY[..(NODE_SIZE_ARY.len() - 1)].binary_search(&need_size) {
            Ok(k) => {
                let n_sz = NODE_SIZE_ARY[k];
                return n_sz < node_size;
            }
            Err(k) => {
                if k < NODE_SIZE_ARY.len() - 1 {
                    let n_sz = NODE_SIZE_ARY[k];
                    return n_sz < node_size;
                }
            }
        }
        false
    }
}

impl VarFile {
    fn read_free_node_offset_on_header(&mut self, node_size: NodeSize) -> Result<NodeOffset> {
        let free_offset = node_size.free_node_list_offset_of_header();
        self.seek_from_start(NodeOffset::new(free_offset))?;
        self.read_free_node_offset()
    }

    fn write_free_node_offset_on_header(
        &mut self,
        node_size: NodeSize,
        offset: NodeOffset,
    ) -> Result<()> {
        debug_assert!(offset.is_zero() || offset.as_value() >= IDX_HEADER_SZ);
        let free_offset = node_size.free_node_list_offset_of_header();
        self.seek_from_start(NodeOffset::new(free_offset))?;
        self.write_free_node_offset(offset)
    }

    fn count_of_free_node_list(&mut self, node_size: NodeSize) -> Result<u64> {
        let mut count = 0;
        let free_1st = self.read_free_node_offset_on_header(node_size)?;
        if !free_1st.is_zero() {
            let mut free_next_offset = free_1st;
            while !free_next_offset.is_zero() {
                count += 1;
                free_next_offset = {
                    self.seek_from_start(free_next_offset)?;
                    let _node_len = self.read_node_size()?;
                    let _keys_count = self.read_keys_count()?;
                    debug_assert!(_keys_count.is_zero());
                    let _node_offset = self.read_node_offset()?;
                    debug_assert!(_node_offset.is_zero());
                    self.read_free_node_offset()?
                };
            }
        }
        Ok(count)
    }

    fn pop_free_node_list(&mut self, new_node_size: NodeSize) -> Result<NodeOffset> {
        let free_1st = self.read_free_node_offset_on_header(new_node_size)?;
        if !new_node_size.is_large_node_size() {
            if !free_1st.is_zero() {
                let free_next = {
                    let (free_next, node_size) = {
                        self.seek_from_start(free_1st)?;
                        let node_size = self.read_node_size()?;
                        debug_assert!(!node_size.is_zero());
                        debug_assert!(node_size == new_node_size);
                        let _keys_count = self.read_keys_count()?;
                        debug_assert!(_keys_count.is_zero());
                        let _node_offset = self.read_node_offset()?;
                        debug_assert!(_node_offset.is_zero());
                        let node_offset = self.read_free_node_offset()?;
                        (node_offset, node_size)
                    };
                    //
                    self.write_node_clear(free_1st, node_size)?;
                    //
                    free_next
                };
                self.write_free_node_offset_on_header(new_node_size, free_next)?;
            }
            Ok(free_1st)
        } else {
            self.pop_free_node_list_large(new_node_size, free_1st)
        }
    }

    fn pop_free_node_list_large(
        &mut self,
        new_node_size: NodeSize,
        free_1st: NodeOffset,
    ) -> Result<NodeOffset> {
        let mut free_prev = NodeOffset::new(0);
        let mut free_curr = free_1st;
        while !free_curr.is_zero() {
            let (free_next, node_size) = {
                self.seek_from_start(free_curr)?;
                let node_size = self.read_node_size()?;
                debug_assert!(!node_size.is_zero());
                let _keys_count = self.read_keys_count()?;
                debug_assert!(_keys_count.is_zero());
                let _node_offset = self.read_node_offset()?;
                debug_assert!(_node_offset.is_zero());
                let node_offset = self.read_free_node_offset()?;
                (node_offset, node_size)
            };
            if new_node_size <= node_size {
                if !free_prev.is_zero() {
                    self.seek_from_start(free_prev)?;
                    let _node_size = self.read_node_size()?;
                    debug_assert!(!_node_size.is_zero());
                    let _keys_count = self.read_keys_count()?;
                    debug_assert!(_keys_count.is_zero());
                    let _node_offset = self.read_node_offset()?;
                    debug_assert!(_node_offset.is_zero());
                    self.write_free_node_offset(free_next)?;
                } else {
                    self.write_free_node_offset_on_header(new_node_size, free_next)?;
                }
                //
                self.write_node_clear(free_curr, node_size)?;
                return Ok(free_curr);
            }
            free_prev = free_curr;
            free_curr = free_next;
        }
        Ok(free_curr)
    }

    fn push_free_node_list(
        &mut self,
        old_node_offset: NodeOffset,
        old_node_size: NodeSize,
    ) -> Result<()> {
        if old_node_offset.is_zero() {
            return Ok(());
        }
        debug_assert!(!old_node_size.is_zero());
        //
        let free_1st = self.read_free_node_offset_on_header(old_node_size)?;
        {
            self.write_node_clear(old_node_offset, old_node_size)?;
            self.seek_from_start(old_node_offset)?;
            self.write_node_size(old_node_size)?;
            self.write_keys_count(KeysCount::new(0))?;
            self.write_node_offset(NodeOffset::new(0))?;
            self.write_free_node_offset(free_1st)?;
        }
        self.write_free_node_offset_on_header(old_node_size, old_node_offset)?;
        Ok(())
    }
}

#[cfg(feature = "small_node_slots")]
pub const NODE_SLOTS_MAX: u16 = 6;

#[cfg(feature = "small_node_slots")]
const NODE_SIZE_ARY: [u32; 8] = [
    16,
    16 * 2,
    16 * 2 * 2,
    16 * 2 * 3,
    16 * 2 * 4,
    16 * 2 * 5,
    16 * 2 * 6,
    16 * 2 * 7,
];

#[cfg(not(feature = "small_node_slots"))]
#[cfg(feature = "vf_u32u32")]
pub const NODE_SLOTS_MAX: u16 = 64;
//pub const NODE_SLOTS_MAX: u16 = 15;

#[cfg(not(feature = "small_node_slots"))]
#[cfg(feature = "vf_u64u64")]
pub const NODE_SLOTS_MAX: u16 = 7;

#[cfg(not(feature = "small_node_slots"))]
#[cfg(feature = "vf_vu64")]
/*
460.43user 71.00system 8:56.61elapsed 99%CPU (0avgtext+0avgdata 13316maxresident)k
3824inputs+2986832outputs (4major+3046minor)pagefaults 0swaps
414M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 4

pub const NODE_SLOTS_MAX: u16 = 256;
#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 40,
    16 * 48,
    16 * 56,
    16 * 64,
    16 * 72,
    16 * 80,
    16 * 88,
    16 * 96,
];
*/
/*
377.26user 67.40system 7:29.32elapsed 98%CPU (0avgtext+0avgdata 9784maxresident)k
4080inputs+2405296outputs (4major+15811minor)pagefaults 0swaps
395M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 4

pub const NODE_SLOTS_MAX: u16 = 128;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 16,
    16 * 24,
    16 * 32,
    16 * 40,
    16 * 48,
    16 * 56,
    16 * 64,
    16 * 72,
];
*/
/*
329.68user 69.59system 6:45.48elapsed 98%CPU (0avgtext+0avgdata 13124maxresident)k
3824inputs+2171664outputs (4major+41444minor)pagefaults 0swaps
383M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 5

pub const NODE_SLOTS_MAX: u16 = 64;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 4 * 2,
    16 * 4 * 4,
    16 * 4 * 6,
    16 * 4 * 8,
    16 * 4 * 10,
    16 * 4 * 12,
    16 * 4 * 14,
    16 * 4 * 16,
];
*/
/*
313.65user 76.71system 6:34.52elapsed 98%CPU (0avgtext+0avgdata 13072maxresident)k
0inputs+2014176outputs (0major+62696minor)pagefaults 0swaps
386M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 6

pub const NODE_SLOTS_MAX: u16 = 32;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 4 * 2,
    16 * 4 * 3,
    16 * 4 * 4,
    16 * 4 * 5,
    16 * 4 * 6,
    16 * 4 * 7,
    16 * 4 * 8,
    16 * 4 * 9,
];
*/
/*
-g
187.01user 1.49system 3:09.54elapsed 99%CPU (0avgtext+0avgdata 407752maxresident)k
0inputs+973656outputs (0major+101670minor)pagefaults 0swaps

-c
141.69user 0.91system 2:23.54elapsed 99%CPU (0avgtext+0avgdata 411168maxresident)k
0inputs+0outputs (0major+102580minor)pagefaults 0swaps

393M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 7

pub const NODE_SLOTS_MAX: u16 = 16;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 2,
    16 * 2 * 2,
    16 * 2 * 3,
    16 * 2 * 4,
    16 * 2 * 5,
    16 * 2 * 6,
    16 * 2 * 7,
    16 * 2 * 8,
];
*/
/*
-g
184.58user 1.35system 3:07.51elapsed 99%CPU (0avgtext+0avgdata 400792maxresident)k
0inputs+778992outputs (0major+99942minor)pagefaults 0swaps

-c
140.35user 0.79system 2:22.02elapsed 99%CPU (0avgtext+0avgdata 406108maxresident)k
0inputs+0outputs (0major+101292minor)pagefaults 0swaps

381M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 8
*/
pub const NODE_SLOTS_MAX: u16 = 12;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 2,
    16 * 3,
    16 * 4,
    16 * 5,
    16 * 6,
    16 * 7,
    16 * 8,
    16 * 9,
];

/*
-g
186.71user 1.41system 3:09.31elapsed 99%CPU (0avgtext+0avgdata 409280maxresident)k
0inputs+798416outputs (0major+102053minor)pagefaults 0swaps

-c
141.90user 0.91system 2:23.81elapsed 99%CPU (0avgtext+0avgdata 415180maxresident)k
0inputs+0outputs (0major+103538minor)pagefaults 0swaps

390M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 9

pub const NODE_SLOTS_MAX: u16 = 10;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 1 * 2,
    16 * 1 * 3,
    16 * 1 * 4,
    16 * 1 * 5,
    16 * 1 * 6,
    16 * 1 * 7,
    16 * 1 * 8,
    16 * 1 * 9,
];
*/
/*
-g
191.93user 1.41system 3:14.53elapsed 99%CPU (0avgtext+0avgdata 411704maxresident)k
0inputs+970704outputs (0major+102705minor)pagefaults 0swaps
-c
143.53user 0.80system 2:25.16elapsed 99%CPU (0avgtext+0avgdata 418428maxresident)k
0inputs+0outputs (0major+104379minor)pagefaults 0swaps

404M	./cmp_siamesedb/target/bench-db.siamesedb
db_map.depth_of_node_tree(): 10

pub const NODE_SLOTS_MAX: u16 = 8;

#[cfg(not(feature = "small_node_slots"))]
const NODE_SIZE_ARY: [u32; 8] = [
    16 * 1 * 2,
    16 * 1 * 3,
    16 * 1 * 4,
    16 * 1 * 5,
    16 * 1 * 6,
    16 * 1 * 7,
    16 * 1 * 8,
    16 * 1 * 9,
];
*/

pub const NODE_SLOTS_MAX_HALF: u16 = NODE_SLOTS_MAX / 2;

/*
 * node_size = keys_count.len + (2 * NODE_SLOTS_MAX - 1) * vu64.len
 * node_size = 1 + (2 *   8 -1) * 9 =  136 --> vu64 encoded len: 2
 * node_size = 1 + (2 *  16 -1) * 9 =  288 --> vu64 encoded len: 2
 * node_size = 1 + (2 *  32 -1) * 9 =  569 --> vu64 encoded len: 2
 * node_size = 1 + (2 *  64 -1) * 9 = 1144 --> vu64 encoded len: 2
 * node_size = 1 + (2 * 128 -1) * 9 = 2296 --> vu64 encoded len: 2
 * node_size = 2 + (2 * 256 -1) * 9 = 4601 --> vu64 encoded len: 2
 * node_size = 2 + (2 * 512 -1) * 9 = 9209 --> vu64 encoded len: 2
*/

impl VarFileNodeCache {
    #[cfg(feature = "node_cache")]
    #[inline]
    fn flush_node_cache(&mut self) -> Result<()> {
        self.1.flush(&mut self.0)?;
        Ok(())
    }

    #[cfg(feature = "node_cache")]
    #[inline]
    fn flush_node_cache_clear(&mut self) -> Result<()> {
        self.1.clear(&mut self.0)?;
        Ok(())
    }

    fn delete_node(&mut self, node_: IdxNode) -> Result<NodeSize> {
        let node_offset = node_.get_ref().offset();
        //
        #[cfg(not(feature = "node_cache"))]
        let old_node_size = {
            self.0.seek_from_start(node_offset)?;
            self.0.read_node_size()?
        };
        #[cfg(feature = "node_cache")]
        let old_node_size = {
            match self.1.delete(&node_offset) {
                Some(node_size) => node_size,
                None => {
                    self.0.seek_from_start(node_offset)?;
                    self.0.read_node_size()?
                }
            }
        };
        //
        self.0.push_free_node_list(node_offset, old_node_size)?;
        Ok(old_node_size)
    }

    fn write_node(&mut self, mut node_: IdxNode, is_new: bool) -> Result<IdxNode> {
        debug_assert!(!node_.get_ref().offset().is_zero());
        debug_assert!((node_.get_ref().offset().as_value() & 0x0F) == 0);
        //
        let new_node_size = {
            let buf_len: u32 = node_.get_ref().encoded_node_size().try_into().unwrap();
            //
            #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
            let encoded_len = 4;
            #[cfg(feature = "vf_vu64")]
            let encoded_len = if buf_len < 128 { 1 } else { 2 };
            //let encoded_len = vu64::encoded_len(buf_len as u64);
            //
            // buggy: size operation for node size.
            NodeSize::new(buf_len + encoded_len).roundup()
        };
        //
        if !is_new {
            #[cfg(not(feature = "node_cache"))]
            let old_node_size = {
                self.0.seek_from_start(node_.get_ref().offset())?;
                self.0.read_node_size()?
            };
            #[cfg(feature = "node_cache")]
            let old_node_size = {
                if let Some(node_size) = self.1.get_node_size(&node_.get_ref().offset()) {
                    node_size
                } else {
                    self.0.seek_from_start(node_.get_ref().offset())?;
                    self.0.read_node_size()?
                }
            };
            if new_node_size <= old_node_size && !old_node_size.can_down(new_node_size) {
                // over writes.
                #[cfg(not(feature = "node_cache"))]
                {
                    node_.get_mut().set_size(old_node_size);
                    node_.idx_write_node_one(&mut self.0)?;
                    return Ok(node_);
                }
                #[cfg(feature = "node_cache")]
                {
                    let node_ = self.1.put(&mut self.0, node_, old_node_size, true)?;
                    return Ok(node_);
                }
            } else {
                // delete old and add new
                #[cfg(feature = "node_cache")]
                self.1.delete(&node_.get_ref().offset());
                // old
                self.0
                    .push_free_node_list(node_.get_ref().offset(), old_node_size)?;
            }
        }
        // add new.
        {
            let free_node_offset = self.0.pop_free_node_list(new_node_size)?;
            let (new_node_offset, new_node_size) = if !free_node_offset.is_zero() {
                self.0.seek_from_start(free_node_offset)?;
                let node_size = self.0.read_node_size()?;
                debug_assert!(
                    (new_node_size.as_value() > NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 1]
                        && node_size >= new_node_size)
                        || node_size == new_node_size,
                    "node_size: {} == new_node_size: {}",
                    node_size.as_value(),
                    new_node_size.as_value()
                );
                //self.0.write_node_clear(free_node_offset, node_size)?;
                (free_node_offset, node_size)
            } else {
                let node_offset: NodeOffset = self.0.seek_to_end()?;
                //self.0.write_node_clear(node_offset, new_node_size)?;
                (node_offset, new_node_size)
            };
            debug_assert!(!new_node_offset.is_zero());
            debug_assert!((new_node_offset.as_value() & 0x0F) == 0);
            {
                let mut node = node_.get_mut();
                node.set_offset(new_node_offset);
                node.set_size(new_node_size);
                node.idx_write_node_one(&mut self.0)?;
            }
        }
        //
        Ok(node_)
    }

    #[cfg(not(feature = "node_cache"))]
    fn read_node(&mut self, offset: NodeOffset) -> Result<IdxNode> {
        debug_assert!(!offset.is_zero());
        debug_assert!((offset.as_value() & 0x0F) == 0);
        //
        let _start_pos = self.0.seek_from_start(offset)?;
        let node_size = self.0.read_node_size()?;
        debug_assert!(
            !node_size.is_zero(),
            "!node_size.is_zero(), offset: {}",
            offset
        );
        let keys_count = self.0.read_keys_count()?;
        debug_assert!(
            keys_count.as_value() < NODE_SLOTS_MAX,
            "keys_count: {} < NODE_SLOTS_MAX",
            keys_count
        );
        let keys_count: usize = keys_count.try_into().unwrap();
        //
        let mut node_ = IdxNode::with_node_size(offset, node_size);
        {
            let mut node = node_.get_mut();
            for _i in 0..keys_count {
                let record_offset = self
                    .0
                    .read_record_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                debug_assert!(!record_offset.is_zero());
                node.keys_push(record_offset);
            }
            for _i in 0..(keys_count + 1) {
                let node_offset = self
                    .0
                    .read_node_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                debug_assert!(
                    (node_offset.as_value() & 0x0F) == 0,
                    "(node_offset.as_value(): {} & 0x0F) == 0, offset: {}",
                    node_offset,
                    offset.as_value()
                );
                node.downs_push(node_offset);
            }
        }
        debug_assert!(_start_pos + node_size >= self.0.seek_position()?);
        //
        Ok(node_)
    }

    #[cfg(feature = "node_cache")]
    fn read_node(&mut self, offset: NodeOffset) -> Result<IdxNode> {
        debug_assert!(!offset.is_zero());
        debug_assert!((offset.as_value() & 0x0F) == 0);
        //
        if let Some(cached_node) = self.1.get(&offset) {
            return Ok(cached_node);
        }
        //
        self.0.seek_from_start(offset)?;
        let node_size = self.0.read_node_size()?;
        debug_assert!(
            !node_size.is_zero(),
            "!node_size.is_zero(), offset: {}",
            offset
        );
        let keys_count = self.0.read_keys_count()?;
        debug_assert!(
            keys_count.as_value() < NODE_SLOTS_MAX,
            "keys_count: {} < NODE_SLOTS_MAX",
            keys_count
        );
        let keys_count: usize = keys_count.into();
        //
        let mut node_ = IdxNode::with_node_size(offset, node_size);
        {
            let mut node = node_.get_mut();
            for _i in 0..keys_count {
                let record_offset = self
                    .0
                    .read_record_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                debug_assert!(!record_offset.is_zero());
                node.keys_push(record_offset);
            }
            for _i in 0..(keys_count + 1) {
                let node_offset = self
                    .0
                    .read_node_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                debug_assert!((node_offset.as_value() & 0x0F) == 0);
                node.downs_push(node_offset);
            }
        }
        //
        let node_ = self.1.put(&mut self.0, node_, node_size, false)?;
        //
        Ok(node_)
    }
}

//const GRAPH_NODE_ST: &str = "∧";
//const GRAPH_NODE_ED: &str = "∨";
const GRAPH_NODE_ST: &str = "^";
const GRAPH_NODE_ED: &str = "v";
//const GRAPH_NODE_ST: &str = "{";
//const GRAPH_NODE_ED: &str = "}";

// for debug
impl VarFileNodeCache {
    fn graph_string(&mut self, head: &str, node_: &IdxNode) -> Result<String> {
        let node = node_.get_ref();
        let mut gs = format!(
            "{}{}:{:04x}\n",
            head,
            GRAPH_NODE_ST,
            node.offset().as_value()
        );
        let mut i = node.downs_len() - 1;
        let node_offset = node.downs_get(i);
        if !node_offset.is_zero() {
            let node = self
                .read_node(node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            let gs0 = self.graph_string(&format!("{}    ", head), &node)?;
            gs += &gs0;
        }
        while i > 0 {
            i -= 1;
            let record_offset = node.keys_get(i);
            gs += &format!("{}{:04x}\n", head, record_offset.as_value());
            let node_offset = node.downs_get(i);
            if !node_offset.is_zero() {
                let node = self
                    .read_node(node_offset)
                    .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
                let gs0 = self.graph_string(&format!("{}    ", head), &node)?;
                gs += &gs0;
            }
        }
        gs += &format!("{}{}\n", head, GRAPH_NODE_ED);
        //
        Ok(gs)
    }

    fn graph_string_with_key_string<KT>(
        &mut self,
        head: &str,
        node_: &IdxNode,
        dbxxx: &FileDbXxxInner<KT>,
    ) -> Result<String>
    where
        KT: DbXxxKeyType + std::fmt::Display,
    {
        let node = node_.get_ref();
        let mut gs = format!(
            "{}{}:0x{:04x},{03}\n",
            head,
            GRAPH_NODE_ST,
            node.offset().as_value(),
            node.size()
        );
        let mut i = node.downs_len() - 1;
        let node_offset = node.downs_get(i);
        if !node_offset.is_zero() {
            let node = self
                .read_node(node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            let gs0 = self.graph_string_with_key_string(&format!("{}    ", head), &node, dbxxx)?;
            gs += &gs0;
        }
        while i > 0 {
            i -= 1;
            let record_offset = node.keys_get(i);
            if !record_offset.is_zero() {
                let key_string = dbxxx.load_key_string_no_cache(record_offset)?;
                gs += &format!(
                    "{}{:04x}:'{}'\n",
                    head,
                    record_offset.as_value(),
                    key_string
                );
            }
            let node_offset = node.downs_get(i);
            if !node_offset.is_zero() {
                let node = self
                    .read_node(node_offset)
                    .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
                let gs0 =
                    self.graph_string_with_key_string(&format!("{}    ", head), &node, dbxxx)?;
                gs += &gs0;
            }
        }
        gs += &format!("{}{}\n", head, GRAPH_NODE_ED);
        //
        Ok(gs)
    }

    fn count_of_used_node<F>(
        &mut self,
        node_: &IdxNode,
        node_vec: &mut Vec<(u32, u64)>,
        record_vec: &mut Vec<(u32, u64)>,
        read_record_size_func: F,
    ) -> Result<()>
    where
        F: Fn(RecordOffset) -> Result<RecordSize> + Copy,
    {
        let node = node_.get_ref();
        match node_vec.iter().position(|v| v.0 == node.size().as_value()) {
            Some(sz_idx) => {
                node_vec[sz_idx].1 += 1;
            }
            None => {
                let last = node_vec.len() - 1;
                node_vec[last].1 += 1;
            }
        }
        //
        let mut i = node.downs_len() - 1;
        let node_offset = node.downs_get(i);
        if !node_offset.is_zero() {
            let node = self
                .read_node(node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            self.count_of_used_node(&node, node_vec, record_vec, read_record_size_func)?;
        }
        while i > 0 {
            i -= 1;
            //
            let record_offset = node.keys_get(i);
            if !record_offset.is_zero() {
                let record_size = read_record_size_func(record_offset)?;
                match record_vec
                    .iter()
                    .position(|v| v.0 == record_size.as_value())
                {
                    Some(sz_idx) => {
                        record_vec[sz_idx].1 += 1;
                    }
                    None => {
                        let last = record_vec.len() - 1;
                        record_vec[last].1 += 1;
                    }
                }
            }
            //
            let node_offset = node.downs_get(i);
            if !node_offset.is_zero() {
                let node = self
                    .read_node(node_offset)
                    .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
                self.count_of_used_node(&node, node_vec, record_vec, read_record_size_func)?;
            }
        }
        //
        Ok(())
    }

    fn idx_record_size_stats<F>(
        &mut self,
        node_: &IdxNode,
        record_vec: &mut RecordSizeStats,
        read_record_size_func: F,
    ) -> Result<()>
    where
        F: Fn(RecordOffset) -> Result<RecordSize> + Copy,
    {
        let node = node_.get_ref();
        let mut i = node.downs_len() - 1;
        let node_offset = node.downs_get(i);
        if !node_offset.is_zero() {
            let node = self
                .read_node(node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            self.idx_record_size_stats(&node, record_vec, read_record_size_func)?;
        }
        while i > 0 {
            i -= 1;
            //
            let record_offset = node.keys_get(i);
            if !record_offset.is_zero() {
                let record_size = read_record_size_func(record_offset)?;
                record_vec.touch_size(record_size);
            }
            //
            let node_offset = node.downs_get(i);
            if !node_offset.is_zero() {
                let node = self
                    .read_node(node_offset)
                    .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
                self.idx_record_size_stats(&node, record_vec, read_record_size_func)?;
            }
        }
        //
        Ok(())
    }

    fn idx_keys_count_stats(
        &mut self,
        node_: &IdxNode,
        keys_vec: &mut KeysCountStats,
    ) -> Result<()> {
        let node = node_.get_ref();
        let mut i = node.downs_len() - 1;
        let node_offset = node.downs_get(i);
        if !node_offset.is_zero() {
            let node = self
                .read_node(node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            self.idx_keys_count_stats(&node, keys_vec)?;
        }
        while i > 0 {
            i -= 1;
            //
            let node_offset = node.downs_get(i);
            if !node_offset.is_zero() {
                let node = self
                    .read_node(node_offset)
                    .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
                let keys_count = node.get_ref().keys_len();
                keys_vec.touch_size(KeysCount::new(keys_count.try_into().unwrap()));
                self.idx_keys_count_stats(&node, keys_vec)?;
            }
        }
        //
        Ok(())
    }
}

//
// ref) http://wwwa.pikara.ne.jp/okojisan/b-tree/bsb-tree.html
//

/*
```text
used node:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | node size   | size in bytes of this node: vu32  |
| 1      | 1     | key-count   | count of keys                     |
| 2      | 1..9  | key1        | offset of key-value               |
|        |       | ...         |                                   |
|        |       | key4        |                                   |
| --     | 1..9  | down1       | offset of next node               |
|        |       | ...         |                                   |
|        |       | down5       |                                   |
+--------+-------+-------------+-----------------------------------+
```
*/
/*
```text
free node:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | node size   | size in bytes of this node: u32   |
| --     | 1     | keys-count  | always zero                       |
| --     | 8     | next        | next free record offset           |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
