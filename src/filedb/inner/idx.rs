use super::super::{CountOfPerSize, FileDbParams};
use super::dat;
use super::semtype::*;
use super::vfile::VarFile;
use std::cell::RefCell;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

use super::super::RecordSizeStats;

type HeaderSignature = [u8; 8];

const IDX_HEADER_SZ: u64 = 128;
const IDX_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'1', 0u8];
const IDX_HEADER_TOP_NODE_OFFSET: u64 = 16;

#[cfg(not(feature = "node_cache"))]
use std::marker::PhantomData;

#[cfg(feature = "node_cache")]
use super::nc::NodeCache;

#[cfg(not(feature = "node_cache"))]
type VarFileNodeCache = (VarFile, PhantomData<i32>);
#[cfg(feature = "node_cache")]
type VarFileNodeCache = (VarFile, NodeCache);

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
        let mut file = VarFile::with_capacity(
            std_file,
            params.idx_buf_num_chunks,
            params.idx_buf_chunk_size,
        )?;
        let _ = file.seek(SeekFrom::End(0))?;
        let len = file.stream_position()?;
        //
        #[cfg(not(feature = "node_cache"))]
        let mut file_nc = (file, PhantomData);
        #[cfg(feature = "node_cache")]
        let mut file_nc = (file, NodeCache::new());
        //
        if len == 0 {
            idx_file_write_init_header(&mut file_nc.0, sig2)?;
            // writing top node
            let top_node = IdxNode::new(NodeOffset::new(IDX_HEADER_SZ));
            let _new_node = idx_write_node(&mut file_nc, top_node, true)?;
            debug_assert!(_new_node.offset == NodeOffset::new(IDX_HEADER_SZ));
        } else {
            idx_file_check_header(&mut file_nc.0, sig2)?;
        }
        //
        Ok(Self(Rc::new(RefCell::new(file_nc))))
    }
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        #[cfg(feature = "node_cache")]
        idx_cache_flush_clear(&mut locked)?;
        locked.0.sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        #[cfg(feature = "node_cache")]
        idx_cache_flush_clear(&mut locked)?;
        locked.0.sync_data()
    }
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        let locked = self.0.borrow();
        locked.0.buf_stats()
    }
    //
    pub fn read_top_node(&self) -> Result<IdxNode> {
        let offset = {
            let mut locked = self.0.borrow_mut();
            idx_file_read_top_node_offset(&mut locked.0)?
        };
        self.read_node(offset)
    }
    pub fn write_top_node(&self, node: IdxNode) -> Result<IdxNode> {
        if node.offset.is_zero() {
            let new_top_node = self.write_new_node(node)?;
            {
                let mut locked = self.0.borrow_mut();
                idx_file_write_top_node_offset(&mut locked.0, new_top_node.offset)?;
            }
            Ok(new_top_node)
        } else {
            let top_node_offset = {
                let mut locked = self.0.borrow_mut();
                idx_file_read_top_node_offset(&mut locked.0)?
            };
            let new_top_node = self.write_node(node)?;
            if new_top_node.offset != top_node_offset {
                let mut locked = self.0.borrow_mut();
                idx_file_write_top_node_offset(&mut locked.0, new_top_node.offset)?;
            }
            Ok(new_top_node)
        }
    }
    //
    pub fn read_node(&self, offset: NodeOffset) -> Result<IdxNode> {
        let mut locked = self.0.borrow_mut();
        idx_read_node(&mut locked, offset)
    }
    pub fn write_node(&self, node: IdxNode) -> Result<IdxNode> {
        let mut locked = self.0.borrow_mut();
        idx_write_node(&mut locked, node, false)
    }
    pub fn write_new_node(&self, mut node: IdxNode) -> Result<IdxNode> {
        node.offset = {
            let mut locked = self.0.borrow_mut();
            let _ = locked.0.seek(SeekFrom::End(0));
            NodeOffset::new(locked.0.stream_position()?)
        };
        let mut locked = self.0.borrow_mut();
        idx_write_node(&mut locked, node, true)
    }
    pub fn delete_node(&self, node: IdxNode) -> Result<NodeSize> {
        let mut locked = self.0.borrow_mut();
        idx_delete_node(&mut locked, node)
    }
}

// for debug
impl IdxFile {
    pub fn to_graph_string(&self) -> Result<String> {
        let top_node = self.read_top_node()?;
        let mut locked = self.0.borrow_mut();
        idx_to_graph_string(&mut locked, "", &top_node)
    }
    pub fn to_graph_string_with_key_string<KT>(&self, dbxxx: &FileDbXxxInner<KT>) -> Result<String>
    where
        KT: FileDbXxxInnerKT + std::fmt::Display,
    {
        let top_node = self.read_top_node()?;
        let mut locked = self.0.borrow_mut();
        idx_to_graph_string_with_key_string(&mut locked, "", &top_node, dbxxx)
    }
    // check the index tree is balanced
    pub fn is_balanced(&self, node: &IdxNode) -> Result<bool> {
        let node_offset = node.downs[0];
        let h = if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_balanced(&node1)? {
                return Ok(false);
            }
            self.height(&node1)?
        } else {
            0
        };
        for i in 1..node.downs.len() {
            let node_offset = node.downs[i];
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
        let node_offset = node.downs[0];
        let mut mx = if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            self.height(&node1)?
        } else {
            0
        };
        for i in 1..node.downs.len() {
            let node_offset = node.downs[i];
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
    pub fn is_mst_valid(&self, node: &IdxNode, dat_file: dat::DatFile) -> Result<bool> {
        if node.keys.is_empty() {
            return Ok(true);
        }
        let record_offset = node.keys[0];
        let key_string = if !record_offset.is_zero() {
            String::from_utf8_lossy(&dat_file.read_record_key(record_offset)?).to_string()
        } else {
            String::new()
        };
        let node_offset = node.downs[0];
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_small(&key_string, &node1, dat_file.clone())? {
                return Ok(false);
            }
            if !self.is_mst_valid(&node1, dat_file.clone())? {
                return Ok(false);
            }
        }
        //
        for i in 1..node.keys.len() {
            let key_offset1 = node.keys[i - 1];
            let key_offset2 = node.keys[i];
            let node_offset = node.downs[i];
            if key_offset2.is_zero() {
                break;
            }
            let key_string1 = if !key_offset1.is_zero() {
                String::from_utf8_lossy(&dat_file.read_record_key(key_offset1)?).to_string()
            } else {
                String::new()
            };
            let key_string2 = if !key_offset2.is_zero() {
                String::from_utf8_lossy(&dat_file.read_record_key(key_offset2)?).to_string()
            } else {
                String::new()
            };
            if key_string1 >= key_string2 {
                return Ok(false);
            }
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_between(&key_string1, &key_string2, &node1, dat_file.clone())? {
                    return Ok(false);
                }
                if !self.is_mst_valid(&node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
        }
        //
        let record_offset = node.keys[node.keys.len() - 1];
        let node_offset = node.downs[node.keys.len()];
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !record_offset.is_zero() {
                let key_string =
                    String::from_utf8_lossy(&dat_file.read_record_key(record_offset)?).to_string();
                if !self.is_large(&key_string, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            if !self.is_mst_valid(&node1, dat_file)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    //
    fn is_small(&self, key: &str, node: &IdxNode, dat_file: dat::DatFile) -> Result<bool> {
        for i in 0..node.keys.len() {
            let node_offset = node.downs[i];
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_small(key, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            let record_offset = node.keys[i];
            if !record_offset.is_zero() {
                let ket_string1 =
                    String::from_utf8_lossy(&dat_file.read_record_key(record_offset)?).to_string();
                if key <= &ket_string1 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.downs[node.keys.len()];
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_small(key, &node1, dat_file)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    fn is_between(
        &self,
        key1: &str,
        key2: &str,
        node: &IdxNode,
        dat_file: dat::DatFile,
    ) -> Result<bool> {
        for i in 0..node.keys.len() {
            let node_offset = node.downs[i];
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_between(key1, key2, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            let record_offset11 = node.keys[i];
            if !record_offset11.is_zero() {
                let ket_string11 =
                    String::from_utf8_lossy(&dat_file.read_record_key(record_offset11)?)
                        .to_string();
                if key1 >= &ket_string11 {
                    return Ok(false);
                }
                if key2 <= &ket_string11 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.downs[node.keys.len()];
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_between(key1, key2, &node1, dat_file)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    fn is_large(&self, key: &str, node: &IdxNode, dat_file: dat::DatFile) -> Result<bool> {
        for i in 0..node.keys.len() {
            let node_offset = node.downs[i];
            if !node_offset.is_zero() {
                let node1 = self.read_node(node_offset)?;
                if !self.is_large(key, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            let record_offset = node.keys[i];
            if !record_offset.is_zero() {
                let ket_string1 =
                    String::from_utf8_lossy(&dat_file.read_record_key(record_offset)?).to_string();
                if key >= &ket_string1 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.downs[node.keys.len()];
        if !node_offset.is_zero() {
            let node1 = self.read_node(node_offset)?;
            if !self.is_large(key, &node1, dat_file)? {
                return Ok(false);
            }
        }
        //
        Ok(true)
    }
    //
    pub fn is_dense(&self, top_node: &IdxNode) -> Result<bool> {
        if top_node.downs.is_empty() {
            return Ok(true);
        }
        let n = top_node.downs.len();
        if n > NODE_SLOTS_MAX as usize {
            return Ok(false);
        }
        if n == 1 && !top_node.downs[0].is_zero() {
            return Ok(false);
        }
        for i in 0..n {
            let node_offset = top_node.downs[i];
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
        let n = node.downs.len();
        if n < NODE_SLOTS_MAX_HALF as usize || n > NODE_SLOTS_MAX as usize {
            return Ok(false);
        }
        for i in 0..n {
            let node_offset = node.downs[i];
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
        if !node.downs.is_empty() {
            let node_offset = node.downs[0];
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
        let mut locked = self.0.borrow_mut();
        for node_size in sz_ary {
            let cnt = idx_file_count_of_free_list(&mut locked.0, NodeSize::new(node_size))?;
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
        let mut locked = self.0.borrow_mut();
        idx_count_of_used_node(
            &mut locked,
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
        let mut locked = self.0.borrow_mut();
        idx_record_size_stats(
            &mut locked,
            &top_node,
            &mut record_size_stats,
            read_record_size_func,
        )?;
        //
        Ok(record_size_stats)
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
fn idx_file_write_init_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    // signature1
    let _ = file.write(&IDX_HEADER_SIGNATURE)?;
    // signature2
    let _ = file.write(&signature2)?;
    // root offset
    file.write_u64_le(IDX_HEADER_SZ)?;
    // free1 .. rserve1
    let _ = file.write(&[0u8; 104]);
    //
    Ok(())
}

fn idx_file_check_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    // signature1
    let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig1)?;
    assert!(!(sig1 != IDX_HEADER_SIGNATURE), "invalid header signature1");
    // signature2
    let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig2)?;
    assert!(
        !(sig2 != signature2),
        "invalid header signature2, type signature: {:?}",
        sig2
    );
    // top node offset
    let _top_node_offset = file.read_u64_le()?;
    assert!(!(_top_node_offset == 0), "invalid root offset");
    //
    Ok(())
}

fn idx_file_read_top_node_offset(file: &mut VarFile) -> Result<NodeOffset> {
    let _ = file.seek(SeekFrom::Start(IDX_HEADER_TOP_NODE_OFFSET))?;
    file.read_u64_le().map(NodeOffset::new)
}

fn idx_file_write_top_node_offset(file: &mut VarFile, offset: NodeOffset) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(IDX_HEADER_TOP_NODE_OFFSET))?;
    file.write_u64_le(offset.as_value())?;
    Ok(())
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

const NODE_SIZE_ARY: [u32; 8] = [8 * 4, 8 * 9, 8 * 13, 8 * 18, 8 * 22, 8 * 27, 8 * 29, 8 * 32];

fn free_node_list_offset_of_header(node_size: NodeSize) -> u64 {
    let node_size = node_size.as_value();
    debug_assert!(node_size > 0, "node_size: {} > 0", node_size);
    for i in 0..NODE_SIZE_ARY.len() {
        if NODE_SIZE_ARY[i] == node_size {
            return NODE_SIZE_FREE_OFFSET[i];
        }
    }
    debug_assert!(
        node_size > NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2],
        "node_size: {} > NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2]: {}",
        node_size,
        NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2]
    );
    NODE_SIZE_FREE_OFFSET[NODE_SIZE_FREE_OFFSET.len() - 1]
}

fn is_large_node_size(node_size: NodeSize) -> bool {
    let node_size = node_size.as_value();
    node_size >= NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 1]
}

fn node_size_roudup(node_size: NodeSize) -> NodeSize {
    let node_size = node_size.as_value();
    debug_assert!(node_size > 0, "node_size: {} > 0", node_size);
    for &n_sz in NODE_SIZE_ARY.iter().take(NODE_SIZE_ARY.len() - 1) {
        if node_size <= n_sz {
            return NodeSize::new(n_sz);
        }
    }
    NodeSize::new(((node_size + 63) / 64) * 64)
}

fn idx_file_read_free_node_offset(file: &mut VarFile, node_size: NodeSize) -> Result<NodeOffset> {
    let _ = file.seek(SeekFrom::Start(free_node_list_offset_of_header(node_size)))?;
    file.read_u64_le().map(NodeOffset::new)
}

fn idx_file_write_free_node_offset(
    file: &mut VarFile,
    node_size: NodeSize,
    offset: NodeOffset,
) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(free_node_list_offset_of_header(node_size)))?;
    file.write_u64_le(offset.as_value())
}

fn idx_file_count_of_free_list(file: &mut VarFile, node_size: NodeSize) -> Result<u64> {
    let mut count = 0;
    let free_1st = idx_file_read_free_node_offset(file, node_size)?;
    if !free_1st.is_zero() {
        let mut free_next_offset = free_1st;
        while !free_next_offset.is_zero() {
            count += 1;
            free_next_offset = {
                let _a = file.seek_from_start(free_next_offset)?;
                debug_assert!(_a == free_next_offset);
                let _node_len = file.read_node_size()?;
                let _keys_count = file.read_keys_count()?;
                debug_assert!(_keys_count.is_zero());
                file.read_free_node_offset()?
            };
        }
    }
    Ok(count)
}

fn idx_file_pop_free_list(file: &mut VarFile, new_node_size: NodeSize) -> Result<NodeOffset> {
    let free_1st = idx_file_read_free_node_offset(file, new_node_size)?;
    if !is_large_node_size(new_node_size) {
        if !free_1st.is_zero() {
            let free_next = {
                let (free_next, node_size) = {
                    let _ = file.seek_from_start(free_1st)?;
                    let node_size = file.read_node_size()?;
                    debug_assert!(!node_size.is_zero());
                    let _keys_count = file.read_keys_count()?;
                    debug_assert!(_keys_count.is_zero());
                    let node_offset = file.read_free_node_offset()?;
                    (node_offset, node_size)
                };
                //
                file.write_node_clear(free_1st, node_size)?;
                //
                free_next
            };
            idx_file_write_free_node_offset(file, new_node_size, free_next)?;
        }
        Ok(free_1st)
    } else {
        idx_file_pop_free_list_large(file, new_node_size, free_1st)
    }
}

fn idx_file_pop_free_list_large(
    file: &mut VarFile,
    new_node_size: NodeSize,
    free_1st: NodeOffset,
) -> Result<NodeOffset> {
    let mut free_prev = NodeOffset::new(0);
    let mut free_curr = free_1st;
    while !free_curr.is_zero() {
        let (free_next, node_size) = {
            let _ = file.seek_from_start(free_curr)?;
            let node_size = file.read_node_size()?;
            debug_assert!(!node_size.is_zero());
            let _keys_count = file.read_keys_count()?;
            debug_assert!(_keys_count.is_zero());
            let node_offset = file.read_free_node_offset()?;
            (node_offset, node_size)
        };
        if new_node_size <= node_size {
            if !free_prev.is_zero() {
                let _ = file.seek_from_start(free_prev)?;
                let _node_size = file.read_node_size()?;
                debug_assert!(!_node_size.is_zero());
                let _keys_count = file.read_keys_count()?;
                debug_assert!(_keys_count.is_zero());
                file.write_free_node_offset(free_next)?;
            } else {
                idx_file_write_free_node_offset(file, new_node_size, free_next)?;
            }
            //
            file.write_node_clear(free_curr, node_size)?;
            return Ok(free_curr);
        }
        free_prev = free_curr;
        free_curr = free_next;
    }
    Ok(free_curr)
}

fn idx_file_push_free_list(
    file: &mut VarFile,
    old_node_offset: NodeOffset,
    old_node_size: NodeSize,
) -> Result<()> {
    if old_node_offset.is_zero() {
        return Ok(());
    }
    debug_assert!(!old_node_size.is_zero());
    //
    let free_1st = idx_file_read_free_node_offset(file, old_node_size)?;
    {
        let _a = file.seek_from_start(old_node_offset)?;
        debug_assert!(_a == old_node_offset);
        file.write_node_size(old_node_size)?;
        file.write_keys_count(KeysCount::new(0))?;
        file.write_free_node_offset(free_1st)?;
    }
    idx_file_write_free_node_offset(file, old_node_size, old_node_offset)?;
    Ok(())
}

#[cfg(feature = "vf_u32u32")]
pub const NODE_SLOTS_MAX: u16 = 15;
#[cfg(feature = "vf_u64u64")]
pub const NODE_SLOTS_MAX: u16 = 7;
#[cfg(feature = "vf_vu64")]
pub const NODE_SLOTS_MAX: u16 = 13;
pub const NODE_SLOTS_MAX_HALF: u16 = (NODE_SLOTS_MAX + 1) / 2;

#[derive(Debug, Default, Clone)]
pub struct IdxNode {
    /// active node flag is used insert operation. this not store into file.
    pub is_active: bool,
    /// offset of IdxNode in idx file.
    pub offset: NodeOffset,
    /// size in bytes of IdxNode in idx file.
    pub size: NodeSize,
    /// key slot: offset of key-value record in dat file.
    pub keys: Vec<RecordOffset>,
    //pub keys: [u64; (NODE_SLOTS_MAX as usize) - 1],
    /// down slot: offset of next IdxNode in idx file.
    pub downs: Vec<NodeOffset>,
    //pub downs: [u64; (NODE_SLOTS_MAX as usize)],
}

impl IdxNode {
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
    pub fn is_over_len(&self) -> bool {
        if self.keys.len() < NODE_SLOTS_MAX as usize && self.downs.len() <= NODE_SLOTS_MAX as usize
        {
            return false;
        }
        true
    }
    /// convert active node to normal node
    pub fn deactivate(&self) -> IdxNode {
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
}

#[cfg(feature = "node_cache")]
fn idx_cache_flush_clear(file_nc: &mut VarFileNodeCache) -> Result<()> {
    file_nc.1.clear(&mut file_nc.0)?;
    Ok(())
}

fn idx_delete_node(file_nc: &mut VarFileNodeCache, node: IdxNode) -> Result<NodeSize> {
    #[cfg(not(feature = "node_cache"))]
    let old_node_size = {
        let _ = file_nc.0.seek_from_start(node.offset)?;
        file_nc.0.read_node_size()?
    };
    #[cfg(feature = "node_cache")]
    let old_node_size = {
        match file_nc.1.delete(&node.offset) {
            Some(node_size) => node_size,
            None => {
                let _ = file_nc.0.seek_from_start(node.offset)?;
                file_nc.0.read_node_size()?
            }
        }
    };
    //
    idx_file_push_free_list(&mut file_nc.0, node.offset, old_node_size)?;
    Ok(old_node_size)
}

/*
fn idx_serialize_to_buf(node: &IdxNode) -> Result<Vec<u8>> {
    let mut buff_cursor = VarCursor::with_capacity(9 * (2 * NODE_SLOTS_MAX as usize - 1));
    //
    let keys_count = node.keys.len() as u16;
    buff_cursor.write_keys_count(KeysCount::new(keys_count))?;
    //
    for i in 0..(keys_count as usize) {
        debug_assert!(!node.keys[i].is_zero());
        let offset = node.keys[i];
        buff_cursor.write_record_offset(offset)?;
    }
    for i in 0..((keys_count as usize) + 1) {
        let offset = if i < node.downs.len() {
            node.downs[i]
        } else {
            NodeOffset::new(0)
        };
        buff_cursor.write_node_offset(offset)?;
    }
    //
    Ok(buff_cursor.into_inner())
}

fn idx_write_node(
    file_nc: &mut VarFileNodeCache,
    mut node: IdxNode,
    is_new: bool,
) -> Result<IdxNode> {
    debug_assert!(!node.offset.is_zero());
    //
    let mut buf_vec = idx_serialize_to_buf(&node)?;
    let buf_ref = &mut buf_vec;
    let buf_len = buf_ref.len();
    //
    #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
    let encoded_len = 4;
    #[cfg(feature = "vf_vu64")]
    let encoded_len = vu64::encoded_len(buf_len as u64);
    //
    let new_node_size = NodeSize::new(buf_len as u32 + encoded_len as u32);
    let new_node_size = node_size_roudup(new_node_size);
    //
    if !is_new {
        #[cfg(not(feature = "node_cache"))]
        let old_node_size = {
            let _ = file_nc.0.seek_from_start(node.offset)?;
            file_nc.0.read_node_size()?
        };
        #[cfg(feature = "node_cache")]
        let old_node_size = {
            if let Some(node_size) = file_nc.1.get_node_size(&node.offset) {
                node_size
            } else {
                let _ = file_nc.0.seek_from_start(node.offset)?;
                file_nc.0.read_node_size()?
            }
        };
        if new_node_size <= old_node_size {
            // over writes.
            #[cfg(not(feature = "node_cache"))]
            {
                let _ = file_nc.0.seek_from_start(node.offset)?;
                file_nc.0.write_node_size(old_node_size)?;
                file_nc.0.write_all(buf_ref)?;
                return Ok(node);
            }
            #[cfg(feature = "node_cache")]
            {
                let node =
                    file_nc
                        .1
                        .put(&mut file_nc.0, node, old_node_size, buf_ref.clone(), true)?;
                return Ok(node);
            }
        } else {
            // delete old and add new
            #[cfg(feature = "node_cache")]
            file_nc.1.delete(&node.offset);
            // old
            idx_file_push_free_list(&mut file_nc.0, node.offset, old_node_size)?;
        }
    }
    // add new.
    {
        let free_node_offset = idx_file_pop_free_list(&mut file_nc.0, new_node_size)?;
        let new_node_offset = if !free_node_offset.is_zero() {
            let _ = file_nc.0.seek_from_start(free_node_offset)?;
            let node_size = file_nc.0.read_node_size()?;
            file_nc.0.write_node_clear(free_node_offset, node_size)?;
            free_node_offset
        } else {
            let _ = file_nc.0.seek(SeekFrom::End(0))?;
            let node_offset = NodeOffset::new(file_nc.0.stream_position()?);
            file_nc.0.write_node_clear(node_offset, new_node_size)?;
            node_offset
        };
        file_nc.0.write_all_small(buf_ref)?;
        node.offset = new_node_offset;
    }
    //
    Ok(node)
}
*/

fn idx_write_node(
    file_nc: &mut VarFileNodeCache,
    mut node: IdxNode,
    is_new: bool,
) -> Result<IdxNode> {
    debug_assert!(!node.offset.is_zero());
    //
    let buf_len = idx_encoded_node_size(&node);
    //
    #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
    let encoded_len = 4;
    #[cfg(feature = "vf_vu64")]
    let encoded_len = vu64::encoded_len(buf_len as u64);
    //
    let new_node_size = NodeSize::new(buf_len as u32 + encoded_len as u32);
    let new_node_size = node_size_roudup(new_node_size);
    //
    if !is_new {
        #[cfg(not(feature = "node_cache"))]
        let old_node_size = {
            let _ = file_nc.0.seek_from_start(node.offset)?;
            file_nc.0.read_node_size()?
        };
        #[cfg(feature = "node_cache")]
        let old_node_size = {
            if let Some(node_size) = file_nc.1.get_node_size(&node.offset) {
                node_size
            } else {
                let _ = file_nc.0.seek_from_start(node.offset)?;
                file_nc.0.read_node_size()?
            }
        };
        if new_node_size <= old_node_size {
            // over writes.
            #[cfg(not(feature = "node_cache"))]
            {
                let _ = file_nc.0.seek_from_start(node.offset)?;
                file_nc.0.write_node_size(old_node_size)?;
                idx_write_node_one(&mut file_nc.0, &node)?;
                return Ok(node);
            }
            #[cfg(feature = "node_cache")]
            {
                let node = file_nc.1.put(&mut file_nc.0, node, old_node_size, true)?;
                return Ok(node);
            }
        } else {
            // delete old and add new
            #[cfg(feature = "node_cache")]
            file_nc.1.delete(&node.offset);
            // old
            idx_file_push_free_list(&mut file_nc.0, node.offset, old_node_size)?;
        }
    }
    // add new.
    {
        let free_node_offset = idx_file_pop_free_list(&mut file_nc.0, new_node_size)?;
        let new_node_offset = if !free_node_offset.is_zero() {
            let _ = file_nc.0.seek_from_start(free_node_offset)?;
            let node_size = file_nc.0.read_node_size()?;
            file_nc.0.write_node_clear(free_node_offset, node_size)?;
            free_node_offset
        } else {
            let _ = file_nc.0.seek(SeekFrom::End(0))?;
            let node_offset = NodeOffset::new(file_nc.0.stream_position()?);
            file_nc.0.write_node_clear(node_offset, new_node_size)?;
            node_offset
        };
        idx_write_node_one(&mut file_nc.0, &node)?;
        node.offset = new_node_offset;
    }
    //
    Ok(node)
}

fn idx_encoded_node_size(node: &IdxNode) -> usize {
    let mut sum_size = 0usize;
    //
    let keys_count = node.keys.len() as u16;
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
        debug_assert!(!node.keys[i].is_zero());
        let _offset = node.keys[i];
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
            sum_size += vu64::encoded_len(_offset.as_value() as u64) as usize;
        }
    }
    for i in 0..((keys_count as usize) + 1) {
        let _offset = if i < node.downs.len() {
            node.downs[i]
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
            sum_size += vu64::encoded_len(_offset.as_value() as u64) as usize;
        }
    }
    //
    sum_size
}

pub(crate) fn idx_write_node_one(file: &mut VarFile, node: &IdxNode) -> Result<()> {
    let keys_count = node.keys.len() as u16;
    file.write_keys_count(KeysCount::new(keys_count))?;
    //
    for i in 0..(keys_count as usize) {
        debug_assert!(!node.keys[i].is_zero());
        let offset = node.keys[i];
        file.write_record_offset(offset)?;
    }
    for i in 0..((keys_count as usize) + 1) {
        let offset = if i < node.downs.len() {
            node.downs[i]
        } else {
            NodeOffset::new(0)
        };
        file.write_node_offset(offset)?;
    }
    //
    Ok(())
}

#[cfg(not(feature = "node_cache"))]
fn idx_read_node(file_nc: &mut VarFileNodeCache, offset: NodeOffset) -> Result<IdxNode> {
    debug_assert!(!offset.is_zero());
    //
    let _ = file_nc.0.seek_from_start(offset)?;
    let node_size = file_nc.0.read_node_size()?;
    let keys_count = file_nc.0.read_keys_count()?;
    let keys_count: usize = keys_count.try_into().unwrap();
    //
    let mut node = IdxNode::with_node_size(offset, node_size);
    for _i in 0..keys_count {
        let record_offset = file_nc
            .0
            .read_record_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        debug_assert!(!record_offset.is_zero());
        node.keys.push(record_offset);
    }
    for _i in 0..(keys_count + 1) {
        let node_offset = file_nc
            .0
            .read_node_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        node.downs.push(node_offset);
    }
    //
    Ok(node)
}

#[cfg(feature = "node_cache")]
fn idx_read_node(file_nc: &mut VarFileNodeCache, offset: NodeOffset) -> Result<IdxNode> {
    debug_assert!(!offset.is_zero());
    //
    if let Some(rc) = file_nc.1.get(&offset) {
        return Ok(rc.as_ref().clone());
    }
    //
    let _ = file_nc.0.seek_from_start(offset)?;
    let node_size = file_nc.0.read_node_size()?;
    debug_assert!(!node_size.is_zero());
    /*
    let node_size_usize: usize = node_size.try_into().unwrap();
    //
    #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
    let encoded_len = 4 as usize;
    #[cfg(feature = "vf_vu64")]
    let encoded_len = vu64::encoded_len(node_size_usize as u64) as usize;
    //
    let buf = {
        let mut vec = vec![0u8; node_size_usize - encoded_len];
        file_nc.0.read_exact(&mut vec)?;
        vec
    };
    */
    //
    let _ = file_nc.0.seek_from_start(offset)?;
    let node_size = file_nc.0.read_node_size()?;
    let keys_count = file_nc.0.read_keys_count()?;
    let keys_count: usize = keys_count.try_into().unwrap();
    //
    let mut node = IdxNode::with_node_size(offset, node_size);
    for _i in 0..keys_count {
        let record_offset = file_nc
            .0
            .read_record_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        debug_assert!(!record_offset.is_zero());
        node.keys.push(record_offset);
    }
    for _i in 0..(keys_count + 1) {
        let node_offset = file_nc
            .0
            .read_node_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        node.downs.push(node_offset);
    }
    //
    let node = file_nc.1.put(&mut file_nc.0, node, node_size, false)?;
    //
    Ok(node)
}

// for debug
fn idx_to_graph_string(
    file_nc: &mut VarFileNodeCache,
    head: &str,
    node: &IdxNode,
) -> Result<String> {
    let mut gs = format!("{}{}:{:04x}\n", head, GRAPH_NODE_ST, node.offset.as_value());
    let mut i = node.downs.len() - 1;
    let node_offset = node.downs[i];
    if !node_offset.is_zero() {
        let node = idx_read_node(file_nc, node_offset)
            .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
        let gs0 = idx_to_graph_string(file_nc, &format!("{}    ", head), &node)?;
        gs += &gs0;
    }
    while i > 0 {
        i -= 1;
        let record_offset = node.keys[i];
        gs += &format!("{}{:04x}\n", head, record_offset.as_value());
        let node_offset = node.downs[i];
        if !node_offset.is_zero() {
            let node = idx_read_node(file_nc, node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            let gs0 = idx_to_graph_string(file_nc, &format!("{}    ", head), &node)?;
            gs += &gs0;
        }
    }
    gs += &format!("{}{}\n", head, GRAPH_NODE_ED);
    //
    Ok(gs)
}

//const GRAPH_NODE_ST: &str = "∧";
//const GRAPH_NODE_ED: &str = "∨";
const GRAPH_NODE_ST: &str = "^";
const GRAPH_NODE_ED: &str = "v";
//const GRAPH_NODE_ST: &str = "{";
//const GRAPH_NODE_ED: &str = "}";

use super::dbxxx::FileDbXxxInner;
use super::dbxxx::FileDbXxxInnerKT;

fn idx_to_graph_string_with_key_string<KT>(
    file_nc: &mut VarFileNodeCache,
    head: &str,
    node: &IdxNode,
    dbxxx: &FileDbXxxInner<KT>,
) -> Result<String>
where
    KT: FileDbXxxInnerKT + std::fmt::Display,
{
    let mut gs = format!(
        "{}{}:0x{:04x},{03}\n",
        head,
        GRAPH_NODE_ST,
        node.offset.as_value(),
        node.size
    );
    let mut i = node.downs.len() - 1;
    let node_offset = node.downs[i];
    if !node_offset.is_zero() {
        let node = idx_read_node(file_nc, node_offset)
            .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
        let gs0 =
            idx_to_graph_string_with_key_string(file_nc, &format!("{}    ", head), &node, dbxxx)?;
        gs += &gs0;
    }
    while i > 0 {
        i -= 1;
        let record_offset = node.keys[i];
        if !record_offset.is_zero() {
            /*
            let key_string = dat_file
                .read_record_key(key_offset)?
                .map(|val| String::from_utf8_lossy(&val).to_string())
                .unwrap();
            */
            let key_string = dbxxx.load_key_string_no_cache(record_offset)?;
            gs += &format!(
                "{}{:04x}:'{}'\n",
                head,
                record_offset.as_value(),
                key_string
            );
        }
        let node_offset = node.downs[i];
        if !node_offset.is_zero() {
            let node = idx_read_node(file_nc, node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            let gs0 = idx_to_graph_string_with_key_string(
                file_nc,
                &format!("{}    ", head),
                &node,
                dbxxx,
            )?;
            gs += &gs0;
        }
    }
    gs += &format!("{}{}\n", head, GRAPH_NODE_ED);
    //
    Ok(gs)
}

fn idx_count_of_used_node<F>(
    file_nc: &mut VarFileNodeCache,
    node: &IdxNode,
    node_vec: &mut Vec<(u32, u64)>,
    record_vec: &mut Vec<(u32, u64)>,
    read_record_size_func: F,
) -> Result<()>
where
    F: Fn(RecordOffset) -> Result<RecordSize> + Copy,
{
    match node_vec.iter().position(|v| v.0 == node.size.as_value()) {
        Some(sz_idx) => {
            node_vec[sz_idx].1 += 1;
        }
        None => {
            let last = node_vec.len() - 1;
            node_vec[last].1 += 1;
        }
    }
    //
    let mut i = node.downs.len() - 1;
    let node_offset = node.downs[i];
    if !node_offset.is_zero() {
        let node = idx_read_node(file_nc, node_offset)
            .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
        idx_count_of_used_node(file_nc, &node, node_vec, record_vec, read_record_size_func)?;
    }
    while i > 0 {
        i -= 1;
        //
        let record_offset = node.keys[i];
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
        let node_offset = node.downs[i];
        if !node_offset.is_zero() {
            let node = idx_read_node(file_nc, node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            idx_count_of_used_node(file_nc, &node, node_vec, record_vec, read_record_size_func)?;
        }
    }
    //
    Ok(())
}

fn idx_record_size_stats<F>(
    file_nc: &mut VarFileNodeCache,
    node: &IdxNode,
    record_vec: &mut RecordSizeStats,
    read_record_size_func: F,
) -> Result<()>
where
    F: Fn(RecordOffset) -> Result<RecordSize> + Copy,
{
    let mut i = node.downs.len() - 1;
    let node_offset = node.downs[i];
    if !node_offset.is_zero() {
        let node = idx_read_node(file_nc, node_offset)
            .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
        idx_record_size_stats(file_nc, &node, record_vec, read_record_size_func)?;
    }
    while i > 0 {
        i -= 1;
        //
        let record_offset = node.keys[i];
        if !record_offset.is_zero() {
            let record_size = read_record_size_func(record_offset)?;
            record_vec.touch_size(record_size);
        }
        //
        let node_offset = node.downs[i];
        if !node_offset.is_zero() {
            let node = idx_read_node(file_nc, node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset.as_value()));
            idx_record_size_stats(file_nc, &node, record_vec, read_record_size_func)?;
        }
    }
    //
    Ok(())
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
| 0      | 1..5  | node size   | size in bytes of this node: u32   |
| 0      | 1     | size        | size in bytes of this node        |
|        |       |             | (must be <= 0x7F)                 |
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
| 0      | 1     | size        | size in bytes of this node        |
|        |       |             | (bit or 0x80)                     |
| --     | 1     | keys-count  | always zero                       |
| --     | 8     | next        | next free record offset           |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
