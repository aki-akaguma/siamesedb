#![allow(dead_code)]

use super::dat;
use super::KeyType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Result, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

use super::buf::BufFile;

const IDX_HEADER_SZ: u64 = 128;

#[derive(Debug, Clone)]
pub struct IdxFile(Rc<RefCell<(BufFile, KeyType)>>);

impl IdxFile {
    pub fn open<P: AsRef<Path>>(path: P, ks_name: &str, kt: KeyType) -> Result<Self> {
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.idx", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = BufFile::with_capacity(16, 1024, std_file)?;
        let _ = file.seek(SeekFrom::End(0))?;
        let len = file.stream_position()?;
        if len == 0 {
            idx_file_write_init_header(&mut file, kt)?;
            // writing top node
            let top_node = IdxNode::new(IDX_HEADER_SZ);
            let _new_node = idx_write_node(&mut file, top_node, true)?;
            assert!(_new_node.offset == IDX_HEADER_SZ);
        } else {
            idx_file_check_header(&mut file, kt)?;
        }
        //
        Ok(Self(Rc::new(RefCell::new((file, kt)))))
    }
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.0.sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.0.sync_data()
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
        if node.offset == 0 {
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
    pub fn read_node(&self, offset: u64) -> Result<IdxNode> {
        let mut locked = self.0.borrow_mut();
        idx_read_node(&mut locked.0, offset)
    }
    pub fn write_node(&self, node: IdxNode) -> Result<IdxNode> {
        let mut locked = self.0.borrow_mut();
        idx_write_node(&mut locked.0, node, false)
    }
    pub fn write_new_node(&self, mut node: IdxNode) -> Result<IdxNode> {
        node.offset = {
            let mut locked = self.0.borrow_mut();
            let _ = locked.0.seek(SeekFrom::End(0));
            locked.0.stream_position()?
        };
        let mut locked = self.0.borrow_mut();
        idx_write_node(&mut locked.0, node, true)
    }
    pub fn delete_node(&self, node: IdxNode) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        idx_delete_node(&mut locked.0, node)
    }
}

// for debug
impl IdxFile {
    pub fn to_graph_string(&self) -> Result<String> {
        let top_node = self.read_top_node()?;
        let mut locked = self.0.borrow_mut();
        idx_to_graph_string(&mut locked.0, "", &top_node)
    }
    pub fn to_graph_string_with_key_string(&self, dat_file: dat::DatFile) -> Result<String> {
        let top_node = self.read_top_node()?;
        let mut locked = self.0.borrow_mut();
        idx_to_graph_string_with_key_string(&mut locked.0, "", &top_node, &dat_file)
    }
    // check the index tree is balanced
    pub fn is_balanced(&self, node: &IdxNode) -> Result<bool> {
        let node_offset = node.downs[0];
        let h = if node_offset != 0 {
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
            let hh = if node_offset != 0 {
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
        let mut mx = if node_offset != 0 {
            let node1 = self.read_node(node_offset)?;
            self.height(&node1)?
        } else {
            0
        };
        for i in 1..node.downs.len() {
            let node_offset = node.downs[i];
            let h = if node_offset != 0 {
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

    pub fn is_mst_valid(&self, node: &IdxNode, dat_file: dat::DatFile) -> Result<bool> {
        if node.keys.is_empty() {
            return Ok(true);
        }
        let key_offset = node.keys[0];
        let key_string = if key_offset != 0 {
            dat_file
                .read_record_key(key_offset)?
                .map(|val| String::from_utf8_lossy(&val).to_string())
                .unwrap()
        } else {
            String::new()
        };
        let node_offset = node.downs[0];
        if node_offset != 0 {
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
            if key_offset2 == 0 {
                break;
            }
            let key_string1 = if key_offset1 != 0 {
                dat_file
                    .read_record_key(key_offset1)?
                    .map(|val| String::from_utf8_lossy(&val).to_string())
                    .unwrap()
            } else {
                String::new()
            };
            let key_string2 = if key_offset2 != 0 {
                dat_file
                    .read_record_key(key_offset2)?
                    .map(|val| String::from_utf8_lossy(&val).to_string())
                    .unwrap()
            } else {
                String::new()
            };
            if key_string1 >= key_string2 {
                return Ok(false);
            }
            if node_offset != 0 {
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
        let key_offset = node.keys[node.keys.len() - 1];
        let node_offset = node.downs[node.keys.len()];
        if node_offset != 0 {
            let node1 = self.read_node(node_offset)?;
            if key_offset != 0 {
                let key_string = dat_file
                    .read_record_key(key_offset)?
                    .map(|val| String::from_utf8_lossy(&val).to_string())
                    .unwrap();
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

    fn is_small(&self, key: &str, node: &IdxNode, dat_file: dat::DatFile) -> Result<bool> {
        for i in 0..node.keys.len() {
            let node_offset = node.downs[i];
            if node_offset != 0 {
                let node1 = self.read_node(node_offset)?;
                if !self.is_small(key, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            let key_offset = node.keys[i];
            if key_offset != 0 {
                let ket_string1 = dat_file
                    .read_record_key(key_offset)?
                    .map(|val| String::from_utf8_lossy(&val).to_string())
                    .unwrap();
                if key <= &ket_string1 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.downs[node.keys.len()];
        if node_offset != 0 {
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
            if node_offset != 0 {
                let node1 = self.read_node(node_offset)?;
                if !self.is_between(key1, key2, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            let key_offset11 = node.keys[i];
            if key_offset11 != 0 {
                let ket_string11 = dat_file
                    .read_record_key(key_offset11)?
                    .map(|val| String::from_utf8_lossy(&val).to_string())
                    .unwrap();
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
        if node_offset != 0 {
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
            if node_offset != 0 {
                let node1 = self.read_node(node_offset)?;
                if !self.is_large(key, &node1, dat_file.clone())? {
                    return Ok(false);
                }
            }
            let key_offset = node.keys[i];
            if key_offset != 0 {
                let ket_string1 = dat_file
                    .read_record_key(key_offset)?
                    .map(|val| String::from_utf8_lossy(&val).to_string())
                    .unwrap();
                if key >= &ket_string1 {
                    return Ok(false);
                }
            }
        }
        //
        let node_offset = node.downs[node.keys.len()];
        if node_offset != 0 {
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
        if n == 1 && top_node.downs[0] != 0 {
            return Ok(false);
        }
        for i in 0..n {
            let node_offset = top_node.downs[i];
            if node_offset != 0 {
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
            if node_offset != 0 {
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
            if node_offset != 0 {
                let node1 = self.read_node(node_offset)?;
                cnt += self.depth_of_node_tree(&node1)?;
            }
        }
        //
        Ok(cnt)
    }
    pub fn count_of_free_nn_list(&self) -> Result<Vec<(usize, u64)>> {
        let mut vec = Vec::new();
        let mut locked = self.0.borrow_mut();
        #[cfg(feature = "idx_u32u32")]
        let sz_ary = [(NODE_SLOTS_MAX as usize * 2 - 1) * 4];
        #[cfg(feature = "idx_u64u64")]
        let sz_ary = [(NODE_SLOTS_MAX as usize * 2 - 1) * 8];
        #[cfg(feature = "idx_varvar")]
        let sz_ary = NODE_SIZE_ARY;
        for node_size in sz_ary {
            let cnt = idx_file_count_of_free_list(&mut locked.0, node_size)?;
            vec.push((node_size, cnt));
        }
        Ok(vec)
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
| 4      | 4     | signature2  | [b'd', b'b', b'1', 0u8]   |
| 8      | 8     | top node    | offset of top node        |
| 16     | 8     | free16 off  | offset of free 16 list    |
| 24     | 8     | free24 off  | offset of free 24 list    |
| 32     | 8     | free32 off  | offset of free 32 list    |
| 40     | 8     | free48 off  | offset of free 48 list    |
| 48     | 8     | free64 off  | offset of free 64 list    |
| 56     | 8     | free92 off  | offset of free 92 list    |
| 64     | 8     | free128 off | offset of free 128 list   |
| 72     | 8     | free256 off | offset of free 256 list   |
| 80     | 8     | freevar off | offset of free var list   |
| 88     | 48    | reserve1    |                           |
+--------+-------+-------------+---------------------------+
```

- signature1: always fixed 4 bytes
- signature2: fixed 4 bytes, variable in future.

*/
fn idx_file_write_init_header(file: &mut BufFile, kt: KeyType) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    //
    let kt_byte = kt.signature();
    // signature
    let _ = file.write(&[b's', b'h', b'a', b'm'])?;
    let _ = file.write(&[b'd', b'b', kt_byte, b'1'])?;
    // root offset
    file.write_u64::<LittleEndian>(IDX_HEADER_SZ)?;
    // free16 .. rserve1
    let _ = file.write(&[0u8; 112]);
    //
    Ok(())
}

fn idx_file_check_header(file: &mut BufFile, kt: KeyType) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    //
    let kt_byte = kt.signature();
    // signature
    let mut sig1 = [0u8, 0u8, 0u8, 0u8];
    let mut sig2 = [0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig1)?;
    if sig1 != [b's', b'h', b'a', b'm'] {
        panic!("invalid header signature1");
    }
    let _sz = file.read_exact(&mut sig2)?;
    if sig2 != [b'd', b'b', kt_byte, b'1'] {
        panic!("invalid header signature2");
    }
    // top node offset
    let _top_node_offset = file.read_u64::<LittleEndian>()?;
    if _top_node_offset == 0 {
        panic!("invalid root offset");
    }
    //
    Ok(())
}

fn idx_file_read_top_node_offset(file: &mut BufFile) -> Result<u64> {
    let _ = file.seek(SeekFrom::Start(8))?;
    file.read_u64::<LittleEndian>()
}

fn idx_file_write_top_node_offset(file: &mut BufFile, offset: u64) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(8))?;
    file.write_u64::<LittleEndian>(offset)?;
    Ok(())
}

// (NODE_SLOTS_MAX as usize * 2 - 1)
#[cfg(feature = "idx_varvar")]
const NODE_SIZE_ARY: [usize; 9] = [15, 23, 31, 39, 47, 51, 63, 71, 256];

//const NODE_SIZE_ARY: [usize; 9] = [17, 26, 35, 44, 53, 62, 71, 80, 256];
//const NODE_SIZE_ARY: [usize; 9] = [15, 23, 31, 39, 47, 51, 63, 71, 256];
//const NODE_SIZE_ARY: [usize; 9] = [18, 27, 36, 45, 54, 63, 72, 81, 256];

#[cfg(feature = "idx_varvar")]
const NODE_SIZE_FREE_OFFSET: [usize; 9] = [16, 24, 32, 40, 48, 56, 64, 72, 80];

fn free_nn_list_offset_of_header(_node_size: usize) -> u64 {
    assert!(_node_size > 0);
    #[cfg(not(feature = "idx_varvar"))]
    let r = 16;
    #[cfg(feature = "idx_varvar")]
    let r = {
        for i in 0..NODE_SIZE_ARY.len() {
            if NODE_SIZE_ARY[i] == _node_size {
                return NODE_SIZE_FREE_OFFSET[i] as u64;
            }
        }
        if _node_size > NODE_SIZE_ARY[NODE_SIZE_ARY.len() - 2] {
            NODE_SIZE_FREE_OFFSET[NODE_SIZE_FREE_OFFSET.len() - 1] as u64
        } else {
            panic!("invalid node_size: {}", _node_size);
        }
    };
    r
}

fn node_size_roudup(node_size: usize) -> usize {
    assert!(node_size > 0);
    #[cfg(not(feature = "idx_varvar"))]
    let r = node_size;
    #[cfg(feature = "idx_varvar")]
    let r = {
        for &n_sz in NODE_SIZE_ARY.iter().take(NODE_SIZE_ARY.len() - 1) {
            if node_size <= n_sz {
                return n_sz;
            }
        }
        eprintln!("WARN:: node is over size: {}", node_size);
        node_size
    };
    r
}

fn idx_file_read_free_nn_offset(file: &mut BufFile, node_size: usize) -> Result<u64> {
    let _ = file.seek(SeekFrom::Start(free_nn_list_offset_of_header(node_size)))?;
    file.read_u64::<LittleEndian>()
}

fn idx_file_write_free_nn_offset(file: &mut BufFile, node_size: usize, offset: u64) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(free_nn_list_offset_of_header(node_size)))?;
    file.write_u64::<LittleEndian>(offset)?;
    Ok(())
}

fn idx_file_count_of_free_list(file: &mut BufFile, new_node_size: usize) -> Result<u64> {
    let mut count = 0;
    let free_1st = idx_file_read_free_nn_offset(file, new_node_size)?;
    if free_1st != 0 {
        let mut free_next_offset = free_1st;
        while free_next_offset != 0 {
            count += 1;
            free_next_offset = {
                let _a = file.seek(SeekFrom::Start(free_next_offset))?;
                assert!(_a == free_next_offset);
                #[cfg(not(feature = "idx_varvar"))]
                let free_next = {
                    let _node_len = file.read_node_size()?;
                    assert!(_node_len > 0x7F);
                    file.read_node_offset()?
                };
                #[cfg(feature = "idx_varvar")]
                let free_next = {
                    let mut inp = file.bytes();
                    let _node_len = inp.read_node_size()?;
                    assert!(_node_len > 0x7F);
                    inp.read_node_offset()?
                };
                free_next
            };
        }
    }
    Ok(count)
}

fn idx_file_pop_free_list(file: &mut BufFile, new_node_size: usize) -> Result<u64> {
    let free_1st = idx_file_read_free_nn_offset(file, new_node_size)?;
    if free_1st != 0 {
        let free_next = {
            let _ = file.seek(SeekFrom::Start(free_1st))?;
            #[cfg(not(feature = "idx_varvar"))]
            let (free_next, node_len) = {
                let node_len = file.read_node_size()?;
                assert!(node_len > 0x7F);
                let node_offset = file.read_node_offset()?;
                (node_offset, node_len & 0x7F)
            };
            #[cfg(feature = "idx_varvar")]
            let (free_next, node_len) = {
                let mut inp = file.bytes();
                let node_len = inp.read_node_size()?;
                assert!(node_len > 0x7F);
                let node_offset = inp.read_node_offset()?;
                (node_offset, node_len & 0x7F)
            };
            //
            let _ = file.seek(SeekFrom::Start(free_1st))?;
            file.write_node_size(node_len)?;
            let buff = &IDX_ZERO_ARY[0..node_len];
            file.write_all(buff)?;
            //
            free_next
        };
        idx_file_write_free_nn_offset(file, new_node_size, free_next)?;
    }
    Ok(free_1st)
}

fn idx_file_push_free_list(
    file: &mut BufFile,
    old_node_offset: u64,
    old_node_size: usize,
) -> Result<()> {
    if old_node_offset == 0 {
        return Ok(());
    }
    assert!(old_node_size <= 0x7F);
    //
    let free_1st = idx_file_read_free_nn_offset(file, old_node_size)?;
    {
        let _a = file.seek(SeekFrom::Start(old_node_offset))?;
        assert!(_a == old_node_offset);
        #[cfg(not(feature = "idx_varvar"))]
        {
            file.write_node_size(old_node_size | 0x80)?;
            file.write_node_offset(free_1st)?;
        }
        #[cfg(feature = "idx_varvar")]
        {
            let mut enc_buf = Vec::with_capacity(9);
            file.write_node_size(old_node_size | 0x80)?;
            file.write_node_offset(free_1st, &mut enc_buf)?;
        }
    }
    idx_file_write_free_nn_offset(file, old_node_size, old_node_offset)?;
    Ok(())
}

//
// ref) http://wwwa.pikara.ne.jp/okojisan/b-tree/bsb-tree.html
//

pub const NODE_SLOTS_MAX: u16 = 5;
//pub const NODE_SLOTS_MAX: u16 = 9;
//pub const NODE_SLOTS_MAX: u16 = 64;
//pub const NODE_SLOTS_MAX: u16 = 256;
pub const NODE_SLOTS_MAX_HALF: u16 = (NODE_SLOTS_MAX + 1) / 2;

#[derive(Debug, Default, Clone)]
pub struct IdxNode {
    /// active node flag is used insert operation. this not store into file.
    pub is_active: bool,
    /// offset of IdxNode in idx file.
    pub offset: u64,
    /// key slot: offset of key-value record in dat file.
    pub keys: Vec<u64>,
    //pub keys: [u64; (NODE_SLOTS_MAX as usize) - 1],
    /// down slot: offset of next IdxNode in idx file.
    pub downs: Vec<u64>,
    //pub downs: [u64; (NODE_SLOTS_MAX as usize)],
}

impl IdxNode {
    pub fn new(offset: u64) -> Self {
        Self {
            offset,
            keys: Vec::with_capacity((NODE_SLOTS_MAX as usize) - 1),
            downs: Vec::with_capacity(NODE_SLOTS_MAX as usize),
            ..Default::default()
        }
    }
    pub fn new_active(key_offset: u64, l_node_offset: u64, r_node_offset: u64) -> Self {
        let mut r = Self {
            is_active: true,
            ..Default::default()
        };
        r.keys.push(key_offset);
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
            let mut r = Self::new(0);
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

#[cfg(feature = "idx_u32u32")]
use byteorder::ByteOrder;

#[cfg(feature = "idx_u32u32")]
trait ReadShamExt: std::io::Read {
    #[inline]
    fn read_key_offset(&mut self) -> Result<u64> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let r = LittleEndian::read_u32(&buf);
        Ok(r as u64)
    }
    #[inline]
    fn read_node_offset(&mut self) -> Result<u64> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        let r = LittleEndian::read_u32(&buf);
        Ok(r as u64)
    }
    #[inline]
    fn read_node_size(&mut self) -> Result<usize> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        let node_size = buf[0] as usize;
        //assert!(node_size <= 0x7F);
        Ok(node_size)
    }
}

#[cfg(feature = "idx_u32u32")]
trait WriteShamExt: std::io::Write {
    #[inline]
    fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        assert!(key_offset <= u32::MAX as u64);
        let mut buf = [0; 4];
        LittleEndian::write_u32(&mut buf, key_offset as u32);
        self.write_all(&buf)
    }
    #[inline]
    fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        assert!(node_offset <= u32::MAX as u64);
        let mut buf = [0; 4];
        LittleEndian::write_u32(&mut buf, node_offset as u32);
        self.write_all(&buf)
    }
    #[inline]
    fn write_node_size(&mut self, node_size: usize) -> Result<()> {
        //assert!(node_size <= 0x7F as usize);
        self.write_all(&[node_size as u8])
    }
}

#[cfg(feature = "idx_u64u64")]
use byteorder::ByteOrder;

#[cfg(feature = "idx_u64u64")]
trait ReadShamExt: std::io::Read {
    #[inline]
    fn read_key_offset(&mut self) -> Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        let r = LittleEndian::read_u64(&buf);
        Ok(r)
    }
    #[inline]
    fn read_node_offset(&mut self) -> Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        let r = LittleEndian::read_u64(&buf);
        Ok(r)
    }
    #[inline]
    fn read_node_size(&mut self) -> Result<usize> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        let node_size = buf[0] as usize;
        //assert!(node_size <= 0x7F);
        Ok(node_size)
    }
}

#[cfg(feature = "idx_u64u64")]
trait WriteShamExt: std::io::Write {
    #[inline]
    fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let mut buf = [0; 8];
        LittleEndian::write_u64(&mut buf, key_offset as u64);
        self.write_all(&buf)
    }
    #[inline]
    fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let mut buf = [0; 8];
        LittleEndian::write_u64(&mut buf, node_offset as u64);
        self.write_all(&buf)
    }
    #[inline]
    fn write_node_size(&mut self, node_size: usize) -> Result<()> {
        //assert!(node_size <= 0x7F as usize);
        self.write_all(&[node_size as u8])
    }
}

#[cfg(feature = "idx_varvar")]
trait ReadShamExt {
    fn read_key_offset(&mut self) -> Result<u64>;
    fn read_node_offset(&mut self) -> Result<u64>;
    fn read_node_size(&mut self) -> Result<usize>;
}
#[cfg(feature = "idx_varvar")]
impl<R: std::io::Read> ReadShamExt for std::io::Bytes<R> {
    #[inline]
    fn read_key_offset(&mut self) -> Result<u64> {
        super::varint::decode_varint(self)
    }
    #[inline]
    fn read_node_offset(&mut self) -> Result<u64> {
        super::varint::decode_varint(self)
    }
    #[inline]
    fn read_node_size(&mut self) -> Result<usize> {
        let node_size = match self.next() {
            Some(r) => r?,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "under length at node_size.",
                ));
            }
        };
        //assert!(node_size <= 0x7F);
        Ok(node_size.into())
    }
}

#[cfg(feature = "idx_varvar")]
trait WriteShamExt: std::io::Write {
    #[inline]
    fn write_key_offset(&mut self, key_offset: u64, enc_buf: &mut Vec<u8>) -> Result<()> {
        super::varint::encode_varint(key_offset, enc_buf);
        self.write_all(enc_buf)
    }
    #[inline]
    fn write_node_offset(&mut self, node_offset: u64, enc_buf: &mut Vec<u8>) -> Result<()> {
        super::varint::encode_varint(node_offset, enc_buf);
        self.write_all(enc_buf)
    }
    #[inline]
    fn write_node_size(&mut self, node_size: usize) -> Result<()> {
        //assert!(node_size <= 0x7F as usize);
        self.write_all(&[node_size as u8])
    }
}

#[cfg(not(feature = "idx_varvar"))]
impl<R: std::io::Read + ?Sized> ReadShamExt for R {}
impl<W: std::io::Write + ?Sized> WriteShamExt for W {}

fn idx_delete_node(file: &mut BufFile, node: IdxNode) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(node.offset))?;
    #[cfg(not(feature = "idx_varvar"))]
    let old_node_len = file.read_node_size()?;
    #[cfg(feature = "idx_varvar")]
    let old_node_len = file.bytes().read_node_size()?;
    assert!(old_node_len <= 0x7F);
    idx_file_push_free_list(file, node.offset, old_node_len)?;
    Ok(())
}

fn idx_serialize_to_buf(node: &IdxNode) -> Result<Vec<u8>> {
    #[cfg(feature = "idx_varvar")]
    let mut enc_buf: Vec<u8> = Vec::with_capacity(9);
    //
    let mut buff_cursor = Cursor::new(Vec::with_capacity(9 * (7 + 8)));
    //
    for i in 0..(NODE_SLOTS_MAX as usize - 1) {
        let val = if i < node.keys.len() {
            let v = node.keys[i];
            assert!(v != 0);
            v
        } else {
            0
        };
        #[cfg(not(feature = "idx_varvar"))]
        buff_cursor.write_key_offset(val)?;
        #[cfg(feature = "idx_varvar")]
        buff_cursor.write_key_offset(val, &mut enc_buf)?;
    }
    //
    for i in 0..(NODE_SLOTS_MAX as usize) {
        let val = if i < node.downs.len() {
            node.downs[i]
        } else {
            0
        };
        #[cfg(not(feature = "idx_varvar"))]
        buff_cursor.write_node_offset(val)?;
        #[cfg(feature = "idx_varvar")]
        buff_cursor.write_node_offset(val, &mut enc_buf)?;
    }
    //
    Ok(buff_cursor.into_inner())
}

fn idx_write_node(file: &mut BufFile, mut node: IdxNode, is_new: bool) -> Result<IdxNode> {
    assert!(node.offset != 0);
    //
    let mut buf_vec = idx_serialize_to_buf(&node)?;
    let buf_ref = &mut buf_vec;
    let new_node_len = buf_ref.len();
    //
    let new_node_len = node_size_roudup(new_node_len);
    #[cfg(feature = "idx_varvar")]
    if buf_ref.len() < new_node_len {
        buf_ref.resize(new_node_len, 0u8);
    }
    //
    if !is_new {
        let _ = file.seek(SeekFrom::Start(node.offset))?;
        #[cfg(not(feature = "idx_varvar"))]
        let old_node_len = file.read_node_size()?;
        #[cfg(feature = "idx_varvar")]
        let old_node_len = file.bytes().read_node_size()?;
        assert!(old_node_len <= 0x7F);
        if new_node_len <= old_node_len {
            // over writes.
            let _ = file.seek(SeekFrom::Start(node.offset))?;
            #[cfg(not(feature = "idx_varvar"))]
            file.write_node_size(old_node_len)?;
            #[cfg(feature = "idx_varvar")]
            file.write_node_size(old_node_len)?;
            file.write_all(buf_ref)?;
            return Ok(node);
        } else {
            // delete old and add new
            // old
            idx_file_push_free_list(file, node.offset, old_node_len)?;
        }
    }
    // add new.
    {
        let free_node_offset = idx_file_pop_free_list(file, new_node_len)?;
        let new_node_offset = if free_node_offset != 0 {
            let _ = file.seek(SeekFrom::Start(free_node_offset))?;
            free_node_offset
        } else {
            let _ = file.seek(SeekFrom::End(0))?;
            file.stream_position()?
        };
        #[cfg(not(feature = "idx_varvar"))]
        file.write_node_size(new_node_len)?;
        #[cfg(feature = "idx_varvar")]
        file.write_node_size(new_node_len)?;
        file.write_all(buf_ref)?;
        node.offset = new_node_offset;
    }
    //
    Ok(node)
}
const IDX_ZERO_ARY: &[u8] = &[0u8; 128];

fn idx_read_node(file: &mut BufFile, offset: u64) -> Result<IdxNode> {
    assert!(offset != 0);
    //
    let _ = file.seek(SeekFrom::Start(offset))?;
    #[cfg(feature = "idx_varvar")]
    let mut inp = file.bytes();
    #[cfg(not(feature = "idx_varvar"))]
    let _node_len = file.read_node_size()?;
    #[cfg(feature = "idx_varvar")]
    let _node_len = inp.read_node_size()?;
    if _node_len <= 0x7F {
    } else {
        assert!(_node_len <= 0x7F);
    }
    //
    let mut node = IdxNode::new(offset);
    for _i in 0..(NODE_SLOTS_MAX as usize - 1) {
        #[cfg(not(feature = "idx_varvar"))]
        let key_offset = file
            .read_key_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        #[cfg(feature = "idx_varvar")]
        let key_offset = inp
            .read_key_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        if key_offset == 0 {
            for _j in (_i + 1)..(NODE_SLOTS_MAX as usize - 1) {
                #[cfg(not(feature = "idx_varvar"))]
                let _key_offset = file
                    .read_key_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _j));
                #[cfg(feature = "idx_varvar")]
                let _key_offset = inp
                    .read_key_offset()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
                //let _ = inp.next();
            }
            break;
        }
        node.keys.push(key_offset as u64);
    }
    for _i in 0..=node.keys.len() {
        #[cfg(not(feature = "idx_varvar"))]
        let node_offset = file
            .read_node_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        #[cfg(feature = "idx_varvar")]
        let node_offset = inp
            .read_node_offset()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        node.downs.push(node_offset as u64);
    }
    //
    Ok(node)
}

/*
```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 4     | size        | size in bytes of this node|
| 8      | 4*4   | keys        | offset of key-value       |
| 64     | 4*5   | downs       | offset of next node       |
+--------+-------+-------------+---------------------------+
```
*/

// for debug
fn idx_to_graph_string(file: &mut BufFile, head: &str, node: &IdxNode) -> Result<String> {
    let mut gs = format!("{}{}:{:04x}\n", head, GRAPH_NODE_ST, node.offset);
    let mut i = node.downs.len() - 1;
    let node_offset = node.downs[i];
    if node_offset != 0 {
        let node = idx_read_node(file, node_offset)
            .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset));
        let gs0 = idx_to_graph_string(file, &format!("{}    ", head), &node)?;
        gs += &gs0;
    }
    while i > 0 {
        i -= 1;
        let key_offset = node.keys[i];
        gs += &format!("{}{:04x}\n", head, key_offset);
        let node_offset = node.downs[i];
        if node_offset != 0 {
            let node = idx_read_node(file, node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset));
            let gs0 = idx_to_graph_string(file, &format!("{}    ", head), &node)?;
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

fn idx_to_graph_string_with_key_string(
    file: &mut BufFile,
    head: &str,
    node: &IdxNode,
    dat_file: &dat::DatFile,
) -> Result<String> {
    let mut gs = format!("{}{}:{:04x}\n", head, GRAPH_NODE_ST, node.offset);
    let mut i = node.downs.len() - 1;
    let node_offset = node.downs[i];
    if node_offset != 0 {
        let node = idx_read_node(file, node_offset)
            .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset));
        let gs0 =
            idx_to_graph_string_with_key_string(file, &format!("{}    ", head), &node, dat_file)?;
        gs += &gs0;
    }
    while i > 0 {
        i -= 1;
        let key_offset = node.keys[i];
        if key_offset != 0 {
            let key_string = dat_file
                .read_record_key(key_offset)?
                .map(|val| String::from_utf8_lossy(&val).to_string())
                .unwrap();
            gs += &format!("{}{:04x}:'{}'\n", head, key_offset, key_string);
        }
        let node_offset = node.downs[i];
        if node_offset != 0 {
            let node = idx_read_node(file, node_offset)
                .unwrap_or_else(|_| panic!("offset: {:04x}", node_offset));
            let gs0 = idx_to_graph_string_with_key_string(
                file,
                &format!("{}    ", head),
                &node,
                dat_file,
            )?;
            gs += &gs0;
        }
    }
    gs += &format!("{}{}\n", head, GRAPH_NODE_ED);
    //
    Ok(gs)
}
