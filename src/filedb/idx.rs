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

const IDX_HEADER_SZ: u64 = 64;

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
            idx_write_node(&mut file, top_node)?;
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
            let node = self.write_new_node(node)?;
            {
                let mut locked = self.0.borrow_mut();
                idx_file_write_top_node_offset(&mut locked.0, node.offset)?;
            }
            Ok(node)
        } else {
            let top_node_offset = {
                let mut locked = self.0.borrow_mut();
                idx_file_read_top_node_offset(&mut locked.0)?
            };
            if node.offset != top_node_offset {
                let mut locked = self.0.borrow_mut();
                idx_file_write_top_node_offset(&mut locked.0, node.offset)?;
            }
            self.write_node(node)
        }
    }
    //
    pub fn read_node(&self, offset: u64) -> Result<IdxNode> {
        let mut locked = self.0.borrow_mut();
        idx_read_node(&mut locked.0, offset)
    }
    pub fn write_node(&self, node: IdxNode) -> Result<IdxNode> {
        let mut locked = self.0.borrow_mut();
        idx_write_node(&mut locked.0, node)
    }
    pub fn write_new_node(&self, mut node: IdxNode) -> Result<IdxNode> {
        node.offset = {
            let mut locked = self.0.borrow_mut();
            let _ = locked.0.seek(SeekFrom::End(0));
            locked.0.stream_position()?
        };
        let mut locked = self.0.borrow_mut();
        idx_write_node(&mut locked.0, node)
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
        if n < 2 || n > NODE_SLOTS_MAX as usize {
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
}

/**
write initiale header to file.

## header map

The db index header size is 64 bytes.

```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 4     | signature1  | [b's', b'h', b'a', b'm']  |
| 4      | 4     | signature2  | [b'd', b'b', b'1', 0u8]   |
| 8      | 8     | count       | count of index            |
| 16     | 8     | top offset  | offset of top node        |
| 24     | 42    | reserve1    |                           |
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
    // count of data
    file.write_u64::<LittleEndian>(0u64)?;
    // root offset
    file.write_u64::<LittleEndian>(64u64)?;
    // reserve1
    let _ = file.write(&[0u8; 48]);
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
    // count of index
    let _count = file.read_u64::<LittleEndian>()?;
    if _count != 0 {
        //panic!("invalid count");
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
    let _ = file.seek(SeekFrom::Start(16))?;
    file.read_u64::<LittleEndian>()
}

fn idx_file_write_top_node_offset(file: &mut BufFile, offset: u64) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(16))?;
    file.write_u64::<LittleEndian>(offset)?;
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

/**
```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 8     | offset      | offset of this node       |
| 8      | 8*7   | keys        | offset of key-value       |
| 64     | 8*8   | downs       | offset of next node       |
+--------+-------+-------------+---------------------------+
```
*/
fn idx_write_node(file: &mut BufFile, node: IdxNode) -> Result<IdxNode> {
    _idx_write_node_with_buff(file, node)
    //_idx_write_node_direct(file, node)
}
fn _idx_write_node_with_buff(file: &mut BufFile, node: IdxNode) -> Result<IdxNode> {
    assert!(node.offset != 0);
    //
    let mut buff_cursor = Cursor::new(Vec::new());
    buff_cursor.write_u64::<LittleEndian>(node.offset)?;
    for i in 0..(NODE_SLOTS_MAX as usize - 1) {
        let val = if i < node.keys.len() { node.keys[i] } else { 0 };
        buff_cursor.write_u64::<LittleEndian>(val)?;
    }
    for i in 0..(NODE_SLOTS_MAX as usize) {
        let val = if i < node.downs.len() {
            node.downs[i]
        } else {
            0
        };
        buff_cursor.write_u64::<LittleEndian>(val)?;
    }
    //
    let _ = file.seek(SeekFrom::Start(node.offset))?;
    file.write_all(buff_cursor.get_ref())?;
    //
    Ok(node)
}
fn _idx_write_node_direct(file: &mut BufFile, node: IdxNode) -> Result<IdxNode> {
    assert!(node.offset != 0);
    //
    let _ = file.seek(SeekFrom::Start(node.offset))?;
    file.write_u64::<LittleEndian>(node.offset)?;
    for i in 0..(NODE_SLOTS_MAX as usize - 1) {
        let val = if i < node.keys.len() { node.keys[i] } else { 0 };
        file.write_u64::<LittleEndian>(val)?;
    }
    for i in 0..(NODE_SLOTS_MAX as usize) {
        let val = if i < node.downs.len() {
            node.downs[i]
        } else {
            0
        };
        file.write_u64::<LittleEndian>(val)?;
    }
    //
    Ok(node)
}
fn idx_read_node(file: &mut BufFile, offset: u64) -> Result<IdxNode> {
    _idx_read_node_with_buff(file, offset)
    //_idx_read_node_direct(file, offset)
}
fn _idx_read_node_with_buff(file: &mut BufFile, offset: u64) -> Result<IdxNode> {
    assert!(offset != 0);
    const BUFF_SZ: usize = NODE_SLOTS_MAX as usize * 2 * std::mem::size_of::<u64>();
    let mut buff = vec![0u8; BUFF_SZ];
    let _ = file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut buff)?;
    //
    let mut buff_cursor = Cursor::new(buff);
    let offset = buff_cursor.read_u64::<LittleEndian>()?;
    let mut node = IdxNode::new(offset);
    for _i in 0..(NODE_SLOTS_MAX as usize - 1) {
        let key_offset = buff_cursor
            .read_u64::<LittleEndian>()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        if key_offset == 0 {
            for _j in (_i + 1)..(NODE_SLOTS_MAX as usize - 1) {
                let _key_offset = buff_cursor
                    .read_u64::<LittleEndian>()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _j));
            }
            break;
        }
        node.keys.push(key_offset);
    }
    for _i in 0..=node.keys.len() {
        let node_offset = buff_cursor
            .read_u64::<LittleEndian>()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        node.downs.push(node_offset);
    }
    //
    Ok(node)
}
fn _idx_read_node_direct(file: &mut BufFile, offset: u64) -> Result<IdxNode> {
    assert!(offset != 0);
    let _ = file.seek(SeekFrom::Start(offset))?;
    let offset = file.read_u64::<LittleEndian>()?;
    let mut node = IdxNode::new(offset);
    for _i in 0..(NODE_SLOTS_MAX as usize - 1) {
        let key_offset = file
            .read_u64::<LittleEndian>()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        if key_offset == 0 {
            for _j in (_i + 1)..(NODE_SLOTS_MAX as usize - 1) {
                let _key_offset = file
                    .read_u64::<LittleEndian>()
                    .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _j));
            }
            break;
        }
        node.keys.push(key_offset);
    }
    for _i in 0..=node.keys.len() {
        let node_offset = file
            .read_u64::<LittleEndian>()
            .unwrap_or_else(|_| panic!("offset:{}, i:{}", offset, _i));
        node.downs.push(node_offset);
    }
    //
    Ok(node)
}

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
