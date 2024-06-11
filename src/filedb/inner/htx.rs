use super::super::{FileBufSizeParam, FileDbParams};
use super::piece::PieceMgr;
use super::semtype::*;
use super::vfile::VarFile;
use rabuf::{SmallRead, SmallWrite};
use std::cell::RefCell;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Result, Write};
use std::path::Path;
use std::rc::Rc;

type HeaderSignature = [u8; 8];

//const CHUNK_SIZE: u32 = 4 * 1024;
const CHUNK_SIZE: u32 = 128 * 1024;
//const CHUNK_SIZE: u32 = 1024 * 1024;
const HTX_HEADER_SZ: u64 = 128;
const HTX_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'H', 0u8];
const DEFAULT_HT_SIZE: u64 = 16 * 1024 * 1024;

#[cfg(not(feature = "htx_print_hits"))]
#[derive(Debug)]
pub struct VarFileHtxCache(pub VarFile, u64);

#[cfg(feature = "htx_print_hits")]
#[derive(Debug)]
pub struct VarFileHtxCache(pub VarFile, u64, u64, u64);

#[derive(Debug, Clone)]
pub struct HtxFile(pub Rc<RefCell<VarFileHtxCache>>);

const HTX_SIZE_FREE_OFFSET: [u64; 0] = [];
const HTX_SIZE_ARY: [u32; 0] = [];

impl HtxFile {
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        sig2: HeaderSignature,
        params: &FileDbParams,
    ) -> Result<Self> {
        let piece_mgr = PieceMgr::new(&HTX_SIZE_FREE_OFFSET, &HTX_SIZE_ARY);
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.htx", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(pb)?;
        let mut file = match params.htx_buf_size {
            FileBufSizeParam::Size(val) => {
                let idx_buf_chunk_size = CHUNK_SIZE;
                let idx_buf_num_chunks = val / idx_buf_chunk_size;
                VarFile::with_capacity(
                    piece_mgr,
                    "htx",
                    std_file,
                    idx_buf_chunk_size,
                    idx_buf_num_chunks.try_into().unwrap(),
                )?
            }
            FileBufSizeParam::PerMille(val) => {
                VarFile::with_per_mille(piece_mgr, "htx", std_file, CHUNK_SIZE, val)?
            }
            FileBufSizeParam::Auto => VarFile::new(piece_mgr, "htx", std_file)?,
        };
        let file_length: NodePieceOffset = file.seek_to_end()?;
        //
        #[cfg(not(feature = "htx_print_hits"))]
        let mut file_nc = VarFileHtxCache(file, 0);
        #[cfg(feature = "htx_print_hits")]
        let mut file_nc = VarFileHtxCache(file, 0, 0, 0);
        //
        if file_length.is_zero() {
            write_htxf_init_header(&mut file_nc.0, sig2)?;
            let off = NodePieceOffset::new(HTX_HEADER_SZ + 8 * DEFAULT_HT_SIZE);
            file_nc.0.set_file_length(off)?;
            let off = NodePieceOffset::new(HTX_HEADER_SZ + 8 * DEFAULT_HT_SIZE - 8);
            file_nc.0.seek_from_start(off)?;
            file_nc.0.write_u64_le(0)?;
            file_nc.1 = DEFAULT_HT_SIZE;
        } else {
            check_htxf_header(&mut file_nc.0, sig2)?;
            let ht_size = file_nc.0.read_hash_table_size()?;
            file_nc.1 = ht_size;
        }
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
        locked.0.flush()
    }
    #[inline]
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.0.sync_all()
    }
    #[inline]
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
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
    pub fn read_key_piece_offset(&self, hash: u64) -> Result<KeyPieceOffset> {
        let mut locked = RefCell::borrow_mut(&self.0);
        //let ht_size = locked.0.read_hash_table_size()?;
        let ht_size = locked.1;
        let idx = hash % ht_size;
        locked.0.read_key_piece_offset(idx)
    }
    #[inline]
    pub fn write_key_piece_offset(&self, hash: u64, offset: KeyPieceOffset) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        //let ht_size = locked.0.read_hash_table_size()?;
        let ht_size = locked.1;
        let idx = hash % ht_size;
        locked.0.write_key_piece_offset(idx, offset)
    }
    #[cfg(feature = "htx_print_hits")]
    pub fn set_hits(&mut self) {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.2 += 1;
    }
    #[cfg(feature = "htx_print_hits")]
    pub fn set_miss(&mut self) {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.3 += 1;
    }
}

#[cfg(feature = "htx_print_hits")]
impl Drop for HtxFile {
    fn drop(&mut self) {
        let (hits, miss) = {
            let locked = RefCell::borrow_mut(&self.0);
            (locked.2, locked.3)
        };
        let total = hits + miss;
        let ratio = hits as f64 / total as f64;
        eprintln!("htx hits: {}/{} [{:.2}%]", hits, total, 100.0 * ratio);
    }
}

// for debug
impl HtxFile {
    pub fn ht_size_and_count(&self) -> Result<(u64, u64)> {
        let mut locked = RefCell::borrow_mut(&self.0);
        let ht_size = locked.0.read_hash_table_size()?;
        let count = locked.0.read_item_count()?;
        Ok((ht_size, count))
    }
    pub fn htx_filling_rate_per_mill(&self) -> Result<(u64, u32)> {
        let mut locked = RefCell::borrow_mut(&self.0);
        let ht_size = locked.1;
        let mut count = 0;
        for idx in 0..ht_size {
            let offset = locked.0.read_key_piece_offset(idx)?;
            if !offset.is_zero() {
                count += 1;
            }
        }
        Ok((count, (count * 1000 / ht_size) as u32))
    }
}

/**
write initiale header to file.

## header map

The htx header size is 128 bytes.

```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 8     | signature1  | b"siamdbH\0"              |
| 8      | 8     | signature2  | 8 bytes type signature    |
| 16     | 8     | ht size     | hash table size           |
| 24     | 8     | count       | count of items            |
| 32     | 96    | reserve1    |                           |
+--------+-------+-------------+---------------------------+
```

- signature1: always fixed 8 bytes
- signature2: 8 bytes type signature

*/
const HTX_HT_SIZE_OFFSET: u64 = 16;
const HTX_ITEM_COUNT_OFFSET: u64 = 24;

fn write_htxf_init_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    file.seek_from_start(NodePieceOffset::new(0))?;
    // signature1
    file.write_all(&HTX_HEADER_SIGNATURE)?;
    // signature2
    file.write_all(&signature2)?;
    // ht size
    file.write_u64_le(DEFAULT_HT_SIZE)?;
    // count .. rserve1
    file.write_all(&[0u8; 104])?;
    //
    Ok(())
}

fn check_htxf_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    file.seek_from_start(NodePieceOffset::new(0))?;
    // signature1
    let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    file.read_exact(&mut sig1)?;
    assert!(sig1 == HTX_HEADER_SIGNATURE, "invalid header signature1");
    // signature2
    let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    file.read_exact(&mut sig2)?;
    assert!(
        sig2 == signature2,
        "invalid header signature2, type signature: {:?}",
        sig2
    );
    // top node offset
    let _top_node_offset = file.read_u64_le()?;
    assert!(_top_node_offset != 0, "invalid root offset");
    //
    Ok(())
}

impl VarFile {
    fn read_hash_table_size(&mut self) -> Result<u64> {
        self.seek_from_start(NodePieceOffset::new(HTX_HT_SIZE_OFFSET))?;
        self.read_u64_le()
    }
    fn _write_hash_table_size(&mut self, val: u64) -> Result<()> {
        self.seek_from_start(NodePieceOffset::new(HTX_HT_SIZE_OFFSET))?;
        self.write_u64_le(val)
    }
    fn read_item_count(&mut self) -> Result<u64> {
        self.seek_from_start(NodePieceOffset::new(HTX_ITEM_COUNT_OFFSET))?;
        self.read_u64_le()
    }
    fn _write_item_count(&mut self, val: u64) -> Result<()> {
        self.seek_from_start(NodePieceOffset::new(HTX_ITEM_COUNT_OFFSET))?;
        self.write_u64_le(val)
    }
    fn read_key_piece_offset(&mut self, idx: u64) -> Result<KeyPieceOffset> {
        self.seek_from_start(NodePieceOffset::new(HTX_HEADER_SZ + 8 * idx))?;
        self.read_u64_le().map(KeyPieceOffset::new)
    }
    fn write_key_piece_offset(&mut self, idx: u64, offset: KeyPieceOffset) -> Result<()> {
        /*<CHECK>
        let count = self.read_item_count()?;
        if offset.is_zero() {
            if count > 0 {
                self.write_item_count(count - 1)?;
            }
        } else {
            self.write_item_count(count + 1)?;
        }
        */
        self.seek_from_start(NodePieceOffset::new(HTX_HEADER_SZ + 8 * idx))?;
        self.write_u64_le(offset.into())?;
        Ok(())
    }
}

/*
```text
hash table:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 8     | key offset  | offset of key piece               |
| 8      | 8     | key offset  | offset of key piece               |
| --     | --    | --          | --                                |
| --     | 8     | key offset  | offset of key piece               |
+--------+-------+-------------+-----------------------------------+
```
*/
