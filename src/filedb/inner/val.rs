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
//const CHUNK_SIZE: u32 = 4 * 4 * 1024;
//const CHUNK_SIZE: u32 = 4 * 4 * 4 * 1024;
const CHUNK_SIZE: u32 = 128 * 1024;
const _DAT_HEADER_SZ: u64 = 192;
const DAT_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'V', 0u8];

use std::marker::PhantomData;

#[derive(Debug)]
struct VarFileValueCache(VarFile, PhantomData<i32>);

#[derive(Debug, Clone)]
pub struct ValueFile(Rc<RefCell<VarFileValueCache>>);

impl ValueFile {
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        sig2: HeaderSignature,
        params: &FileDbParams,
    ) -> Result<Self> {
        let piece_mgr = PieceMgr::new(&REC_SIZE_FREE_OFFSET, &REC_SIZE_ARY);
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.val", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = match params.val_buf_size {
            FileBufSizeParam::Size(val) => {
                let dat_buf_chunk_size = CHUNK_SIZE;
                let dat_buf_num_chunks = val / dat_buf_chunk_size;
                VarFile::with_capacity(
                    piece_mgr,
                    "val",
                    std_file,
                    dat_buf_chunk_size,
                    dat_buf_num_chunks.try_into().unwrap(),
                )?
            }
            FileBufSizeParam::PerMille(val) => {
                VarFile::with_per_mille(piece_mgr, "val", std_file, CHUNK_SIZE, val)?
            }
            FileBufSizeParam::Auto => VarFile::new(piece_mgr, "val", std_file)?,
        };
        let file_length: ValuePieceOffset = file.seek_to_end()?;
        if file_length.is_zero() {
            write_valrecf_init_header(&mut file, sig2)?;
        } else {
            check_valrecf_header(&mut file, sig2)?;
        }
        //
        let file_rc = VarFileValueCache(file, PhantomData);
        //
        Ok(Self(Rc::new(RefCell::new(file_rc))))
    }
    #[inline]
    pub fn read_fill_buffer(&self) -> Result<()> {
        let mut locked = RefCell::borrow_mut(&self.0);
        locked.0.read_fill_buffer()
    }
    #[inline]
    pub fn flush(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.0.flush()
    }
    #[inline]
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.0.sync_all()
    }
    #[inline]
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.0.sync_data()
    }
    #[cfg(feature = "buf_stats")]
    #[inline]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        let locked = self.0.borrow();
        locked.0.buf_stats()
    }
    //
    #[inline]
    pub(crate) fn read_piece_only_size(&self, offset: ValuePieceOffset) -> Result<ValuePieceSize> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_size(offset)
    }
    #[inline]
    pub fn read_piece_only_value_length(&self, offset: ValuePieceOffset) -> Result<ValueLength> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_value_length(offset)
    }
    #[inline]
    pub fn read_piece_only_value(&self, offset: ValuePieceOffset) -> Result<Vec<u8>> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_value(offset)
    }
    #[inline]
    pub fn read_piece(&self, offset: ValuePieceOffset) -> Result<ValuePiece> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece(offset)
    }
    #[inline]
    pub fn write_piece(&self, piece: ValuePiece) -> Result<ValuePiece> {
        let mut locked = self.0.borrow_mut();
        locked.write_piece(piece, false)
    }
    #[inline]
    pub fn delete_piece(&self, offset: ValuePieceOffset) -> Result<ValuePieceSize> {
        let mut locked = self.0.borrow_mut();
        locked.delete_piece(offset)
    }
    #[inline]
    pub fn add_value_piece(&self, value: &[u8]) -> Result<ValuePiece> {
        let mut locked = self.0.borrow_mut();
        locked.add_value_piece(value)
    }
}

// for debug
impl ValueFile {
    pub fn count_of_free_value_piece(&self) -> Result<Vec<(u32, u64)>> {
        let sz_ary = REC_SIZE_ARY;
        //
        let mut vec = Vec::new();
        let mut locked = self.0.borrow_mut();
        for piece_size in sz_ary {
            let cnt = locked
                .0
                .count_of_free_piece_list(ValuePieceSize::new(piece_size))?;
            vec.push((piece_size, cnt));
        }
        Ok(vec)
    }
}

/**
write initiale header to file.

## header map

The db data header size is 192 bytes.

```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 8     | signature1  | b"siamdbV\0"              |
| 8      | 8     | signature2  | 8 bytes type signature    |
| 16     | 8     | reserve0    |                           |
| 24     | 8     | reserve1    |                           |
| 32     | 8     | free1 off   | offset of free 1st list   |
| ...    | ...   | ...         | ...                       |
| 152    | 8     | free16 off  | offset of free 16th list  |
| 160    | 32    | reserve2    |                           |
+--------+-------+-------------+---------------------------+
```

- signature1: always fixed 8 bytes
- signature2: 8 bytes type signature

*/

fn write_valrecf_init_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    file.seek_from_start(ValuePieceOffset::new(0))?;
    // signature1
    file.write_all(&DAT_HEADER_SIGNATURE)?;
    // signature2
    file.write_all(&signature2)?;
    // reserve0
    file.write_u64_le(0)?;
    // reserve1
    file.write_u64_le(0)?;
    // free1 .. reserve2
    file.write_all(&[0u8; 160])?;
    //
    Ok(())
}

fn check_valrecf_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    file.seek_from_start(ValuePieceOffset::new(0))?;
    // signature1
    let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig1)?;
    assert!(sig1 == DAT_HEADER_SIGNATURE, "invalid header signature1");
    // signature2
    let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig2)?;
    assert!(
        sig2 == signature2,
        "invalid header signature2, type signature: {:?}",
        sig2
    );
    // reserve0
    let _reserve0 = file.read_u64_le()?;
    assert!(_reserve0 == 0, "invalid reserve0");
    //
    Ok(())
}

const REC_SIZE_FREE_OFFSET_1ST: u64 = 32;

const REC_SIZE_FREE_OFFSET: [u64; 16] = [
    REC_SIZE_FREE_OFFSET_1ST,
    REC_SIZE_FREE_OFFSET_1ST + 8,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 2,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 3,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 4,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 5,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 6,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 7,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 8,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 9,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 10,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 11,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 12,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 13,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 14,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 15,
];

pub(crate) const REC_SIZE_ARY: [u32; 16] = [
    8 * 2,
    8 * 3,
    8 * 4,
    8 * 6,
    8 * 8,
    8 * 10,
    8 * 12,
    8 * 14,
    8 * 8 * 2,
    8 * 8 * 4,
    8 * 8 * 6,
    8 * 8 * 8,
    8 * 8 * 10,
    8 * 8 * 12,
    8 * 8 * 14,
    8 * 8 * 8 * 2,
];
//    [8 * 2, 8 * 3, 8 * 4, 8 * 6, 8 * 8, 8 * 32, 8 * 64, 8 * 256];

impl ValuePieceSize {
    pub(crate) fn is_valid_value(&self) -> bool {
        let value_piece_size = self.as_value();
        assert!(
            value_piece_size > 0,
            "value_piece_size: {} > 0",
            value_piece_size
        );
        for &sz in &REC_SIZE_ARY {
            if sz == value_piece_size {
                return true;
            }
        }
        assert!(
            value_piece_size > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2],
            "value_piece_size: {} > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]: {}",
            value_piece_size,
            REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]
        );
        true
    }
}

#[derive(Debug, Default, Clone)]
pub struct ValuePiece {
    /// offset of ValuePiece in value-file.
    pub offset: ValuePieceOffset,
    /// size in bytes of ValuePiece in value-file.
    pub size: ValuePieceSize,
    /// value data.
    pub value: Vec<u8>,
}

impl ValuePiece {
    #[inline]
    pub fn with(offset: ValuePieceOffset, size: ValuePieceSize, value: Vec<u8>) -> Self {
        Self {
            offset,
            size,
            value,
        }
    }
    #[inline]
    pub fn with_value(value: &[u8]) -> Self {
        Self {
            value: value.to_vec(),
            ..Default::default()
        }
    }
    //
    fn encoded_piece_size(&self) -> (u32, u32, ValueLength) {
        #[cfg(feature = "siamese_debug")]
        let value_len = ValueLength::new(self.value.len().try_into().unwrap());
        #[cfg(not(feature = "siamese_debug"))]
        let value_len = ValueLength::new(self.value.len() as u32);
        //
        #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
        let (encorded_piece_len, piece_len) = {
            let enc_val_len = 4;
            let piece_len: u32 = enc_val_len + value_len.as_value();
            let encorded_piece_len = 4;
            (encorded_piece_len, piece_len)
        };
        #[cfg(feature = "vf_vu64")]
        let (encorded_piece_len, piece_len) = {
            let enc_val_len = vu64::encoded_len(value_len.as_value() as u64) as u32;
            let piece_len: u32 = enc_val_len + value_len.as_value();
            let encorded_piece_len = vu64::encoded_len((piece_len as u64 + 7) / 8) as u32;
            (encorded_piece_len, piece_len)
        };
        //
        (encorded_piece_len, piece_len, value_len)
    }
    //
    pub(crate) fn dat_write_piece_one(&self, file: &mut VarFile) -> Result<()> {
        assert!(!self.size.is_zero());
        //
        let value = &self.value;
        #[cfg(feature = "siamese_debug")]
        let value_len = ValueLength::new(value.len().try_into().unwrap());
        #[cfg(not(feature = "siamese_debug"))]
        let value_len = ValueLength::new(value.len() as u32);
        //
        file.seek_from_start(self.offset)?;
        file.write_piece_size(self.size)?;
        file.write_value_len(value_len)?;
        file.write_all_small(value)?;
        file.write_zero_to_offset(self.offset + self.size)?;
        //
        Ok(())
    }
}

impl VarFileValueCache {
    fn delete_piece(&mut self, offset: ValuePieceOffset) -> Result<ValuePieceSize> {
        let old_piece_size = {
            self.0.seek_from_start(offset)?;
            self.0.read_piece_size()?
        };
        //
        self.0.push_free_piece_list(offset, old_piece_size)?;
        Ok(old_piece_size)
    }

    #[inline]
    fn add_value_piece(&mut self, value: &[u8]) -> Result<ValuePiece> {
        self.write_piece(ValuePiece::with_value(value), true)
    }

    fn write_piece(&mut self, mut piece: ValuePiece, is_new: bool) -> Result<ValuePiece> {
        debug_assert!(is_new || !piece.offset.is_zero());
        //
        let (encorded_piece_len, piece_len, _value_len) = piece.encoded_piece_size();
        let new_piece_size = self
            .0
            .piece_mgr
            .roundup(ValuePieceSize::new(encorded_piece_len + piece_len));
        //
        if !is_new {
            let old_piece_size = {
                self.0.seek_from_start(piece.offset)?;
                self.0.read_piece_size()?
            };
            debug_assert!(old_piece_size.is_valid_value());
            if new_piece_size <= old_piece_size {
                // over writes.
                self.0.seek_from_start(piece.offset)?;
                piece.size = old_piece_size;
                piece.dat_write_piece_one(&mut self.0)?;
                return Ok(piece);
            } else {
                // delete old and add new
                // old
                self.0.push_free_piece_list(piece.offset, old_piece_size)?;
            }
        }
        // add new.
        {
            let free_piece_offset = self.0.pop_free_piece_list(new_piece_size)?;
            let new_piece_offset = if !free_piece_offset.is_zero() {
                self.0.seek_from_start(free_piece_offset)?;
                free_piece_offset
            } else {
                self.0.seek_to_end()?
            };
            piece.offset = new_piece_offset;
            piece.size = new_piece_size;
            debug_assert!(piece.size.is_valid_value());
            match piece.dat_write_piece_one(&mut self.0) {
                Ok(()) => (),
                Err(err) => {
                    // recover on error
                    let _ = self.0.set_file_length(new_piece_offset);
                    return Err(err);
                }
            }
            piece.offset = new_piece_offset;
        }
        //
        Ok(piece)
    }

    fn read_piece(&mut self, offset: ValuePieceOffset) -> Result<ValuePiece> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_from_start(offset)?;
        let piece_size = self.0.read_piece_size()?;
        debug_assert!(piece_size.is_valid_value());
        //
        let val_len = self.0.read_value_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(val_len.into())?;
        let value = maybe_slice.to_vec();
        //
        let piece = ValuePiece::with(offset, piece_size, value);
        //
        Ok(piece)
    }

    #[inline]
    fn read_piece_only_size(&mut self, offset: ValuePieceOffset) -> Result<ValuePieceSize> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_from_start(offset)?;
        let piece_size = self.0.read_piece_size()?;
        //
        Ok(piece_size)
    }

    #[inline]
    fn read_piece_only_value_length(&mut self, offset: ValuePieceOffset) -> Result<ValueLength> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_piece_value(offset)?;
        let val_len = self.0.read_value_len()?;
        //
        Ok(val_len)
    }

    #[inline]
    fn read_piece_only_value(&mut self, offset: ValuePieceOffset) -> Result<Vec<u8>> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_piece_value(offset)?;
        //
        let val_len = self.0.read_value_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(val_len.into())?;
        let value = maybe_slice.into_vec();
        //
        Ok(value)
    }
}

/*
```text
used piece:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | piece size  | size in bytes of this piece: u32  |
| --     | 1..5  | val len     | a byte length of value            |
| --     | --    | val data    | raw value data                    |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
/*
```text
free piece:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | piece size  | size in bytes of this piece: u32  |
| --     | 1     | val len     | always zero                       |
| --     | 8     | next        | next free piece offset            |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
