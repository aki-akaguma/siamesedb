use super::super::super::DbMapKeyType;
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
const DAT_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'K', 0u8];

use std::marker::PhantomData;

#[derive(Debug)]
pub struct VarFileKeyCache<KT: DbMapKeyType>(pub VarFile, PhantomData<KT>);

#[derive(Debug, Clone)]
pub struct KeyFile<KT: DbMapKeyType>(pub Rc<RefCell<VarFileKeyCache<KT>>>);

impl<KT: DbMapKeyType> KeyFile<KT> {
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        sig2: HeaderSignature,
        params: &FileDbParams,
    ) -> Result<Self> {
        let piece_mgr = PieceMgr::new(&REC_SIZE_FREE_OFFSET, &REC_SIZE_ARY);
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.key", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = match params.key_buf_size {
            FileBufSizeParam::Size(val) => {
                let dat_buf_chunk_size = CHUNK_SIZE;
                let dat_buf_num_chunks = val / dat_buf_chunk_size;
                VarFile::with_capacity(
                    piece_mgr,
                    "key",
                    std_file,
                    dat_buf_chunk_size,
                    dat_buf_num_chunks.try_into().unwrap(),
                )?
            }
            FileBufSizeParam::PerMille(val) => {
                VarFile::with_per_mille(piece_mgr, "key", std_file, CHUNK_SIZE, val)?
            }
            FileBufSizeParam::Auto => VarFile::new(piece_mgr, "key", std_file)?,
        };
        let file_length: KeyPieceOffset = file.seek_to_end()?;
        if file_length.is_zero() {
            write_keyrecf_init_header(&mut file, sig2)?;
        } else {
            check_keyrecf_header(&mut file, sig2)?;
        }
        //
        let file_rc = VarFileKeyCache(file, PhantomData);
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
    pub(crate) fn read_piece_only_size(&self, offset: KeyPieceOffset) -> Result<KeyPieceSize> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_size(offset)
    }
    #[inline]
    pub fn read_piece_only_key_length(&self, offset: KeyPieceOffset) -> Result<KeyLength> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_key_length(offset)
    }
    #[inline]
    pub fn read_piece_only_key(&self, offset: KeyPieceOffset) -> Result<KT> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_key(offset)
    }
    #[inline]
    pub fn read_piece_only_value_offset(&self, offset: KeyPieceOffset) -> Result<ValuePieceOffset> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece_only_value_offset(offset)
    }
    #[inline]
    pub fn read_piece(&self, offset: KeyPieceOffset) -> Result<KeyPiece<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.read_piece(offset)
    }
    #[inline]
    pub fn write_piece(&self, piece: KeyPiece<KT>) -> Result<KeyPiece<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.write_piece(piece, false)
    }
    #[inline]
    pub fn delete_piece(&self, offset: KeyPieceOffset) -> Result<KeyPieceSize> {
        let mut locked = self.0.borrow_mut();
        locked.delete_piece(offset)
    }
    #[inline]
    pub fn add_key_piece(&self, key: &KT, value_offset: ValuePieceOffset) -> Result<KeyPiece<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.add_key_piece(key, value_offset)
    }
}

// for debug
impl<KT: DbMapKeyType> KeyFile<KT> {
    pub fn count_of_free_key_piece(&self) -> Result<Vec<(u32, u64)>> {
        let sz_ary = REC_SIZE_ARY;
        //
        let mut vec = Vec::new();
        let mut locked = self.0.borrow_mut();
        for piece_size in sz_ary {
            let cnt = locked
                .0
                .count_of_free_piece_list(KeyPieceSize::new(piece_size))?;
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
| 0      | 8     | signature1  | b"siamdbK\0"              |
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

fn write_keyrecf_init_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    file.seek_from_start(KeyPieceOffset::new(0))?;
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

fn check_keyrecf_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    file.seek_from_start(KeyPieceOffset::new(0))?;
    // signature1
    let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    file.read_exact(&mut sig1)?;
    assert!(sig1 == DAT_HEADER_SIGNATURE, "invalid header signature1");
    // signature2
    let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    file.read_exact(&mut sig2)?;
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

impl KeyPieceSize {
    pub(crate) fn is_valid_key(&self) -> bool {
        let piece_size = self.as_value();
        assert!(piece_size > 0, "piece_size: {} > 0", piece_size);
        for &sz in &REC_SIZE_ARY {
            if sz == piece_size {
                return true;
            }
        }
        assert!(
            piece_size > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2],
            "piece_size: {} > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]: {}",
            piece_size,
            REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]
        );
        true
    }
}

#[derive(Debug, Default, Clone)]
pub struct KeyPiece<KT: DbMapKeyType> {
    /// offset of KeyPiece in key-file.
    pub offset: KeyPieceOffset,
    /// size in bytes of KeyPiece in key-file.
    pub size: KeyPieceSize,
    /// key data.
    pub key: KT,
    /// value offset.
    pub value_offset: ValuePieceOffset,
}

impl<KT: DbMapKeyType> KeyPiece<KT> {
    #[inline]
    pub fn with(
        offset: KeyPieceOffset,
        size: KeyPieceSize,
        key: KT,
        value_offset: ValuePieceOffset,
    ) -> Self {
        Self {
            offset,
            size,
            key,
            value_offset,
        }
    }
    #[inline]
    pub fn with_key_value(key: KT, value_offset: ValuePieceOffset) -> Self {
        Self {
            key,
            value_offset,
            ..Default::default()
        }
    }
    #[cfg(feature = "htx")]
    pub fn hash_value(&self) -> u64 {
        self.key.hash_value()
    }
    //
    fn encoded_piece_size(&self) -> (u32, u32, KeyLength) {
        let key = self.key.as_bytes();
        #[cfg(feature = "siamese_debug")]
        let key_len = KeyLength::new(key.len().try_into().unwrap());
        #[cfg(not(feature = "siamese_debug"))]
        let key_len = KeyLength::new(key.len() as u32);
        //
        #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
        let (encorded_piece_len, piece_len) = {
            let enc_key_len = 4;
            #[cfg(feature = "vf_u32u32")]
            let enc_val_off = 4;
            #[cfg(feature = "vf_u64u64")]
            let enc_val_off = 8;
            //
            let piece_len: u32 = enc_key_len + key_len.as_value() + enc_val_off;
            //
            let encorded_piece_len = 4;
            (encorded_piece_len, piece_len)
        };
        #[cfg(feature = "vf_vu64")]
        let (encorded_piece_len, piece_len) = {
            let enc_key_len = vu64::encoded_len(key_len.as_value() as u64) as u32;
            //
            #[cfg(any(feature = "htx", feature = "idx_straight"))]
            let enc_val_off = 8;
            #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
            let enc_val_off = vu64::encoded_len(self.value_offset.as_value() as u64) as u32;
            //
            let piece_len: u32 = enc_key_len + key_len.as_value() + enc_val_off;
            //
            let encorded_piece_len = vu64::encoded_len((piece_len as u64 + 7) / 8) as u32;
            (encorded_piece_len, piece_len)
        };
        //
        (encorded_piece_len, piece_len, key_len)
    }
    //
    pub(crate) fn dat_write_piece_one(&self, file: &mut VarFile) -> Result<()> {
        assert!(!self.size.is_zero());
        //
        let key = self.key.as_bytes();
        #[cfg(feature = "siamese_debug")]
        let key_len = KeyLength::new(key.len().try_into().unwrap());
        #[cfg(not(feature = "siamese_debug"))]
        let key_len = KeyLength::new(key.len() as u32);
        //
        file.seek_from_start(self.offset)?;
        file.write_piece_size(self.size)?;
        file.write_key_len(key_len)?;
        file.write_all_small(key)?;
        //
        #[cfg(any(feature = "htx", feature = "idx_straight"))]
        file.write_value_piece_offset(self.value_offset)?;
        #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
        file.write_piece_offset(self.value_offset)?;
        //
        file.write_zero_to_offset(self.offset + self.size)?;
        //
        Ok(())
    }
}

impl<KT: DbMapKeyType> VarFileKeyCache<KT> {
    fn delete_piece(&mut self, offset: KeyPieceOffset) -> Result<KeyPieceSize> {
        let old_piece_size = {
            self.0.seek_from_start(offset)?;
            self.0.read_piece_size()?
        };
        //
        self.0.push_free_piece_list(offset, old_piece_size)?;
        Ok(old_piece_size)
    }

    #[inline]
    fn add_key_piece(&mut self, key: &KT, value_offset: ValuePieceOffset) -> Result<KeyPiece<KT>> {
        self.write_piece(KeyPiece::with_key_value(key.clone(), value_offset), true)
    }

    fn write_piece(&mut self, mut piece: KeyPiece<KT>, is_new: bool) -> Result<KeyPiece<KT>> {
        debug_assert!(is_new || !piece.offset.is_zero());
        //
        let (encorded_piece_len, piece_len, _key_len) = piece.encoded_piece_size();
        let new_piece_size = self
            .0
            .piece_mgr
            .roundup(KeyPieceSize::new(encorded_piece_len + piece_len));
        //
        if !is_new {
            let old_piece_size = {
                self.0.seek_from_start(piece.offset)?;
                self.0.read_piece_size()?
            };
            debug_assert!(old_piece_size.is_valid_key());
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
            debug_assert!(piece.size.is_valid_key());
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

    #[inline]
    fn read_piece(&mut self, offset: KeyPieceOffset) -> Result<KeyPiece<KT>> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_from_start(offset)?;
        let piece_size = self.0.read_piece_size()?;
        debug_assert!(piece_size.is_valid_key());
        let key_len = self.0.read_key_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(key_len.into())?;
        let key = KT::from_bytes(&maybe_slice);
        //
        #[cfg(any(feature = "htx", feature = "idx_straight"))]
        let val_offset = self.0.read_value_piece_offset()?;
        #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
        let val_offset = self.0.read_piece_offset()?;
        //
        let piece = KeyPiece::with(offset, piece_size, key, val_offset);
        //
        Ok(piece)
    }

    #[inline]
    fn read_piece_only_size(&mut self, offset: KeyPieceOffset) -> Result<KeyPieceSize> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_from_start(offset)?;
        let piece_size = self.0.read_piece_size()?;
        Ok(piece_size)
    }

    #[inline]
    fn read_piece_only_key_length(&mut self, offset: KeyPieceOffset) -> Result<KeyLength> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_piece_key(offset)?;
        let key_len = self.0.read_key_len()?;
        Ok(key_len)
    }

    #[inline]
    pub fn read_piece_only_key_maybeslice(
        &mut self,
        offset: KeyPieceOffset,
    ) -> Result<rabuf::MaybeSlice> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_piece_key(offset)?;
        let key_len = self.0.read_key_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(key_len.into())?;
        Ok(maybe_slice)
    }

    #[inline]
    fn read_piece_only_key(&mut self, offset: KeyPieceOffset) -> Result<KT> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_piece_key(offset)?;
        let key_len = self.0.read_key_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(key_len.into())?;
        Ok(KT::from_bytes(&maybe_slice))
    }

    #[inline]
    fn read_piece_only_value_offset(&mut self, offset: KeyPieceOffset) -> Result<ValuePieceOffset> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_piece_key(offset)?;
        let key_len = self.0.read_key_len()?;
        self.0.seek_skip_length(key_len)?;
        //
        #[cfg(any(feature = "htx", feature = "idx_straight"))]
        let value_offset = self.0.read_value_piece_offset()?;
        #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
        let value_offset = self.0.read_piece_offset()?;
        //
        Ok(value_offset)
    }
}

/*
```text
used piece:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | piece size  | size in bytes of this piece: u32  |
| --     | 1..5  | key len     | a byte length of key              |
| --     | --    | key data    | raw key data                      |
| --     | 8     | val offset  | value piece offset: u64           |
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
| --     | 1     | key len     | always zero                       |
| --     | 8     | next        | next free piece offset            |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
