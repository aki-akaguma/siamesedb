use super::super::super::DbXxxKeyType;
use super::super::{FileBufSizeParam, FileDbParams};
use super::semtype::*;
use super::vfile::VarFile;
use rabuf::SmallRead;
use std::cell::RefCell;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Result, Write};
use std::path::Path;
use std::rc::Rc;

type HeaderSignature = [u8; 8];

//const CHUNK_SIZE: u32 = 4 * 1024;
const CHUNK_SIZE: u32 = 4 * 4 * 1024;
const _DAT_HEADER_SZ: u64 = 192;
const DAT_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'K', 0u8];

use std::marker::PhantomData;

#[derive(Debug)]
pub struct VarFileKeyCache<KT: DbXxxKeyType>(pub VarFile, PhantomData<KT>);

#[derive(Debug, Clone)]
pub struct KeyFile<KT: DbXxxKeyType>(pub Rc<RefCell<VarFileKeyCache<KT>>>);

impl<KT: DbXxxKeyType> KeyFile<KT> {
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        sig2: HeaderSignature,
        params: &FileDbParams,
    ) -> Result<Self> {
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
                    &REC_SIZE_FREE_OFFSET,
                    &REC_SIZE_ARY,
                    "key",
                    std_file,
                    dat_buf_chunk_size,
                    dat_buf_num_chunks.try_into().unwrap(),
                )?
            }
            FileBufSizeParam::PerMille(val) => VarFile::with_per_mille(
                &REC_SIZE_FREE_OFFSET,
                &REC_SIZE_ARY,
                "key",
                std_file,
                CHUNK_SIZE,
                val,
            )?,
            FileBufSizeParam::Auto => {
                VarFile::new(&REC_SIZE_FREE_OFFSET, &REC_SIZE_ARY, "key", std_file)?
            }
        };
        let file_length: KeyRecordOffset = file.seek_to_end()?;
        if file_length.is_zero() {
            file.write_keyrecf_init_header(sig2)?;
        } else {
            file.check_keyrecf_header(sig2)?;
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
    pub(crate) fn read_record_only_size(&self, offset: KeyRecordOffset) -> Result<KeyRecordSize> {
        let mut locked = self.0.borrow_mut();
        locked.read_record_only_size(offset)
    }
    #[inline]
    pub fn read_record_only_key_length(&self, offset: KeyRecordOffset) -> Result<KeyLength> {
        let mut locked = self.0.borrow_mut();
        locked.read_record_only_key_length(offset)
    }
    #[inline]
    pub fn read_record_only_key(&self, offset: KeyRecordOffset) -> Result<KT> {
        let mut locked = self.0.borrow_mut();
        locked.read_record_only_key(offset)
    }
    #[inline]
    pub fn read_record_only_value_offset(
        &self,
        offset: KeyRecordOffset,
    ) -> Result<ValueRecordOffset> {
        let mut locked = self.0.borrow_mut();
        locked.read_record_only_value_offset(offset)
    }
    #[inline]
    pub fn read_record(&self, offset: KeyRecordOffset) -> Result<KeyRecord<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.read_record(offset)
    }
    #[inline]
    pub fn write_record(&self, record: KeyRecord<KT>) -> Result<KeyRecord<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.write_record(record, false)
    }
    #[inline]
    pub fn delete_record(&self, offset: KeyRecordOffset) -> Result<KeyRecordSize> {
        let mut locked = self.0.borrow_mut();
        locked.delete_record(offset)
    }
    #[inline]
    pub fn _add_key_record(
        &self,
        key: &KT,
        value_offset: ValueRecordOffset,
    ) -> Result<KeyRecord<KT>> {
        let mut locked = self.0.borrow_mut();
        locked._add_key_record(key, value_offset)
    }
    #[inline]
    pub fn add_key_record_with_slice(
        &self,
        key_slice: &[u8],
        value_offset: ValueRecordOffset,
    ) -> Result<KeyRecord<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.add_key_record_with_slice(key_slice, value_offset)
    }
}

// for debug
impl<KT: DbXxxKeyType> KeyFile<KT> {
    pub fn count_of_free_key_record(&self) -> Result<Vec<(u32, u64)>> {
        let sz_ary = REC_SIZE_ARY;
        //
        let mut vec = Vec::new();
        let mut locked = self.0.borrow_mut();
        for record_size in sz_ary {
            let cnt = locked
                .0
                .count_of_free_piece_list(KeyRecordSize::new(record_size))?;
            vec.push((record_size, cnt));
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

impl VarFile {
    fn write_keyrecf_init_header(&mut self, signature2: HeaderSignature) -> Result<()> {
        self.seek_from_start(KeyRecordOffset::new(0))?;
        // signature1
        self.write_all(&DAT_HEADER_SIGNATURE)?;
        // signature2
        self.write_all(&signature2)?;
        // reserve0
        self.write_u64_le(0)?;
        // reserve1
        self.write_u64_le(0)?;
        // free1 .. reserve2
        self.write_all(&[0u8; 160])?;
        //
        Ok(())
    }
    fn check_keyrecf_header(&mut self, signature2: HeaderSignature) -> Result<()> {
        self.seek_from_start(KeyRecordOffset::new(0))?;
        // signature1
        let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let _sz = self.read_exact(&mut sig1)?;
        assert!(sig1 == DAT_HEADER_SIGNATURE, "invalid header signature1");
        // signature2
        let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let _sz = self.read_exact(&mut sig2)?;
        assert!(
            sig2 == signature2,
            "invalid header signature2, type signature: {:?}",
            sig2
        );
        // reserve0
        let _reserve0 = self.read_u64_le()?;
        assert!(_reserve0 == 0, "invalid reserve0");
        //
        Ok(())
    }
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

impl KeyRecordSize {
    pub(crate) fn is_valid_key(&self) -> bool {
        let record_size = self.as_value();
        assert!(record_size > 0, "record_size: {} > 0", record_size);
        for &sz in &REC_SIZE_ARY {
            if sz == record_size {
                return true;
            }
        }
        assert!(
            record_size > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2],
            "record_size: {} > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]: {}",
            record_size,
            REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]
        );
        true
    }
}

#[derive(Debug, Default, Clone)]
pub struct KeyRecord<KT: DbXxxKeyType> {
    /// offset of IdxNode in dat file.
    pub offset: KeyRecordOffset,
    /// size in bytes of KeyRecord in dat file.
    pub size: KeyRecordSize,
    /// key data.
    pub key: KT,
    /// value offset.
    pub value_offset: ValueRecordOffset,
}

impl<KT: DbXxxKeyType> KeyRecord<KT> {
    #[inline]
    pub fn with(
        offset: KeyRecordOffset,
        size: KeyRecordSize,
        key: KT,
        value_offset: ValueRecordOffset,
    ) -> Self {
        Self {
            offset,
            size,
            key,
            value_offset,
        }
    }
    #[inline]
    pub fn _with_key_value(key: KT, value_offset: ValueRecordOffset) -> Self {
        Self {
            key,
            value_offset,
            ..Default::default()
        }
    }
    #[inline]
    pub fn with_key_slice_value(key_slice: &[u8], value_offset: ValueRecordOffset) -> Self {
        let key = KT::from(key_slice);
        Self {
            key,
            value_offset,
            ..Default::default()
        }
    }
    #[cfg(feature = "htx")]
    pub fn hash_value(&self) -> u64 {
        use std::hash::Hasher;
        //
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.key.hash(&mut hasher);
        hasher.finish()
    }
    //
    fn encoded_record_size(&self) -> (u32, u32, KeyLength) {
        let key_len = KeyLength::new(self.key.byte_len() as u32);
        //
        #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
        let (encorded_record_len, record_len) = {
            let enc_key_len = 4;
            #[cfg(feature = "vf_u32u32")]
            let enc_val_off = 4;
            #[cfg(feature = "vf_u64u64")]
            let enc_val_off = 8;
            let record_len: u32 = enc_key_len + key_len.as_value() + enc_val_off;
            let encorded_record_len = 4;
            (encorded_record_len, record_len)
        };
        #[cfg(feature = "vf_vu64")]
        let (encorded_record_len, record_len) = {
            let enc_key_len = vu64::encoded_len(key_len.as_value() as u64) as u32;
            #[cfg(feature = "htx")]
            let enc_val_off = 8;
            #[cfg(not(feature = "htx"))]
            let enc_val_off = vu64::encoded_len(self.value_offset.as_value() as u64) as u32;
            let record_len: u32 = enc_key_len + key_len.as_value() + enc_val_off;
            let encorded_record_len = vu64::encoded_len((record_len as u64 + 7) / 8) as u32;
            (encorded_record_len, record_len)
        };
        //
        (encorded_record_len, record_len, key_len)
    }
    //
    pub(crate) fn dat_write_record_one(&self, file: &mut VarFile) -> Result<()> {
        assert!(!self.size.is_zero());
        //
        let key = self.key.as_bytes();
        let key_len = KeyLength::new(key.len().try_into().unwrap());
        //
        file.seek_from_start(self.offset)?;
        file.write_record_size(self.size)?;
        file.write_key_len(key_len)?;
        file.write_all(key)?;
        #[cfg(feature = "htx")]
        file.write_value_record_offset(self.value_offset)?;
        #[cfg(not(feature = "htx"))]
        file.write_record_offset(self.value_offset)?;
        file.write_zero_to_offset(self.offset + self.size)?;
        //
        Ok(())
    }
}

impl<KT: DbXxxKeyType> VarFileKeyCache<KT> {
    fn delete_record(&mut self, offset: KeyRecordOffset) -> Result<KeyRecordSize> {
        let old_record_size = {
            self.0.seek_from_start(offset)?;
            self.0.read_record_size()?
        };
        //
        self.0.push_free_piece_list(offset, old_record_size)?;
        Ok(old_record_size)
    }

    #[inline]
    fn _add_key_record(
        &mut self,
        key: &KT,
        value_offset: ValueRecordOffset,
    ) -> Result<KeyRecord<KT>> {
        self.write_record(KeyRecord::_with_key_value(key.clone(), value_offset), true)
    }

    #[inline]
    fn add_key_record_with_slice(
        &mut self,
        key_slice: &[u8],
        value_offset: ValueRecordOffset,
    ) -> Result<KeyRecord<KT>> {
        self.write_record(
            KeyRecord::with_key_slice_value(key_slice, value_offset),
            true,
        )
    }

    fn write_record(&mut self, mut record: KeyRecord<KT>, is_new: bool) -> Result<KeyRecord<KT>> {
        debug_assert!(is_new || !record.offset.is_zero());
        //
        let (encorded_record_len, record_len, _key_len) = record.encoded_record_size();
        let new_record_size = self
            .0
            .piece_mgr
            .roundup(KeyRecordSize::new(encorded_record_len + record_len));
        //
        if !is_new {
            let old_record_size = {
                self.0.seek_from_start(record.offset)?;
                self.0.read_record_size()?
            };
            debug_assert!(old_record_size.is_valid_key());
            if new_record_size <= old_record_size {
                // over writes.
                self.0.seek_from_start(record.offset)?;
                record.size = old_record_size;
                record.dat_write_record_one(&mut self.0)?;
                return Ok(record);
            } else {
                // delete old and add new
                // old
                self.0
                    .push_free_piece_list(record.offset, old_record_size)?;
            }
        }
        // add new.
        {
            let free_record_offset = self.0.pop_free_piece_list(new_record_size)?;
            let new_record_offset = if !free_record_offset.is_zero() {
                self.0.seek_from_start(free_record_offset)?;
                free_record_offset
            } else {
                self.0.seek_to_end()?
            };
            record.offset = new_record_offset;
            record.size = new_record_size;
            debug_assert!(record.size.is_valid_key());
            match record.dat_write_record_one(&mut self.0) {
                Ok(()) => (),
                Err(err) => {
                    // recover on error
                    let _ = self.0.set_file_length(new_record_offset);
                    return Err(err);
                }
            }
            record.offset = new_record_offset;
        }
        //
        Ok(record)
    }

    fn read_record(&mut self, offset: KeyRecordOffset) -> Result<KeyRecord<KT>> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_from_start(offset)?;
        let record_size = self.0.read_record_size()?;
        debug_assert!(record_size.is_valid_key());
        let key_len = self.0.read_key_len()?;
        let maybe = self.0.read_exact_maybeslice(key_len.into())?;
        let key = KT::from(&maybe);
        //
        #[cfg(feature = "htx")]
        let val_offset = self.0.read_value_record_offset()?;
        #[cfg(not(feature = "htx"))]
        let val_offset = self.0.read_record_offset()?;
        //
        let record = KeyRecord::with(offset, record_size, key, val_offset);
        //
        Ok(record)
    }

    #[inline]
    fn read_record_only_size(&mut self, offset: KeyRecordOffset) -> Result<KeyRecordSize> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_from_start(offset)?;
        let record_size = self.0.read_record_size()?;
        Ok(record_size)
    }

    #[inline]
    fn read_record_only_key_length(&mut self, offset: KeyRecordOffset) -> Result<KeyLength> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_record_key(offset)?;
        let key_len = self.0.read_key_len()?;
        Ok(key_len)
    }

    #[inline]
    pub fn read_record_only_key_maybeslice(
        &mut self,
        offset: KeyRecordOffset,
    ) -> Result<rabuf::MaybeSlice> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_record_key(offset)?;
        let key_len = self.0.read_key_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(key_len.into())?;
        Ok(maybe_slice)
    }

    #[inline]
    fn read_record_only_key(&mut self, offset: KeyRecordOffset) -> Result<KT> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_record_key(offset)?;
        let key_len = self.0.read_key_len()?;
        let maybe_slice = self.0.read_exact_maybeslice(key_len.into())?;
        Ok(KT::from(&maybe_slice))
    }

    #[inline]
    fn read_record_only_value_offset(
        &mut self,
        offset: KeyRecordOffset,
    ) -> Result<ValueRecordOffset> {
        debug_assert!(!offset.is_zero());
        //
        self.0.seek_skip_to_record_key(offset)?;
        let key_len = self.0.read_key_len()?;
        self.0.seek_skip_length(key_len)?;
        //
        #[cfg(feature = "htx")]
        let value_offset = self.0.read_value_record_offset()?;
        #[cfg(not(feature = "htx"))]
        let value_offset = self.0.read_record_offset()?;
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
| --     | 8     | val offset  | value record offset: u64          |
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
| --     | 8     | next        | next free record offset           |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
