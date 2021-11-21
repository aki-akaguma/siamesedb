use super::super::FileDbParams;
use super::dbxxx::FileDbXxxInnerKT;
use super::semtype::*;
use super::vfile::VarFile;
use std::cell::RefCell;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

type HeaderSignature = [u8; 8];

const _DAT_HEADER_SZ: u64 = 128;
const DAT_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'0', 0u8];

#[cfg(not(feature = "record_cache"))]
use std::marker::PhantomData;

#[cfg(feature = "record_cache")]
use super::rc::RecordCache;

#[cfg(not(feature = "record_cache"))]
#[derive(Debug)]
struct VarFileRecordCache<KT: FileDbXxxInnerKT>(VarFile, PhantomData<KT>);

#[cfg(feature = "record_cache")]
#[derive(Debug)]
struct VarFileRecordCache<KT: FileDbXxxInnerKT>(VarFile, RecordCache<KT>);

#[derive(Debug, Clone)]
pub struct DatFile<KT: FileDbXxxInnerKT>(Rc<RefCell<VarFileRecordCache<KT>>>);

impl<KT: FileDbXxxInnerKT> DatFile<KT> {
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        ks_name: &str,
        sig2: HeaderSignature,
        params: &FileDbParams,
    ) -> Result<Self> {
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.dat", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = VarFile::with_capacity(
            std_file,
            params.dat_buf_num_chunks,
            params.dat_buf_chunk_size,
        )?;
        let _ = file.seek(SeekFrom::End(0))?;
        let len = file.stream_position()?;
        if len == 0 {
            file.write_recf_init_header(sig2)?;
        } else {
            file.check_recf_header(sig2)?;
        }
        //
        #[cfg(not(feature = "record_cache"))]
        let file_rc = VarFileRecordCache(file, PhantomData);
        #[cfg(feature = "record_cache")]
        let file_rc = VarFileRecordCache(file, RecordCache::new());
        //
        Ok(Self(Rc::new(RefCell::new(file_rc))))
    }
    pub fn flush(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        #[cfg(feature = "record_cache")]
        locked.flush_record_cache()?;
        locked.0.flush()
    }
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        #[cfg(feature = "record_cache")]
        locked.flush_record_cache_clear()?;
        locked.0.sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        #[cfg(feature = "record_cache")]
        locked.flush_record_cache_clear()?;
        locked.0.sync_data()
    }
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        let locked = self.0.borrow();
        locked.0.buf_stats()
    }
    //
    pub(crate) fn read_record_only_size(&self, offset: RecordOffset) -> Result<RecordSize> {
        let mut locked = self.0.borrow_mut();
        locked.read_record_only_size(offset)
    }
    pub fn read_record_only_key(&self, offset: RecordOffset) -> Result<KT> {
        let mut locked = self.0.borrow_mut();
        locked.read_record_only_key(offset)
    }
    pub fn read_record(&self, offset: RecordOffset) -> Result<Record<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.read_record(offset)
    }
    pub fn write_record(&self, record: Record<KT>) -> Result<Record<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.write_record(record, false)
    }
    pub fn delete_record(&self, offset: RecordOffset) -> Result<RecordSize> {
        let mut locked = self.0.borrow_mut();
        locked.delete_record(offset)
    }
    pub fn add_record(&self, key: &KT, value: &[u8]) -> Result<Record<KT>> {
        let mut locked = self.0.borrow_mut();
        locked.add_record(key, value)
    }
}

// for debug
impl<KT: FileDbXxxInnerKT> DatFile<KT> {
    pub fn count_of_free_record(&self) -> Result<Vec<(u32, u64)>> {
        let sz_ary = REC_SIZE_ARY;
        //
        let mut vec = Vec::new();
        let mut locked = self.0.borrow_mut();
        for record_size in sz_ary {
            let cnt = locked
                .0
                .count_of_free_record_list(RecordSize::new(record_size))?;
            vec.push((record_size, cnt));
        }
        Ok(vec)
    }
}

/**
write initiale header to file.

## header map

The db data header size is 128 bytes.

```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 4     | signature1  | [b's', b'h', b'a', b'm']  |
| 4      | 4     | signature1  | [b'd', b'b', b'1', 0u8]   |
| 8      | 8     | signature2  | 8 bytes type signature    |
| 16     | 8     | reserve0    |                           |
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
    fn write_recf_init_header(&mut self, signature2: HeaderSignature) -> Result<()> {
        let _ = self.seek(SeekFrom::Start(0))?;
        // signature1
        self.write_all(&DAT_HEADER_SIGNATURE)?;
        // signature2
        self.write_all(&signature2)?;
        // reserve0
        self.write_u64_le(0)?;
        // free1 .. rserve1
        self.write_all(&[0u8; 104])?;
        //
        Ok(())
    }
    fn check_recf_header(&mut self, signature2: HeaderSignature) -> Result<()> {
        let _ = self.seek(SeekFrom::Start(0))?;
        // signature1
        let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let _sz = self.read_exact(&mut sig1)?;
        assert!(!(sig1 != DAT_HEADER_SIGNATURE), "invalid header signature1");
        // signature2
        let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
        let _sz = self.read_exact(&mut sig2)?;
        assert!(
            !(sig2 != signature2),
            "invalid header signature2, type signature: {:?}",
            sig2
        );
        // reserve0
        let _reserve0 = self.read_u64_le()?;
        assert!(!(_reserve0 != 0), "invalid reserve0");
        //
        Ok(())
    }
}

const REC_SIZE_FREE_OFFSET_1ST: u64 = 24;

const REC_SIZE_FREE_OFFSET: [u64; 8] = [
    REC_SIZE_FREE_OFFSET_1ST,
    REC_SIZE_FREE_OFFSET_1ST + 8,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 2,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 3,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 4,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 5,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 6,
    REC_SIZE_FREE_OFFSET_1ST + 8 * 7,
];

pub(crate) const REC_SIZE_ARY: [u32; 8] =
    [8 * 2, 8 * 3, 8 * 4, 8 * 6, 8 * 8, 8 * 32, 8 * 64, 8 * 256];

impl RecordSize {
    pub(crate) fn is_valid(&self) -> bool {
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
    fn free_record_list_offset_of_header(&self) -> u64 {
        let record_size = self.as_value();
        debug_assert!(record_size > 0, "record_size: {} > 0", record_size);
        for i in 0..REC_SIZE_ARY.len() {
            if REC_SIZE_ARY[i] == record_size {
                return REC_SIZE_FREE_OFFSET[i];
            }
        }
        debug_assert!(
            record_size > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2],
            "record_size: {} > REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]: {}",
            record_size,
            REC_SIZE_ARY[REC_SIZE_ARY.len() - 2]
        );
        REC_SIZE_FREE_OFFSET[REC_SIZE_FREE_OFFSET.len() - 1]
    }
    fn is_large_record_size(&self) -> bool {
        let record_size = self.as_value();
        record_size >= REC_SIZE_ARY[REC_SIZE_ARY.len() - 1]
    }
    fn roundup(&self) -> RecordSize {
        let record_size = self.as_value();
        debug_assert!(record_size > 0, "record_size: {} > 0", record_size);
        for &n_sz in REC_SIZE_ARY.iter().take(REC_SIZE_ARY.len() - 1) {
            if record_size <= n_sz {
                return RecordSize::new(n_sz);
            }
        }
        RecordSize::new(((record_size + 511) / 512) * 512)
    }
}

impl VarFile {
    fn read_free_record_offset_on_header(
        &mut self,
        record_size: RecordSize,
    ) -> Result<RecordOffset> {
        let _ = self.seek(SeekFrom::Start(
            record_size.free_record_list_offset_of_header(),
        ))?;
        self.read_u64_le().map(RecordOffset::new)
    }

    fn write_free_record_offset_on_header(
        &mut self,
        record_size: RecordSize,
        offset: RecordOffset,
    ) -> Result<()> {
        let _ = self.seek(SeekFrom::Start(
            record_size.free_record_list_offset_of_header(),
        ))?;
        self.write_u64_le(offset.as_value())
    }

    fn count_of_free_record_list(&mut self, new_record_size: RecordSize) -> Result<u64> {
        let mut count = 0;
        let free_1st = self.read_free_record_offset_on_header(new_record_size)?;
        if !free_1st.is_zero() {
            let mut free_next_offset = free_1st;
            while !free_next_offset.is_zero() {
                count += 1;
                free_next_offset = {
                    let _a = self.seek_from_start(free_next_offset)?;
                    debug_assert!(_a == free_next_offset);
                    let _record_size = self.read_record_size()?;
                    let _key_len = self.read_key_len()?;
                    debug_assert!(_key_len.is_zero());
                    self.read_free_record_offset()?
                };
            }
        }
        Ok(count)
    }

    fn pop_free_record_list(&mut self, new_record_size: RecordSize) -> Result<RecordOffset> {
        let free_1st = self.read_free_record_offset_on_header(new_record_size)?;
        if !new_record_size.is_large_record_size() {
            if !free_1st.is_zero() {
                let free_next = {
                    let _ = self.seek_from_start(free_1st)?;
                    let (free_next, record_size) = {
                        let record_size = self.read_record_size()?;
                        let _key_len = self.read_key_len()?;
                        debug_assert!(_key_len.is_zero());
                        let record_offset = self.read_free_record_offset()?;
                        (record_offset, record_size)
                    };
                    //
                    self.write_record_clear(free_1st, record_size)?;
                    //
                    free_next
                };
                self.write_free_record_offset_on_header(new_record_size, free_next)?;
            }
            Ok(free_1st)
        } else {
            self.pop_free_record_list_large(new_record_size, free_1st)
        }
    }

    fn pop_free_record_list_large(
        &mut self,
        new_record_size: RecordSize,
        free_1st: RecordOffset,
    ) -> Result<RecordOffset> {
        let mut free_prev = RecordOffset::new(0);
        let mut free_curr = free_1st;
        while !free_curr.is_zero() {
            let _ = self.seek_from_start(free_curr)?;
            let (free_next, record_size) = {
                let record_size = self.read_record_size()?;
                let _key_len = self.read_key_len()?;
                debug_assert!(_key_len.is_zero());
                let record_offset = self.read_free_record_offset()?;
                (record_offset, record_size)
            };
            if new_record_size >= record_size {
                if !free_prev.is_zero() {
                    let _ = self.seek_from_start(free_prev)?;
                    let _record_size = self.read_record_size()?;
                    let _key_len = self.read_key_len()?;
                    debug_assert!(_key_len.is_zero());
                    self.write_free_record_offset(free_next)?;
                } else {
                    self.write_free_record_offset_on_header(new_record_size, free_next)?;
                }
                //
                self.write_record_clear(free_curr, record_size)?;
                return Ok(free_curr);
            }
            free_prev = free_curr;
            free_curr = free_next;
        }
        Ok(free_curr)
    }

    fn push_free_record_list(
        &mut self,
        old_record_offset: RecordOffset,
        old_record_size: RecordSize,
    ) -> Result<()> {
        if old_record_offset.is_zero() {
            return Ok(());
        }
        debug_assert!(!old_record_size.is_zero());
        //
        let free_1st = self.read_free_record_offset_on_header(old_record_size)?;
        {
            let start_offset = self.seek_from_start(old_record_offset)?;
            debug_assert!(start_offset == old_record_offset);
            self.write_record_size(old_record_size)?;
            self.write_key_len(KeyLength::new(0))?;
            self.write_free_record_offset(free_1st)?;
            self.write_zero_to(start_offset.as_value() + old_record_size.as_value() as u64)?;
        }
        self.write_free_record_offset_on_header(old_record_size, old_record_offset)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct Record<KT: FileDbXxxInnerKT> {
    /// offset of IdxNode in dat file.
    pub offset: RecordOffset,
    /// size in bytes of Record in dat file.
    pub size: RecordSize,
    /// key data.
    pub key: KT,
    /// value data.
    pub value: Vec<u8>,
}

impl<KT: FileDbXxxInnerKT> Record<KT> {
    pub fn with(offset: RecordOffset, size: RecordSize, key: KT, value: Vec<u8>) -> Self {
        Self {
            offset,
            size,
            key,
            value,
        }
    }
    pub fn with_key_value(key: KT, value: &[u8]) -> Self {
        Self {
            key,
            value: value.to_vec(),
            ..Default::default()
        }
    }
    //
    fn encoded_record_size(&self) -> (u32, u32, KeyLength, ValueLength) {
        let key_len = KeyLength::new(self.key.byte_len() as u32);
        let value_len = ValueLength::new(self.value.len() as u32);
        //
        #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
        let (encorded_record_len, record_len) = {
            let enc_key_len = 4;
            let enc_val_len = 4;
            let record_len: u32 =
                enc_key_len + key_len.as_value() + enc_val_len + value_len.as_value();
            let encorded_record_len = 4;
            (encorded_record_len, record_len)
        };
        #[cfg(feature = "vf_vu64")]
        let (encorded_record_len, record_len) = {
            let enc_key_len = vu64::encoded_len(key_len.as_value() as u64) as u32;
            let enc_val_len = vu64::encoded_len(value_len.as_value() as u64) as u32;
            let record_len: u32 =
                enc_key_len + key_len.as_value() + enc_val_len + value_len.as_value();
            let encorded_record_len = vu64::encoded_len(record_len as u64) as u32;
            (encorded_record_len, record_len)
        };
        //
        (encorded_record_len, record_len, key_len, value_len)
    }
    //
    pub(crate) fn dat_write_record_one(&self, file: &mut VarFile) -> Result<()> {
        assert!(!self.size.is_zero());
        //
        let key = self.key.as_bytes();
        let key_len = KeyLength::new(key.len().try_into().unwrap());
        let value = &self.value;
        let value_len = ValueLength::new(value.len().try_into().unwrap());
        //
        let _a = file.seek_from_start(self.offset)?;
        debug_assert!(_a == self.offset);
        //
        file.write_record_size(self.size)?;
        file.write_key_len(key_len)?;
        file.write_all(&key)?;
        file.write_value_len(value_len)?;
        file.write_all(value)?;
        file.write_zero_to_offset(self.offset + self.size)?;
        //
        Ok(())
    }
}

impl<KT: FileDbXxxInnerKT> VarFileRecordCache<KT> {
    #[cfg(feature = "record_cache")]
    fn flush_record_cache(&mut self) -> Result<()> {
        self.1.flush(&mut self.0)?;
        Ok(())
    }

    #[cfg(feature = "record_cache")]
    fn flush_record_cache_clear(&mut self) -> Result<()> {
        self.1.clear(&mut self.0)?;
        Ok(())
    }

    fn delete_record(&mut self, offset: RecordOffset) -> Result<RecordSize> {
        #[cfg(not(feature = "record_cache"))]
        let old_record_size = {
            let _ = self.0.seek_from_start(offset)?;
            self.0.read_record_size()?
        };
        #[cfg(feature = "record_cache")]
        let old_record_size = {
            match self.1.delete(&offset) {
                Some(record_size) => record_size,
                None => {
                    let _ = self.0.seek_from_start(offset)?;
                    self.0.read_record_size()?
                }
            }
        };
        //
        self.0.push_free_record_list(offset, old_record_size)?;
        Ok(old_record_size)
    }

    fn add_record(&mut self, key: &KT, value: &[u8]) -> Result<Record<KT>> {
        self.write_record(Record::with_key_value(key.clone(), value), true)
    }

    fn write_record(&mut self, mut record: Record<KT>, is_new: bool) -> Result<Record<KT>> {
        debug_assert!(is_new || !record.offset.is_zero());
        //
        let (encorded_record_len, record_len, _key_len, _value_len) = record.encoded_record_size();
        let new_record_size = RecordSize::new(encorded_record_len + record_len).roundup();
        //
        if !is_new {
            #[cfg(not(feature = "record_cache"))]
            let old_record_size = {
                let _ = self.0.seek_from_start(record.offset)?;
                self.0.read_record_size()?
            };
            #[cfg(feature = "record_cache")]
            let old_record_size = {
                if let Some(record_size) = self.1.get_record_size(&record.offset) {
                    record_size
                } else {
                    let _ = self.0.seek_from_start(record.offset)?;
                    self.0.read_record_size()?
                }
            };
            debug_assert!(old_record_size.is_valid());
            if new_record_size <= old_record_size {
                // over writes.
                #[cfg(not(feature = "record_cache"))]
                {
                    let _ = self.0.seek_from_start(record.offset)?;
                    record.size = old_record_size;
                    record.dat_write_record_one(&mut self.0)?;
                    return Ok(record);
                }
                #[cfg(feature = "record_cache")]
                {
                    let record = self.1.put(&mut self.0, record, old_record_size, true)?;
                    return Ok(record);
                }
            } else {
                // delete old and add new
                #[cfg(feature = "record_cache")]
                self.1.delete(&record.offset);
                // old
                self.0
                    .push_free_record_list(record.offset, old_record_size)?;
            }
        }
        // add new.
        {
            let free_record_offset = self.0.pop_free_record_list(new_record_size)?;
            let new_record_offset = if !free_record_offset.is_zero() {
                let _ = self.0.seek_from_start(free_record_offset)?;
                free_record_offset
            } else {
                let _ = self.0.seek(SeekFrom::End(0))?;
                RecordOffset::new(self.0.stream_position()?)
            };
            record.offset = new_record_offset;
            record.size = new_record_size;
            debug_assert!(record.size.is_valid());
            match record.dat_write_record_one(&mut self.0) {
                Ok(()) => (),
                Err(err) => {
                    // recover on error
                    let _ = self.0.set_len(new_record_offset);
                    return Err(err);
                }
            }
            record.offset = new_record_offset;
        }
        //
        Ok(record)
    }

    fn read_record(&mut self, offset: RecordOffset) -> Result<Record<KT>> {
        debug_assert!(!offset.is_zero());
        //
        #[cfg(feature = "record_cache")]
        if let Some(rc) = self.1.get(&offset) {
            return Ok(rc.as_ref().clone());
        }
        //
        let _ = self.0.seek_from_start(offset)?;
        let record_size = self.0.read_record_size()?;
        debug_assert!(record_size.is_valid());
        let key_len = self.0.read_key_len()?;
        let key = if key_len.is_zero() {
            Vec::with_capacity(0)
        } else {
            let mut key = vec![0u8; key_len.try_into().unwrap()];
            let _ = self.0.read_exact(&mut key)?;
            key
        };
        //
        let val_len = self.0.read_value_len()?;
        let mut value = vec![0u8; val_len.try_into().unwrap()];
        let _ = self.0.read_exact(&mut value)?;
        //
        let record = Record::with(offset, record_size, KT::from(&key), value.to_vec());
        //
        #[cfg(feature = "record_cache")]
        let record = self.1.put(&mut self.0, record, record_size, false)?;
        //
        Ok(record)
    }

    fn read_record_only_key(&mut self, offset: RecordOffset) -> Result<KT> {
        debug_assert!(!offset.is_zero());
        //
        #[cfg(feature = "record_cache")]
        {
            let record = self.read_record(offset)?;
            return Ok(record.key.clone());
        }
        //
        #[cfg(not(feature = "record_cache"))]
        {
            let _ = self.0.seek_from_start(offset)?;
            let _record_size = self.0.read_record_size()?;
            let key_len = self.0.read_key_len()?;
            let key = if key_len.is_zero() {
                Vec::with_capacity(0)
            } else {
                let mut key = vec![0u8; key_len.try_into().unwrap()];
                let _ = self.0.read_exact(&mut key)?;
                key
            };
            //
            Ok(KT::from(&key))
        }
    }

    fn read_record_only_size(&mut self, offset: RecordOffset) -> Result<RecordSize> {
        debug_assert!(!offset.is_zero());
        //
        #[cfg(feature = "record_cache")]
        {
            let record = self.read_record(offset)?;
            return Ok(record.size);
        }
        //
        #[cfg(not(feature = "record_cache"))]
        {
            let _ = self.0.seek_from_start(offset)?;
            let record_size = self.0.read_record_size()?;
            //
            Ok(record_size)
        }
    }
}

/*
```text
used record:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | record size | size in bytes of this record: u32 |
| --     | 1..5  | key len     | a byte length of key              |
| --     | --    | key data    | raw key data                      |
| --     | 1..5  | val len     | a byte length of value            |
| --     | --    | val data    | raw value data                    |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
/*
```text
free record:
+--------+-------+-------------+-----------------------------------+
| offset | bytes | name        | comment                           |
+--------+-------+-------------+-----------------------------------+
| 0      | 1..5  | record size | size in bytes of this record: u32 |
| --     | 1     | key len     | always zero                       |
| --     | 8     | next        | next free record offset           |
| --     | --    | reserve     | reserved free space               |
+--------+-------+-------------+-----------------------------------+
```
*/
