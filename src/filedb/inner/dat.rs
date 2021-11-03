use super::semtype::*;
use super::vfile::{VarCursor, VarFile};
use std::cell::RefCell;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

type HeaderSignature = [u8; 8];

const _DAT_HEADER_SZ: u64 = 128;
const DAT_HEADER_SIGNATURE: HeaderSignature = [b's', b'i', b'a', b'm', b'd', b'b', b'0', 0u8];

#[derive(Debug, Clone)]
pub struct DatFile(Rc<RefCell<VarFile>>);

impl DatFile {
    pub fn open<P: AsRef<Path>>(path: P, ks_name: &str, sig2: HeaderSignature) -> Result<Self> {
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.dat", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = VarFile::new(std_file)?;
        let _ = file.seek(SeekFrom::End(0))?;
        let len = file.stream_position()?;
        if len == 0 {
            dat_file_write_init_header(&mut file, sig2)?;
        } else {
            dat_file_check_header(&mut file, sig2)?;
        }
        //
        Ok(Self(Rc::new(RefCell::new(file))))
    }
    pub fn sync_all(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.sync_all()
    }
    pub fn sync_data(&self) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        locked.sync_data()
    }
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        let locked = self.0.borrow();
        locked.buf_stats()
    }
    //
    pub(crate) fn read_record_size(&self, offset: RecordOffset) -> Result<RecordSize> {
        let mut locked = self.0.borrow_mut();
        dat_read_record_size(&mut locked, offset)
    }
    pub fn read_record_key(&self, offset: RecordOffset) -> Result<Option<Vec<u8>>> {
        let mut locked = self.0.borrow_mut();
        dat_read_record_key(&mut locked, offset)
    }
    pub fn read_record(&self, offset: RecordOffset) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut locked = self.0.borrow_mut();
        dat_read_record(&mut locked, offset)
    }
    pub fn write_record(
        &self,
        offset: RecordOffset,
        key: &[u8],
        value: &[u8],
    ) -> Result<RecordOffset> {
        let mut locked = self.0.borrow_mut();
        dat_write_record(&mut locked, offset, key, value, false)
    }
    pub fn delete_record(&self, offset: RecordOffset) -> Result<RecordSize> {
        let mut locked = self.0.borrow_mut();
        dat_delete_record(&mut locked, offset)
    }
    pub fn add_record(&self, key: &[u8], value: &[u8]) -> Result<RecordOffset> {
        let mut locked = self.0.borrow_mut();
        dat_add_record(&mut locked, key, value)
    }
}

// for debug
impl DatFile {
    pub fn count_of_free_record(&self) -> Result<Vec<(u32, u64)>> {
        let sz_ary = REC_SIZE_ARY;
        //
        let mut vec = Vec::new();
        let mut locked = self.0.borrow_mut();
        for record_size in sz_ary {
            let cnt = dat_file_count_of_free_list(&mut locked, RecordSize::new(record_size))?;
            vec.push((record_size, cnt));
        }
        Ok(vec)
    }
    /*
    pub fn count_of_used_record(&self) -> Result<Vec<(usize, u64)>> {
        let sz_ary = NODE_SIZE_ARY;
        //
        let mut vec = Vec::new();
        for node_size in sz_ary {
            let cnt = 0;
            vec.push((node_size, cnt));
        }
        //
        let top_node = self.read_top_node()?;
        let mut locked = self.0.borrow_mut();
        idx_count_of_used_node(&mut locked.0, &top_node, &mut vec)?;
        //
        Ok(vec)
    }
    */
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
fn dat_file_write_init_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    // signature1
    let _ = file.write(&DAT_HEADER_SIGNATURE)?;
    // signature2
    let _ = file.write(&signature2)?;
    // reserve0
    file.write_u64_le(0)?;
    // free1 .. rserve1
    let _ = file.write(&[0u8; 104]);
    //
    Ok(())
}

fn dat_file_check_header(file: &mut VarFile, signature2: HeaderSignature) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    // signature1
    let mut sig1 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig1)?;
    assert!(!(sig1 != DAT_HEADER_SIGNATURE), "invalid header signature1");
    // signature2
    let mut sig2 = [0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8];
    let _sz = file.read_exact(&mut sig2)?;
    assert!(
        !(sig2 != signature2),
        "invalid header signature2, type signature: {:?}",
        sig2
    );
    // reserve0
    let _reserve0 = file.read_u64_le()?;
    assert!(!(_reserve0 != 0), "invalid reserve0");
    //
    Ok(())
}

const REC_SIZE_FREE_OFFSET_1ST: u64 = 16;

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

fn free_record_list_offset_of_header(record_size: RecordSize) -> u64 {
    let record_size = record_size.as_value();
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

fn is_large_record_size(record_size: RecordSize) -> bool {
    let record_size = record_size.as_value();
    record_size >= REC_SIZE_ARY[REC_SIZE_ARY.len() - 1]
}

fn record_size_roudup(record_size: RecordSize) -> RecordSize {
    let record_size = record_size.as_value();
    debug_assert!(record_size > 0, "record_size: {} > 0", record_size);
    for &n_sz in REC_SIZE_ARY.iter().take(REC_SIZE_ARY.len() - 1) {
        if record_size <= n_sz {
            return RecordSize::new(n_sz);
        }
    }
    RecordSize::new(((record_size + 511) / 512) * 512)
}

fn dat_file_read_free_record_offset(
    file: &mut VarFile,
    record_size: RecordSize,
) -> Result<RecordOffset> {
    let _ = file.seek(SeekFrom::Start(free_record_list_offset_of_header(
        record_size,
    )))?;
    file.read_u64_le().map(RecordOffset::new)
}

fn dat_file_write_free_record_offset(
    file: &mut VarFile,
    record_size: RecordSize,
    offset: RecordOffset,
) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(free_record_list_offset_of_header(
        record_size,
    )))?;
    file.write_u64_le(offset.as_value())
}

fn dat_file_count_of_free_list(file: &mut VarFile, new_record_size: RecordSize) -> Result<u64> {
    let mut count = 0;
    let free_1st = dat_file_read_free_record_offset(file, new_record_size)?;
    if !free_1st.is_zero() {
        let mut free_next_offset = free_1st;
        while !free_next_offset.is_zero() {
            count += 1;
            free_next_offset = {
                let _a = file.seek_from_start(free_next_offset)?;
                debug_assert!(_a == free_next_offset);
                let _record_size = file.read_record_size()?;
                let _key_len = file.read_key_len()?;
                debug_assert!(_key_len.is_zero());
                file.read_free_record_offset()?
            };
        }
    }
    Ok(count)
}

fn dat_file_pop_free_list(file: &mut VarFile, new_record_size: RecordSize) -> Result<RecordOffset> {
    let free_1st = dat_file_read_free_record_offset(file, new_record_size)?;
    if !is_large_record_size(new_record_size) {
        if !free_1st.is_zero() {
            let free_next = {
                let _ = file.seek_from_start(free_1st)?;
                let (free_next, record_size) = {
                    let record_size = file.read_record_size()?;
                    let _key_len = file.read_key_len()?;
                    debug_assert!(_key_len.is_zero());
                    let record_offset = file.read_free_record_offset()?;
                    (record_offset, record_size)
                };
                //
                let _ = file.seek_from_start(free_1st)?;
                file.write_record_size(record_size)?;
                let buff = vec![0; record_size.try_into().unwrap()];
                file.write_all(&buff)?;
                //
                free_next
            };
            dat_file_write_free_record_offset(file, new_record_size, free_next)?;
        }
        Ok(free_1st)
    } else {
        dat_file_pop_free_list_large(file, new_record_size, free_1st)
    }
}

fn dat_file_pop_free_list_large(
    file: &mut VarFile,
    new_record_size: RecordSize,
    free_1st: RecordOffset,
) -> Result<RecordOffset> {
    let mut free_prev = RecordOffset::new(0);
    let mut free_curr = free_1st;
    while !free_curr.is_zero() {
        let _ = file.seek_from_start(free_curr)?;
        let (free_next, record_size) = {
            let record_size = file.read_record_size()?;
            let _key_len = file.read_key_len()?;
            debug_assert!(_key_len.is_zero());
            let record_offset = file.read_free_record_offset()?;
            (record_offset, record_size)
        };
        if new_record_size >= record_size {
            if !free_prev.is_zero() {
                let _ = file.seek_from_start(free_prev)?;
                let _record_size = file.read_record_size()?;
                let _key_len = file.read_key_len()?;
                debug_assert!(_key_len.is_zero());
                file.write_free_record_offset(free_next)?;
            } else {
                dat_file_write_free_record_offset(file, new_record_size, free_next)?;
            }
            //
            let _ = file.seek_from_start(free_curr)?;
            file.write_record_size(record_size)?;
            let buff = vec![0; record_size.try_into().unwrap()];
            file.write_all(&buff)?;
            return Ok(free_curr);
        }
        free_prev = free_curr;
        free_curr = free_next;
    }
    Ok(free_curr)
}

fn dat_file_push_free_list(
    file: &mut VarFile,
    old_record_offset: RecordOffset,
    old_record_size: RecordSize,
) -> Result<()> {
    if old_record_offset.is_zero() {
        return Ok(());
    }
    debug_assert!(!old_record_size.is_zero());
    //
    let free_1st = dat_file_read_free_record_offset(file, old_record_size)?;
    {
        let _a = file.seek_from_start(old_record_offset)?;
        debug_assert!(_a == old_record_offset);
        file.write_record_size(old_record_size)?;
        file.write_key_len(KeyLength::new(0))?;
        file.write_free_record_offset(free_1st)?;
    }
    dat_file_write_free_record_offset(file, old_record_size, old_record_offset)?;
    Ok(())
}

fn dat_serialize_to_buf(key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    let mut buff_cursor = VarCursor::with_capacity(128);
    //
    let key_len = KeyLength::new(key.len() as u32);
    let value_len = ValueLength::new(value.len() as u32);
    //
    buff_cursor.write_key_len(key_len)?;
    let _ = buff_cursor.write_all(key)?;
    buff_cursor.write_value_len(value_len)?;
    let _ = buff_cursor.write_all(value)?;
    //
    Ok(buff_cursor.into_inner())
}

fn dat_write_record(
    file: &mut VarFile,
    offset: RecordOffset,
    key: &[u8],
    value: &[u8],
    is_new: bool,
) -> Result<RecordOffset> {
    debug_assert!(is_new || !offset.is_zero());
    //
    let mut buf_vec = dat_serialize_to_buf(key, value)?;
    let buf_ref = &mut buf_vec;
    let buf_len = buf_ref.len();
    #[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
    let encoded_len = 4;
    #[cfg(feature = "vf_vu64")]
    let encoded_len = super::vu64::encoded_len(buf_len as u64);
    let new_record_size = RecordSize::new(buf_len as u32 + encoded_len as u32);
    //
    let new_record_size = record_size_roudup(new_record_size);
    if buf_len < new_record_size.try_into().unwrap() {
        buf_ref.resize(new_record_size.try_into().unwrap(), 0u8);
    }
    //
    if !is_new {
        let _ = file.seek_from_start(offset)?;
        let old_record_size = file.read_record_size()?;
        if new_record_size <= old_record_size {
            // over writes.
            let _ = file.seek_from_start(offset)?;
            file.write_record_size(old_record_size)?;
            file.write_all(buf_ref)?;
            return Ok(offset);
        } else {
            // delete old and add new
            // old
            dat_file_push_free_list(file, offset, old_record_size)?;
        }
    }
    // add new.
    {
        let free_record_offset = dat_file_pop_free_list(file, new_record_size)?;
        let new_record_offset = if !free_record_offset.is_zero() {
            let _ = file.seek_from_start(free_record_offset)?;
            free_record_offset
        } else {
            let _ = file.seek(SeekFrom::End(0))?;
            RecordOffset::new(file.stream_position()?)
        };
        file.write_record_size(new_record_size)?;
        match file.write_all(buf_ref) {
            Ok(()) => (),
            Err(err) => {
                // recover on error
                let _ = file.set_len(new_record_offset);
                return Err(err);
            }
        }
        Ok(new_record_offset)
    }
}

fn dat_read_record(file: &mut VarFile, offset: RecordOffset) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    debug_assert!(!offset.is_zero());
    //
    let _ = file.seek_from_start(offset)?;
    let _record_size = file.read_record_size()?;
    let key_len = file.read_key_len()?;
    if key_len.is_zero() {
        return Ok(None);
    }
    let mut key = vec![0u8; key_len.try_into().unwrap()];
    let _ = file.read_exact(&mut key)?;
    //
    let val_len = file.read_value_len()?;
    let mut value = vec![0u8; val_len.try_into().unwrap()];
    let _ = file.read_exact(&mut value)?;
    //
    Ok(Some((key, value)))
}

fn dat_read_record_key(file: &mut VarFile, offset: RecordOffset) -> Result<Option<Vec<u8>>> {
    debug_assert!(!offset.is_zero());
    //
    let _ = file.seek_from_start(offset)?;
    let _record_size = file.read_record_size()?;
    let key_len = file.read_key_len()?;
    if key_len.is_zero() {
        return Ok(None);
    }
    //
    let mut key = vec![0u8; key_len.try_into().unwrap()];
    let _ = file.read_exact(&mut key)?;
    //
    Ok(Some(key))
}

fn dat_read_record_size(file: &mut VarFile, offset: RecordOffset) -> Result<RecordSize> {
    debug_assert!(!offset.is_zero());
    //
    let _ = file.seek_from_start(offset)?;
    let record_size = file.read_record_size()?;
    //
    Ok(record_size)
}

fn dat_delete_record(file: &mut VarFile, offset: RecordOffset) -> Result<RecordSize> {
    let _ = file.seek_from_start(offset)?;
    let old_record_size = file.read_record_size()?;
    dat_file_push_free_list(file, offset, old_record_size)?;
    //
    Ok(old_record_size)
}

fn dat_add_record(file: &mut VarFile, key: &[u8], value: &[u8]) -> Result<RecordOffset> {
    dat_write_record(file, RecordOffset::new(0), key, value, true)
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
