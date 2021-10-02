#![allow(dead_code)]

use super::KeyType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

use super::buf::BufFile;

#[derive(Debug, Clone)]
pub struct DatFile(Rc<RefCell<(BufFile, KeyType)>>);

impl DatFile {
    pub fn open<P: AsRef<Path>>(path: P, ks_name: &str, kt: KeyType) -> Result<Self> {
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.dat", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = BufFile::new(std_file)?;
        let _ = file.seek(SeekFrom::End(0))?;
        let len = file.stream_position()?;
        if len == 0 {
            dat_file_write_init_header(&mut file, kt)?;
        } else {
            dat_file_check_header(&mut file, kt)?;
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
    pub fn read_record_key(&self, offset: u64) -> Result<Option<Vec<u8>>> {
        let mut locked = self.0.borrow_mut();
        dat_read_record_key(&mut locked.0, offset)
    }
    pub fn read_record(&self, offset: u64) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut locked = self.0.borrow_mut();
        dat_read_record(&mut locked.0, offset)
    }
    pub fn write_record(&self, offset: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        dat_write_record(&mut locked.0, offset, key, value)
    }
    pub fn delete_record(&self, offset: u64) -> Result<u64> {
        let mut locked = self.0.borrow_mut();
        dat_delete_record(&mut locked.0, offset)
    }
    pub fn add_record(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        let mut locked = self.0.borrow_mut();
        dat_add_record(&mut locked.0, key, value)
    }
}

/**
write initiale header to file.

## header map

The db data header size is 64 bytes.

```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 4     | signature1  | [b's', b'h', b'a', b'm']  |
| 4      | 4     | signature2  | [b'd', b'b', b'0', 0u8]   |
| 8      | 8     | count       | count of data             |
| 16     | 48    | reserve1    |                           |
+--------+-------+-------------+---------------------------+
```

- signature1: always fixed 4 bytes
- signature2: fixed 4 bytes, variable in future.

*/
fn dat_file_write_init_header(file: &mut BufFile, kt: KeyType) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    //
    let kt_byte = kt.signature();
    // signature
    let _ = file.write_all(&[b's', b'h', b'a', b'm'])?;
    let _ = file.write_all(&[b'd', b'b', kt_byte, b'0'])?;
    // count of data
    file.write_u64::<LittleEndian>(0u64)?;
    // reserve1
    let _ = file.write_all(&[0u8; 48]);
    //
    Ok(())
}

fn dat_file_check_header(file: &mut BufFile, kt: KeyType) -> Result<()> {
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
    if sig2 != [b'd', b'b', kt_byte, b'0'] {
        panic!("invalid header signature2");
    }
    // count of data
    let _count = file.read_u64::<LittleEndian>()?;
    if _count != 0 {
        //panic!("invalid count");
    }
    //
    Ok(())
}

/**
```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 8     | key_len     | is zero, unused space     |
| 8      | 8     | value_len   | value length              |
| 16     | --    | key_data    | key data                  |
| --     | --    | value_data  | value data                |
+--------+-------+-------------+---------------------------+
```
*/
fn dat_write_record(file: &mut BufFile, offset: u64, key: &[u8], value: &[u8]) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    file.write_u64::<LittleEndian>(key.len() as u64)?;
    file.write_u64::<LittleEndian>(value.len() as u64)?;
    let _ = file.write_all(key)?;
    let _ = file.write_all(value)?;
    //
    Ok(())
}
fn dat_add_record(file: &mut BufFile, key: &[u8], value: &[u8]) -> Result<u64> {
    let _ = file.seek(SeekFrom::End(0))?;
    let last_offset = file.stream_position()?;
    dat_write_record(file, last_offset, key, value)?;
    Ok(last_offset)
}
fn dat_read_record(file: &mut BufFile, offset: u64) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    let key_len = file.read_u64::<LittleEndian>()?;
    if key_len == 0 {
        return Ok(None);
    }
    let val_len = file.read_u64::<LittleEndian>()?;
    //
    let mut key = vec![0u8; key_len as usize];
    let _ = file.read_exact(&mut key)?;
    let mut value = vec![0u8; val_len as usize];
    let _ = file.read_exact(&mut value)?;
    //
    Ok(Some((key, value)))
}
fn dat_read_record_key(file: &mut BufFile, offset: u64) -> Result<Option<Vec<u8>>> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    let key_len = file.read_u64::<LittleEndian>()?;
    if key_len == 0 {
        return Ok(None);
    }
    let _val_len = file.read_u64::<LittleEndian>()?;
    //
    let mut key = vec![0u8; key_len as usize];
    let _ = file.read_exact(&mut key)?;
    //
    Ok(Some(key))
}

/**
```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 8     | key_len     | is zero, unused space     |
| 8      | 8     | reserve_len | reserve length            |
| 16     | --    | reserve     | reserve data              |
+--------+-------+-------------+---------------------------+
```
*/
fn dat_delete_record(file: &mut BufFile, offset: u64) -> Result<u64> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    //
    let key_len = file.read_u64::<LittleEndian>()?;
    if key_len == 0 {
        let reserve_len = file.read_u64::<LittleEndian>()?;
        return Ok(reserve_len);
    }
    let val_len = file.read_u64::<LittleEndian>()?;
    //
    let reserve_len = key_len + val_len;
    //
    let _ = file.seek(SeekFrom::Start(offset))?;
    file.write_u64::<LittleEndian>(0)?;
    file.write_u64::<LittleEndian>(reserve_len)?;
    let _ = file.write_all(&vec![0u8; reserve_len as usize])?;
    //
    Ok(reserve_len)
}
