#![allow(dead_code)]

use super::KeyType;
use std::cell::RefCell;
use std::fs::OpenOptions;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

use super::vfile::VarFile;

#[derive(Debug, Clone)]
pub struct UnuFile(Rc<RefCell<(VarFile, KeyType)>>);

impl UnuFile {
    pub fn open<P: AsRef<Path>>(path: P, ks_name: &str, kt: KeyType) -> Result<Self> {
        let mut pb = path.as_ref().to_path_buf();
        pb.push(format!("{}.unu", ks_name));
        let std_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pb)?;
        let mut file = VarFile::new(std_file)?;
        let _ = file.seek(SeekFrom::End(0))?;
        let len = file.stream_position()?;
        if len == 0 {
            unu_file_write_init_header(&mut file, kt)?;
        } else {
            unu_file_check_header(&mut file, kt)?;
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
    pub fn read_unu(&self, offset: u64) -> Result<Option<u64>> {
        let mut locked = self.0.borrow_mut();
        unu_read_unu(&mut locked.0, offset)
    }
    pub fn write_unu(&self, offset: u64, dat_offset: u64) -> Result<()> {
        let mut locked = self.0.borrow_mut();
        unu_write_unu(&mut locked.0, offset, dat_offset)
    }
    pub fn delete_unu(&self, offset: u64) -> Result<u64> {
        let mut locked = self.0.borrow_mut();
        unu_delete_unu(&mut locked.0, offset)
    }
    pub fn add_unu(&self, dat_offset: u64) -> Result<u64> {
        let mut locked = self.0.borrow_mut();
        unu_add_unu(&mut locked.0, dat_offset)
    }
}

/**
write initiale header to file.

## header map

The db unused header size is 64 bytes.

```text
+--------+-------+------------+---------------------------+
| offset | bytes | name       | comment                   |
+--------+-------+------------+---------------------------+
| 0      | 4     | signature1 | [b's', b'h', b'a', b'm']  |
| 4      | 4     | signature2 | [b'd', b'b', b'2', 0u8]   |
| 8      | 8     | count      | count of unused           |
| 16     | 48    | reserve1   |                           |
+--------+-------+------------+---------------------------+
```

- signature1: always fixed 4 bytes
- signature2: fixed 4 bytes, variable in future.

*/
fn unu_file_write_init_header(file: &mut VarFile, kt: KeyType) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(0))?;
    //
    let kt_byte = kt.signature();
    // signature
    let _ = file.write(&[b's', b'h', b'a', b'm'])?;
    let _ = file.write(&[b'd', b'b', kt_byte, b'2'])?;
    // count of data
    file.write_u64_le(0u64)?;
    // reserve1
    let _ = file.write(&[0u8; 48]);
    //
    Ok(())
}

fn unu_file_check_header(file: &mut VarFile, kt: KeyType) -> Result<()> {
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
    if sig2 != [b'd', b'b', kt_byte, b'2'] {
        panic!("invalid header signature2");
    }
    // count of unused
    let _count = file.read_u64_le()?;
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
| 0      | 8     | offset      | offset at dat file        |
+--------+-------+-------------+---------------------------+
```
*/
fn unu_write_unu(file: &mut VarFile, offset: u64, dat_offset: u64) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    file.write_u64_le(dat_offset)?;
    //
    Ok(())
}
fn unu_add_unu(file: &mut VarFile, dat_offset: u64) -> Result<u64> {
    let _ = file.seek(SeekFrom::End(0))?;
    let last_offset = file.stream_position()?;
    unu_write_unu(file, last_offset, dat_offset)?;
    Ok(last_offset)
}
fn unu_read_unu(file: &mut VarFile, offset: u64) -> Result<Option<u64>> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    let dat_offset = file.read_u64_le()?;
    if dat_offset == 0 {
        Ok(None)
    } else {
        Ok(Some(offset))
    }
}

/**
```text
+--------+-------+-------------+---------------------------+
| offset | bytes | name        | comment                   |
+--------+-------+-------------+---------------------------+
| 0      | 8     | reserve     | offset at dat file        |
+--------+-------+-------------+---------------------------+
```
*/
fn unu_delete_unu(file: &mut VarFile, offset: u64) -> Result<u64> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    let dat_offset = file.read_u64_le()?;
    let _ = file.seek(SeekFrom::Start(offset))?;
    file.write_u64_le(0)?;
    //
    Ok(dat_offset)
}
