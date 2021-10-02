use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct RawFile(Rc<RefCell<File>>);

impl RawFile {
    pub fn new(file: File) -> Self {
        Self(Rc::new(RefCell::new(file)))
    }
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.borrow().metadata()?.len() == 0u64)
    }
    pub fn sync_data(&self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
    /*
    pub fn write_header(&self) -> Result<()> {
        if self.length()? < 4096 {
            self.0.borrow_mut().set_len(4096)?;
        }
        write_header(&mut self.0.borrow_mut())
    }
    pub fn check_header(&self) -> Result<()> {
        check_header(&mut self.0.borrow_mut())
    }
    */
    pub fn seek_to_offset(&self, offset: u64) -> Result<()> {
        seek_to_offset(&mut self.0.borrow_mut(), offset)
    }
    pub fn position(&self) -> Result<u64> {
        position(&mut self.0.borrow_mut())
    }
    pub fn length(&self) -> Result<u64> {
        length(&mut self.0.borrow_mut())
    }
    pub fn write_record(&self, key: &[u8], value: &[u8]) -> Result<()> {
        write_record(&mut self.0.borrow_mut(), key, value)
    }
    pub fn read_record(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        read_record(&mut self.0.borrow_mut())
    }
    //
    pub fn read_all_db_map_idxs(&self) -> Result<Vec<(String, u64)>> {
        let map_idxs_len = 4096.min(self.length()?);
        self.seek_to_offset(512)?;
        let mut vec = Vec::new();
        while self.position()? < map_idxs_len {
            let rec = read_map_idx_record(&mut self.0.borrow_mut())?;
            vec.push(rec);
        }
        //
        Ok(vec)
    }
    pub fn write_all_db_map_idxs(&self, dat: &[(String, u64)]) -> Result<()> {
        let map_idxs_len = 4096.min(self.length()?);
        self.seek_to_offset(512)?;
        for a in dat {
            write_map_idx_record(&mut self.0.borrow_mut(), &a.0, a.1)?;
            if self.position()? >= map_idxs_len {
                let _ = self.sync_data();
                panic!("can not write more db_map_idx");
            }
        }
        //
        Ok(())
    }
}

fn seek_to_offset(file: &mut File, offset: u64) -> Result<()> {
    let _ = file.seek(SeekFrom::Start(offset))?;
    //
    Ok(())
}

fn position(file: &mut File) -> Result<u64> {
    file.stream_position()
}

fn length(file: &mut File) -> Result<u64> {
    let _ = file.seek(SeekFrom::End(0))?;
    file.stream_position()
}

/**
```text
+--------+-------+------------+---------------------------+
| offset | bytes | name       | comment                   |
+--------+-------+------------+---------------------------+
| 0      | 8     | key_len    | is zero, unused space     |
| 8      | 8     | value_len  | value length              |
| 16     | --    | key_data   | key data                  |
| --     | --    | value_data | value data                |
+--------+-------+------------+---------------------------+
```
*/
fn write_record(file: &mut File, key: &[u8], value: &[u8]) -> Result<()> {
    file.write_u64::<LittleEndian>(key.len() as u64)?;
    file.write_u64::<LittleEndian>(value.len() as u64)?;
    let _ = file.write(key)?;
    let _ = file.write(value)?;
    //
    Ok(())
}
fn read_record(file: &mut File) -> Result<(Vec<u8>, Vec<u8>)> {
    let key_len = file.read_u64::<LittleEndian>()?;
    let val_len = file.read_u64::<LittleEndian>()?;
    //
    let mut key = vec![0u8; key_len as usize];
    let _ = file.read_exact(&mut key)?;
    let mut value = vec![0u8; val_len as usize];
    let _ = file.read_exact(&mut value)?;
    //
    Ok((key, value))
}

/**
```text
+--------+-------+------------+---------------------------+
| offset | bytes | name       | comment                   |
+--------+-------+------------+---------------------------+
| 0      | 8     | key_len    | is zero, unused space     |
| 8      | --    | key_data   | key data                  |
| --     | 8     | value_data | value data as u64         |
+--------+-------+------------+---------------------------+
```
*/
fn read_map_idx_record(file: &mut File) -> Result<(String, u64)> {
    let key_len = file.read_u64::<LittleEndian>()?;
    //
    let mut key = vec![0u8; key_len as usize];
    let _ = file.read_exact(&mut key)?;
    let value = file.read_u64::<LittleEndian>()?;
    //
    Ok((String::from_utf8_lossy(&key).to_string(), value))
}
fn write_map_idx_record(file: &mut File, key: &str, value: u64) -> Result<()> {
    let key = key.as_bytes();
    file.write_u64::<LittleEndian>(key.len() as u64)?;
    let _ = file.write(key)?;
    file.write_u64::<LittleEndian>(value)?;
    //
    Ok(())
}
