use std::fs::File;
use std::io::{Cursor, Read, Result, Seek, SeekFrom, Write};

use super::buf::BufFile;

#[cfg(feature = "vf_vu64")]
use super::vu64;

/// Variable length integer access for a random access file.
#[derive(Debug)]
pub struct VarFile {
    buf_file: BufFile,
    /*
    #[cfg(feature = "vf_u32u32")]
    #[cfg(feature = "vf_u64u64")]
    #[cfg(feature = "vf_leb128")]
    #[cfg(feature = "vf_sqlvli")]
    */
}

#[derive(Debug)]
pub struct VarCursor {
    buf_cursor: Cursor<Vec<u8>>,
    /*
    #[cfg(feature = "vf_u32u32")]
    #[cfg(feature = "vf_u64u64")]
    #[cfg(feature = "vf_leb128")]
    #[cfg(feature = "vf_sqlvli")]
    */
}

impl VarFile {
    /// Creates a new VarFile.
    pub fn new(file: File) -> Result<VarFile> {
        Ok(Self {
            buf_file: BufFile::new(file)?,
        })
    }
    /// Creates a new VarFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    #[allow(dead_code)]
    pub fn with_capacity(max_num_chunks: u16, chunk_size: u32, file: File) -> Result<VarFile> {
        Ok(Self {
            buf_file: BufFile::with_capacity(max_num_chunks, chunk_size, file)?,
        })
    }
    ///
    pub fn sync_all(&mut self) -> Result<()> {
        self.buf_file.sync_all()
    }
    ///
    pub fn sync_data(&mut self) -> Result<()> {
        self.buf_file.sync_data()
    }
    ///
    pub fn _clear_buf(&mut self) -> Result<()> {
        self.buf_file._clear_buf()
    }
    ///
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        self.buf_file.buf_stats()
    }
}

impl Read for VarFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.buf_file.read(buf)
    }
}

impl Write for VarFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buf_file.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.buf_file.flush()
    }
}

impl Seek for VarFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.buf_file.seek(pos)
    }
}

impl VarCursor {
    /// Creates a new VarCursor with a capacity size.
    pub fn with_capacity(capacity_size: usize) -> VarCursor {
        Self {
            buf_cursor: Cursor::new(Vec::with_capacity(capacity_size)),
        }
    }
    pub fn into_inner(self) -> Vec<u8> {
        self.buf_cursor.into_inner()
    }
}

impl Write for VarCursor {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buf_cursor.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.buf_cursor.flush()
    }
}

impl VarFile {
    #[inline]
    pub fn read_u64_le(&mut self) -> Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
    #[inline]
    pub fn write_u64_le(&mut self, value: u64) -> Result<()> {
        let mut buf = [0; 8];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarCursor {
    #[inline]
    pub fn write_u64_le(&mut self, value: u64) -> Result<()> {
        let mut buf = [0; 8];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
}

#[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
impl VarFile {
    #[inline]
    pub fn _read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    #[inline]
    pub fn _write_u8(&mut self, value: u8) -> Result<()> {
        self.write_all(&[value])
    }
    #[inline]
    pub fn read_u16_le(&mut self) -> Result<u16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }
    #[inline]
    pub fn _write_u16_le(&mut self, value: u16) -> Result<()> {
        let mut buf = [0; 2];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
    #[inline]
    pub fn read_u32_le(&mut self) -> Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
    #[inline]
    pub fn write_u32_le(&mut self, value: u32) -> Result<()> {
        let mut buf = [0; 4];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
}

#[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
impl VarCursor {
    #[inline]
    pub fn _write_u8(&mut self, value: u8) -> Result<()> {
        self.write_all(&[value])
    }
    #[inline]
    pub fn write_u16_le(&mut self, value: u16) -> Result<()> {
        let mut buf = [0; 2];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
    #[inline]
    pub fn write_u32_le(&mut self, value: u32) -> Result<()> {
        let mut buf = [0; 4];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
}

#[cfg(feature = "vf_u32u32")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        self.read_u32_le().map(|n| n as u64)
    }
    //
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<u64> {
        self.read_u32_le().map(|n| n as u64)
    }
    #[inline]
    pub fn write_record_offset(&mut self, offset: u64) -> Result<()> {
        debug_assert!(offset <= u32::MAX as u64);
        self.write_u32_le(offset as u32)
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: u32) -> Result<()> {
        self.write_u32_le(record_size)
    }
    //
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        self.read_u32_le().map(|n| n as u64)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        debug_assert!(node_offset <= u32::MAX as u64);
        self.write_u32_le(node_offset as u32)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: u32) -> Result<()> {
        self.write_u32_le(node_size)
    }
    #[inline]
    pub fn read_keys_len(&mut self) -> Result<u16> {
        self.read_u16_le()
    }
}

#[cfg(feature = "vf_u32u32")]
impl VarCursor {
    #[inline]
    pub fn write_key_len(&mut self, key_len: u32) -> Result<()> {
        self.write_u32_le(key_len)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: u32) -> Result<()> {
        self.write_u32_le(value_len)
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        debug_assert!(key_offset <= u32::MAX as u64);
        self.write_u32_le(key_offset as u32)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        debug_assert!(node_offset <= u32::MAX as u64);
        self.write_u32_le(node_offset as u32)
    }
    #[inline]
    pub fn write_keys_len(&mut self, keys_len: u16) -> Result<()> {
        self.write_u16_le(keys_len)
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    //
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn write_record_offset(&mut self, offset: u64) -> Result<()> {
        self.write_u64_le(offset)
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: u32) -> Result<()> {
        self.write_u32_le(record_size)
    }
    //
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_u64_le(node_offset)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<u32> {
        self.read_u32_le()
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: u32) -> Result<()> {
        self.write_u32_le(node_size)
    }
    #[inline]
    pub fn read_keys_len(&mut self) -> Result<u16> {
        self.read_u16_le()
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarCursor {
    #[inline]
    pub fn write_key_len(&mut self, key_len: u32) -> Result<()> {
        self.write_u32_le(key_len)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: u32) -> Result<()> {
        self.write_u32_le(value_len)
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        self.write_u64_le(key_offset)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_u64_le(node_offset)
    }
    #[inline]
    pub fn write_keys_len(&mut self, keys_len: u16) -> Result<()> {
        self.write_u16_le(keys_len)
    }
}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_vu64_u16(&mut self) -> Result<u16> {
        vu64::decode_vu64(&mut self.buf_file).map(|n| n as u16)
    }
    #[inline]
    pub fn read_vu64_u32(&mut self) -> Result<u32> {
        vu64::decode_vu64(&mut self.buf_file).map(|n| n as u32)
    }
    #[inline]
    pub fn read_vu64_u64(&mut self) -> Result<u64> {
        vu64::decode_vu64(&mut self.buf_file)
    }
    #[inline]
    pub fn _write_vu64_u16(&mut self, val: u16) -> Result<()> {
        self.write_all(vu64::encode(val as u64).as_ref())
    }
    #[inline]
    pub fn write_vu64_u32(&mut self, val: u32) -> Result<()> {
        self.write_all(vu64::encode(val as u64).as_ref())
    }
    #[inline]
    pub fn write_vu64_u64(&mut self, val: u64) -> Result<()> {
        self.write_all(vu64::encode(val).as_ref())
    }
}

#[cfg(feature = "vf_vu64")]
impl VarCursor {
    #[inline]
    pub fn write_vu64_u16(&mut self, val: u16) -> Result<()> {
        self.write_all(vu64::encode(val as u64).as_ref())
    }
    #[inline]
    pub fn write_vu64_u32(&mut self, val: u32) -> Result<()> {
        self.write_all(vu64::encode(val as u64).as_ref())
    }
    #[inline]
    pub fn write_vu64_u64(&mut self, val: u64) -> Result<()> {
        self.write_all(vu64::encode(val).as_ref())
    }
}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u32> {
        self.read_vu64_u32()
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u32> {
        self.read_vu64_u32()
    }
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        self.read_vu64_u64()
    }
    //
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn write_record_offset(&mut self, offset: u64) -> Result<()> {
        self.write_u64_le(offset)
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<u32> {
        self.read_vu64_u32()
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: u32) -> Result<()> {
        self.write_vu64_u32(record_size)
    }
    //
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        self.read_vu64_u64()
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_vu64_u64(node_offset)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<u32> {
        self.read_vu64_u32()
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: u32) -> Result<()> {
        self.write_vu64_u32(node_size)
    }
    #[inline]
    pub fn read_keys_len(&mut self) -> Result<u16> {
        self.read_vu64_u16()
    }
}

#[cfg(feature = "vf_vu64")]
impl VarCursor {
    #[inline]
    pub fn write_key_len(&mut self, key_len: u32) -> Result<()> {
        self.write_vu64_u32(key_len)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: u32) -> Result<()> {
        self.write_vu64_u32(value_len)
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        self.write_vu64_u64(key_offset)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_vu64_u64(node_offset)
    }
    #[inline]
    pub fn write_keys_len(&mut self, keys_len: u16) -> Result<()> {
        self.write_vu64_u16(keys_len)
    }
}

//--
mod debug {
    #[test]
    fn test_size_of() {
        use super::VarFile;
        //
        #[cfg(target_pointer_width = "64")]
        #[cfg(not(feature = "buf_stats"))]
        {
            #[cfg(feature = "vf_u32u32")]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
            #[cfg(feature = "vf_u64u64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
            #[cfg(feature = "vf_vu64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
        }
        #[cfg(target_pointer_width = "32")]
        #[cfg(not(feature = "buf_stats"))]
        {
            #[cfg(feature = "vf_u32u32")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
            #[cfg(feature = "vf_u64u64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
            #[cfg(feature = "vf_vu64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
        }
        //
        #[cfg(target_pointer_width = "64")]
        #[cfg(feature = "buf_stats")]
        {
            #[cfg(feature = "vf_u32u32")]
            assert_eq!(std::mem::size_of::<VarFile>(), 128);
            #[cfg(feature = "vf_u64u64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 128);
            #[cfg(feature = "vf_vu64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 128);
        }
        #[cfg(target_pointer_width = "32")]
        #[cfg(feature = "buf_stats")]
        {
            #[cfg(feature = "vf_u32u32")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
            #[cfg(feature = "vf_u64u64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
            #[cfg(feature = "vf_vu64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
        }
    }
}
