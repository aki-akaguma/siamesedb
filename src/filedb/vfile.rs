use std::fs::File;
use std::io::{Cursor, Read, Result, Seek, SeekFrom, Write};

use super::buf::BufFile;

#[cfg(feature = "vf_v64")]
use super::v64;

#[cfg(feature = "vf_vint64")]
use super::vint64::vint64;

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
    #[cfg(feature = "vf_vint64")]
    enc_buf: [u8; 9],
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
    #[cfg(feature = "vf_vint64")]
    enc_buf: [u8; 9],
}

impl VarFile {
    /// Creates a new VarFile.
    pub fn new(file: File) -> Result<VarFile> {
        Ok(Self {
            buf_file: BufFile::new(file)?,
            #[cfg(feature = "vf_vint64")]
            enc_buf: [0u8; 9],
        })
    }
    /// Creates a new VarFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    pub fn with_capacity(max_num_chunks: u16, chunk_size: u32, file: File) -> Result<VarFile> {
        Ok(Self {
            buf_file: BufFile::with_capacity(max_num_chunks, chunk_size, file)?,
            #[cfg(feature = "vf_vint64")]
            enc_buf: [0u8; 9],
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
    pub fn clear_buf(&mut self) -> Result<()> {
        self.buf_file.clear_buf()
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
            #[cfg(feature = "vf_vint64")]
            enc_buf: [0u8; 9],
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
    pub fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
    #[inline]
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.write_all(&[value])
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<usize> {
        Ok(self.read_u8()? as usize)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: usize) -> Result<()> {
        //debug_assert!(node_size <= 0x7F, "node_size: 0x{:02x} <= 0x7F", node_size);
        self.write_u8(node_size as u8)
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

impl VarCursor {
    #[inline]
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.write_all(&[value])
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: usize) -> Result<()> {
        debug_assert!(node_size <= 0x7F);
        self.write_u8(node_size as u8)
    }
}

impl VarCursor {
    #[inline]
    pub fn _write_u64_le(&mut self, value: u64) -> Result<()> {
        let mut buf = [0; 8];
        buf[0..].copy_from_slice(&value.to_le_bytes());
        self.write_all(&buf)
    }
}

#[cfg(feature = "vf_u32u32")]
impl VarFile {
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

#[cfg(feature = "vf_u32u32")]
impl VarCursor {
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
    pub fn read_key_len(&mut self) -> Result<u64> {
        Ok(self.read_u32_le()? as u64)
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u64> {
        Ok(self.read_u32_le()? as u64)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: usize) -> Result<()> {
        debug_assert!(key_len <= u32::MAX as usize);
        self.write_u32_le(key_len as u32)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: usize) -> Result<()> {
        debug_assert!(value_len <= u32::MAX as usize);
        self.write_u32_le(value_len as u32)
    }
}

#[cfg(feature = "vf_u32u32")]
impl VarFile {
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        Ok(self.read_u32_le()? as u64)
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        Ok(self.read_u32_le()? as u64)
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
}

#[cfg(feature = "vf_u32u32")]
impl VarCursor {
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
}

#[cfg(feature = "vf_u64u64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: usize) -> Result<()> {
        debug_assert!(key_len <= u64::MAX as usize);
        self.write_u64_le(key_len as u64)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: usize) -> Result<()> {
        debug_assert!(value_len <= u64::MAX as usize);
        self.write_u64_le(value_len as u64)
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarFile {
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        self.read_u64_le()
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        self.write_u64_le(key_offset)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_u64_le(node_offset)
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarCursor {
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        self.write_u64_le(key_offset)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_u64_le(node_offset)
    }
}

#[cfg(feature = "vf_v64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u64> {
        super::v64::decode_v64(&mut self.buf_file)
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u64> {
        super::v64::decode_v64(&mut self.buf_file)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: usize) -> Result<()> {
        debug_assert!(key_len <= u64::MAX as usize);
        self.write_all(v64::encode(key_len as u64).as_ref())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: usize) -> Result<()> {
        debug_assert!(value_len <= u64::MAX as usize);
        self.write_all(v64::encode(value_len as u64).as_ref())
    }
}

#[cfg(feature = "vf_v64")]
impl VarFile {
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        super::v64::decode_v64(&mut self.buf_file)
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        super::v64::decode_v64(&mut self.buf_file)
    }
    #[inline]
    pub fn _write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        self.write_all(v64::encode(key_offset).as_ref())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_all(v64::encode(node_offset).as_ref())
    }
}

#[cfg(feature = "vf_v64")]
impl VarCursor {
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        self.write_all(v64::encode(key_offset).as_ref())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        self.write_all(v64::encode(node_offset).as_ref())
    }
}

#[cfg(feature = "vf_vint64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u64> {
        super::vint64::decode_vint64(&mut self.buf_file, &mut self.enc_buf)
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u64> {
        super::vint64::decode_vint64(&mut self.buf_file, &mut self.enc_buf)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: usize) -> Result<()> {
        debug_assert!(key_len <= u64::MAX as usize);
        let enc = vint64::encode(key_len as u64);
        self.write_all(enc.as_ref())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: usize) -> Result<()> {
        debug_assert!(value_len <= u64::MAX as usize);
        let enc = vint64::encode(value_len as u64);
        self.write_all(enc.as_ref())
    }
}

#[cfg(feature = "vf_vint64")]
impl VarFile {
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        super::vint64::decode_vint64(&mut self.buf_file, &mut self.enc_buf)
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        super::vint64::decode_vint64(&mut self.buf_file, &mut self.enc_buf)
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let enc = vint64::encode(key_offset);
        self.write_all(enc.as_ref())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let enc = vint64::encode(node_offset);
        self.write_all(enc.as_ref())
    }
}

#[cfg(feature = "vf_vint64")]
impl VarCursor {
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let enc = vint64::encode(key_offset);
        self.write_all(enc.as_ref())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let enc = vint64::encode(node_offset);
        self.write_all(enc.as_ref())
    }
}

#[cfg(feature = "vf_leb128")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u64> {
        let mut inp = self.bytes();
        super::leb128::decode_varint(&mut inp)
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u64> {
        let mut inp = self.bytes();
        super::leb128::decode_varint(&mut inp)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: usize) -> Result<()> {
        debug_assert!(key_len <= u64::MAX as usize);
        let mut enc_buf = Vec::with_capacity(9);
        super::leb128::encode_varint(key_len as u64, &mut enc_buf);
        self.write_all(&enc_buf)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: usize) -> Result<()> {
        debug_assert!(value_len <= u64::MAX as usize);
        let mut enc_buf = Vec::with_capacity(9);
        super::leb128::encode_varint(value_len as u64, &mut enc_buf);
        self.write_all(&enc_buf)
    }
}

#[cfg(feature = "vf_leb128")]
impl VarFile {
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        let mut inp = self.bytes();
        super::leb128::decode_varint(&mut inp)
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        let mut inp = self.bytes();
        super::leb128::decode_varint(&mut inp)
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let mut enc_buf = Vec::with_capacity(9);
        super::leb128::encode_varint(key_offset, &mut enc_buf);
        self.write_all(&enc_buf)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let mut enc_buf = Vec::with_capacity(9);
        super::leb128::encode_varint(node_offset, &mut enc_buf);
        self.write_all(&enc_buf)
    }
}

#[cfg(feature = "vf_leb128")]
impl VarCursor {
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let mut enc_buf = Vec::with_capacity(9);
        super::leb128::encode_varint(key_offset, &mut enc_buf);
        self.write_all(&enc_buf)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let mut enc_buf = Vec::with_capacity(9);
        super::leb128::encode_varint(node_offset, &mut enc_buf);
        self.write_all(&enc_buf)
    }
}

#[cfg(feature = "vf_sqlvli")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<u64> {
        super::sqlvli::decode_vli(self)
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<u64> {
        super::sqlvli::decode_vli(self)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: usize) -> Result<()> {
        debug_assert!(key_len <= u64::MAX as usize);
        let enc = super::sqlvli::encode_vli(key_len as u64);
        self.write_all(enc.as_ref())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: usize) -> Result<()> {
        debug_assert!(value_len <= u64::MAX as usize);
        let enc = super::sqlvli::encode_vli(value_len as u64);
        self.write_all(enc.as_ref())
    }
}

#[cfg(feature = "vf_sqlvli")]
impl VarFile {
    #[inline]
    pub fn read_key_offset(&mut self) -> Result<u64> {
        super::sqlvli::decode_vli(self)
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<u64> {
        super::sqlvli::decode_vli(self)
    }
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let enc = super::sqlvli::encode_vli(key_offset);
        self.write_all(enc.as_ref())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let enc = super::sqlvli::encode_vli(node_offset);
        self.write_all(enc.as_ref())
    }
}

#[cfg(feature = "vf_sqlvli")]
impl VarCursor {
    #[inline]
    pub fn write_key_offset(&mut self, key_offset: u64) -> Result<()> {
        let enc = super::sqlvli::encode_vli(key_offset);
        self.write_all(enc.as_ref())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: u64) -> Result<()> {
        let enc = super::sqlvli::encode_vli(node_offset);
        self.write_all(enc.as_ref())
    }
}

//--
mod debug {
    #[test]
    fn test_size_of() {
        use super::VarFile;
        //
        #[cfg(target_pointer_width = "64")]
        {
            #[cfg(feature = "vf_u32u32")]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
            #[cfg(feature = "vf_vint64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 136);
            #[cfg(feature = "vf_v64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
        }
        #[cfg(target_pointer_width = "32")]
        {
            #[cfg(feature = "vf_u32u32")]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
            #[cfg(feature = "vf_vint64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 136);
            #[cfg(feature = "vf_v64")]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
        }
    }
}
