use super::buf::BufFile;
use super::semtype::*;
use std::convert::TryInto;
use std::fs::File;
use std::io::{Cursor, Read, Result, Seek, SeekFrom, Write};

#[cfg(feature = "vf_vu64")]
use super::vu64_io::{ReadVu64, WriteVu64};

/// Variable length integer access for a random access file.
#[derive(Debug)]
pub struct VarFile {
    buf_file: BufFile,
}

#[derive(Debug)]
pub struct VarCursor {
    buf_cursor: Cursor<Vec<u8>>,
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
    ///
    pub fn seek_from_start<T>(&mut self, offset: Offset<T>) -> Result<Offset<T>> {
        self.seek(SeekFrom::Start(offset.as_value()))
            .map(Offset::<T>::new)
    }
    ///
    pub fn set_len<T>(&mut self, size: Offset<T>) -> Result<()> {
        self.buf_file.set_len(size.as_value())
    }
    ///
    pub fn write_all_small(&mut self, buf: &mut [u8]) -> Result<()> {
        self.buf_file.write_all_small(buf)
    }
    ///
    pub fn write_zero(&mut self, size: usize) -> Result<()> {
        self.buf_file.write_zero(size)
    }
    ///
    pub fn write_node_clear(&mut self, node_offset: NodeOffset, node_size: NodeSize) -> Result<()> {
        let _ = self.seek_from_start(node_offset)?;
        self.write_zero(node_size.try_into().unwrap())?;
        let _ = self.seek_from_start(node_offset)?;
        self.write_node_size(node_size)?;
        Ok(())
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
    pub fn write_u16_le(&mut self, value: u16) -> Result<()> {
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
    pub fn read_key_len(&mut self) -> Result<KeyLength> {
        self.read_u32_le().map(KeyLength::new)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_u32_le(key_len.as_value())
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<ValueLength> {
        self.read_u32_le().map(ValueLength::new)
    }
    //
    #[inline]
    pub fn read_free_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u32_le().map(|o| RecordOffset::new(o as u64))
    }
    #[inline]
    pub fn write_free_record_offset(&mut self, offset: RecordOffset) -> Result<()> {
        debug_assert!(offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(offset.as_value() as u32)
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<RecordSize> {
        self.read_u32_le().map(RecordSize::new)
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: RecordSize) -> Result<()> {
        self.write_u32_le(record_size.as_value())
    }
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u32_le().map(|o| RecordOffset::new(o as u64))
    }
    //
    #[inline]
    pub fn read_free_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u32_le().map(|o| NodeOffset::new(o as u64))
    }
    #[inline]
    pub fn write_free_node_offset(&mut self, offset: NodeOffset) -> Result<()> {
        debug_assert!(offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(offset.as_value() as u32)
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u32_le().map(|n| NodeOffset::new(n as u64))
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodeSize> {
        self.read_u32_le().map(NodeSize::new)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodeSize) -> Result<()> {
        self.write_u32_le(node_size.as_value())
    }
    #[inline]
    pub fn read_keys_count(&mut self) -> Result<KeysCount> {
        self.read_u16_le().map(KeysCount::new)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_len: KeysCount) -> Result<()> {
        self.write_u16_le(keys_len.as_value())
    }
}

#[cfg(feature = "vf_u32u32")]
impl VarCursor {
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_u32_le(key_len.as_value())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_u32_le(value_len.as_value())
    }
    #[inline]
    pub fn write_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        debug_assert!(record_offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(record_offset.as_value() as u32)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        debug_assert!(node_offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(node_offset.as_value() as u32)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_len: KeysCount) -> Result<()> {
        self.write_u16_le(keys_len.as_value())
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<KeyLength> {
        self.read_u32_le().map(KeyLength::new)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_u32_le(key_len.as_value())
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<ValueLength> {
        self.read_u32_le().map(ValueLength::new)
    }
    //
    #[inline]
    pub fn read_free_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u64_le().map(RecordOffset::new)
    }
    #[inline]
    pub fn write_free_record_offset(&mut self, offset: RecordOffset) -> Result<()> {
        self.write_u64_le(offset.as_value())
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<RecordSize> {
        self.read_u32_le().map(RecordSize::new)
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: RecordSize) -> Result<()> {
        self.write_u32_le(record_size.as_value())
    }
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u64_le().map(RecordOffset::new)
    }
    //
    #[inline]
    pub fn read_free_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u64_le().map(NodeOffset::new)
    }
    #[inline]
    pub fn write_free_node_offset(&mut self, offset: NodeOffset) -> Result<()> {
        self.write_u64_le(offset.as_value())
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u64_le().map(NodeOffset::new)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodeSize> {
        self.read_u32_le().map(NodeSize::new)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodeSize) -> Result<()> {
        self.write_u32_le(node_size.as_value())
    }
    #[inline]
    pub fn read_keys_count(&mut self) -> Result<KeysCount> {
        self.read_u16_le().map(KeysCount::new)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_len: KeysCount) -> Result<()> {
        self.write_u16_le(keys_len.as_value())
    }
}

#[cfg(feature = "vf_u64u64")]
impl VarCursor {
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_u32_le(key_len.as_value())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_u32_le(value_len.as_value())
    }
    #[inline]
    pub fn write_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        self.write_u64_le(record_offset.as_value())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        self.write_u64_le(node_offset.as_value())
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_len: KeysCount) -> Result<()> {
        self.write_u16_le(keys_len.as_value())
    }
}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_vu64_u16(&mut self) -> Result<u16> {
        self.buf_file.read_and_decode_vu64().map(|n| n as u16)
    }
    #[inline]
    pub fn read_vu64_u32(&mut self) -> Result<u32> {
        self.buf_file.read_and_decode_vu64().map(|n| n as u32)
    }
    #[inline]
    pub fn read_vu64_u64(&mut self) -> Result<u64> {
        self.buf_file.read_and_decode_vu64()
    }
    #[inline]
    pub fn write_vu64_u16(&mut self, value: u16) -> Result<()> {
        self.buf_file.encode_and_write_vu64(value as u64)
    }
    #[inline]
    pub fn write_vu64_u32(&mut self, value: u32) -> Result<()> {
        self.buf_file.encode_and_write_vu64(value as u64)
    }
    #[inline]
    pub fn _write_vu64_u64(&mut self, value: u64) -> Result<()> {
        self.buf_file.encode_and_write_vu64(value)
    }
}

#[cfg(feature = "vf_vu64")]
impl VarCursor {
    #[inline]
    pub fn write_vu64_u16(&mut self, value: u16) -> Result<()> {
        self.encode_and_write_vu64(value as u64)
    }
    #[inline]
    pub fn write_vu64_u32(&mut self, value: u32) -> Result<()> {
        self.encode_and_write_vu64(value as u64)
    }
    #[inline]
    pub fn write_vu64_u64(&mut self, value: u64) -> Result<()> {
        self.encode_and_write_vu64(value)
    }
}

#[cfg(feature = "vf_vu64")]
impl WriteVu64 for VarCursor {}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<KeyLength> {
        self.read_vu64_u32().map(KeyLength::new)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_vu64_u32(key_len.as_value())
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<ValueLength> {
        self.read_vu64_u32().map(ValueLength::new)
    }
    //
    #[inline]
    pub fn read_free_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u64_le().map(RecordOffset::new)
    }
    #[inline]
    pub fn write_free_record_offset(&mut self, offset: RecordOffset) -> Result<()> {
        self.write_u64_le(offset.as_value())
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<RecordSize> {
        self.read_vu64_u32().map(RecordSize::new)
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: RecordSize) -> Result<()> {
        self.write_vu64_u32(record_size.as_value())
    }
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_vu64_u64().map(RecordOffset::new)
    }
    //
    #[inline]
    pub fn read_free_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u64_le().map(NodeOffset::new)
    }
    #[inline]
    pub fn write_free_node_offset(&mut self, offset: NodeOffset) -> Result<()> {
        self.write_u64_le(offset.as_value())
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_vu64_u64().map(NodeOffset::new)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodeSize> {
        self.read_vu64_u32().map(NodeSize::new)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodeSize) -> Result<()> {
        self.write_vu64_u32(node_size.as_value())
    }
    #[inline]
    pub fn read_keys_count(&mut self) -> Result<KeysCount> {
        self.read_vu64_u16().map(KeysCount::new)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_count: KeysCount) -> Result<()> {
        self.write_vu64_u16(keys_count.as_value())
    }
}

#[cfg(feature = "vf_vu64")]
impl VarCursor {
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_vu64_u32(key_len.as_value())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_vu64_u32(value_len.as_value())
    }
    #[inline]
    pub fn write_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        self.write_vu64_u64(record_offset.as_value())
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        self.write_vu64_u64(node_offset.as_value())
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_count: KeysCount) -> Result<()> {
        self.write_vu64_u16(keys_count.as_value())
    }
}

//--
#[cfg(test)]
mod debug {
    use super::VarFile;
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            #[cfg(not(feature = "buf_stats"))]
            assert_eq!(std::mem::size_of::<VarFile>(), 120);
            #[cfg(feature = "buf_stats")]
            assert_eq!(std::mem::size_of::<VarFile>(), 128);
        }
        #[cfg(target_pointer_width = "32")]
        {
            #[cfg(not(feature = "buf_stats"))]
            assert_eq!(std::mem::size_of::<VarFile>(), 76);
            #[cfg(feature = "buf_stats")]
            assert_eq!(std::mem::size_of::<VarFile>(), 84);
        }
    }
}
