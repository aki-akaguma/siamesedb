use super::semtype::*;
use rabuf::BufFile;
use rabuf::{FileSetLen, FileSync, SmallWrite};
use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom, Write};

#[cfg(not(feature = "vf_u64u64"))]
use std::convert::TryInto;

#[cfg(feature = "vf_vu64")]
use rabuf::SmallRead;

#[cfg(feature = "vf_vu64")]
use vu64::io::{ReadVu64, WriteVu64};

/// Variable length integer access for a random access file.
#[derive(Debug)]
pub struct VarFile {
    buf_file: BufFile,
}

impl VarFile {
    /// Creates a new VarFile.
    #[allow(dead_code)]
    pub fn new(file: File) -> Result<VarFile> {
        Ok(Self {
            buf_file: BufFile::new(file)?,
        })
    }
    /// Creates a new VarFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    #[allow(dead_code)]
    pub fn with_capacity(file: File, chunk_size: u32, max_num_chunks: u16) -> Result<VarFile> {
        debug_assert!(chunk_size == rabuf::roundup_powerof2(chunk_size));
        Ok(Self {
            buf_file: BufFile::with_capacity(file, chunk_size, max_num_chunks)?,
        })
    }
    /// Creates a new VarFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    #[allow(dead_code)]
    pub fn with_per_mille(file: File, chunk_size: u32, per_mille: u16) -> Result<VarFile> {
        debug_assert!(chunk_size == rabuf::roundup_powerof2(chunk_size));
        Ok(Self {
            buf_file: BufFile::with_per_mille(file, chunk_size, per_mille)?,
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
    pub fn _clear(&mut self) -> Result<()> {
        self.buf_file.clear()
    }
    ///
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        self.buf_file.buf_stats()
    }
    ///
    pub fn seek_from_start<T: PartialEq + Copy>(&mut self, offset: Offset<T>) -> Result<Offset<T>> {
        let pos = self
            .seek(SeekFrom::Start(offset.into()))
            .map(Offset::<T>::new)?;
        debug_assert!(pos == offset, "_pos: {} == offset: {}", pos, offset);
        Ok(pos)
    }
    pub fn seek_skip_length<T: PartialEq + Copy>(
        &mut self,
        length: Length<T>,
    ) -> Result<Offset<T>> {
        let val: u32 = length.into();
        self.seek(SeekFrom::Current(val as i64))
            .map(Offset::<T>::new)
    }
    pub fn seek_to_end<T>(&mut self) -> Result<Offset<T>> {
        self.seek(SeekFrom::End(0)).map(Offset::<T>::new)
    }
    pub fn seek_position<T>(&mut self) -> Result<Offset<T>> {
        self.seek(SeekFrom::Current(0)).map(Offset::<T>::new)
    }
    ///
    pub fn set_file_length<T>(&mut self, file_length: Offset<T>) -> Result<()> {
        self.buf_file.set_len(file_length.into())
    }
    ///
    pub fn _write_all_small(&mut self, buf: &[u8]) -> Result<()> {
        self.buf_file.write_all_small(buf)
    }
    ///
    pub fn write_zero<T>(&mut self, size: Size<T>) -> Result<()> {
        self.buf_file.write_zero(size.into())
    }
    pub fn write_zero_to_offset<T: PartialOrd>(&mut self, offset: Offset<T>) -> Result<()> {
        let start_offset = self.seek_position()?;
        if offset > start_offset {
            let size = offset - start_offset;
            self.buf_file.write_zero(size.into())
        } else {
            Ok(())
        }
    }
    ///
    pub fn write_node_clear(&mut self, node_offset: NodeOffset, node_size: NodeSize) -> Result<()> {
        debug_assert!(!node_size.is_zero());
        #[cfg(debug_assertions)]
        {
            self.seek_from_start(node_offset)?;
            let _node_size = self.read_node_size()?;
            debug_assert!(
                _node_size.is_zero() || node_size == _node_size,
                "node_size: {} == _node_size: {}, offset: {}",
                node_size,
                _node_size,
                node_offset
            );
        }
        self.seek_from_start(node_offset)?;
        self.write_zero(node_size)?;
        self.seek_from_start(node_offset)?;
        self.write_node_size(node_size)?;
        Ok(())
    }
    ///
    pub fn write_record_clear(
        &mut self,
        record_offset: RecordOffset,
        record_size: RecordSize,
    ) -> Result<()> {
        self.seek_from_start(record_offset)?;
        self.write_zero(record_size)?;
        self.seek_from_start(record_offset)?;
        self.write_record_size(record_size)?;
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

#[cfg(feature = "vf_u32u32")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<KeyLength> {
        self.read_u32_le().map(KeyLength::new)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_u32_le(key_len.into())
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<ValueLength> {
        self.read_u32_le().map(ValueLength::new)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_u32_le(value_len.into())
    }
    //
    #[inline]
    pub fn read_free_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u32_le().map(|o| RecordOffset::new(o as u64))
    }
    #[inline]
    pub fn write_free_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        debug_assert!(record_offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(
            record_offset.try_into().unwrap_or_else(|err| {
                panic!("record_offset: {}: {}", record_offset.as_value(), err)
            }),
        )
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<RecordSize> {
        self.read_u32_le().map(RecordSize::new)
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: RecordSize) -> Result<()> {
        self.write_u32_le(record_size.into())
    }
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u32_le().map(|o| RecordOffset::new(o as u64))
    }
    #[inline]
    pub fn write_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        debug_assert!(record_offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(
            record_offset.try_into().unwrap_or_else(|err| {
                panic!("record_offset: {}: {}", record_offset.as_value(), err)
            }),
        )
    }
    //
    #[inline]
    pub fn read_free_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u32_le().map(|o| NodeOffset::new(o as u64))
    }
    #[inline]
    pub fn write_free_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        debug_assert!(node_offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(
            node_offset
                .try_into()
                .unwrap_or_else(|err| panic!("node_offset: {}: {}", node_offset.as_value(), err)),
        )
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u32_le().map(|n| NodeOffset::new(n as u64))
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        debug_assert!(node_offset.as_value() <= u32::MAX as u64);
        self.write_u32_le(
            node_offset
                .try_into()
                .unwrap_or_else(|err| panic!("node_offset: {}: {}", node_offset.as_value(), err)),
        )
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodeSize> {
        self.read_u32_le().map(NodeSize::new)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodeSize) -> Result<()> {
        self.write_u32_le(node_size.into())
    }
    #[inline]
    pub fn read_keys_count(&mut self) -> Result<KeysCount> {
        self.read_u16_le().map(KeysCount::new)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_len: KeysCount) -> Result<()> {
        self.write_u16_le(keys_len.into())
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
        self.write_u32_le(key_len.into())
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<ValueLength> {
        self.read_u32_le().map(ValueLength::new)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_u32_le(value_len.into())
    }
    //
    #[inline]
    pub fn read_free_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u64_le().map(RecordOffset::new)
    }
    #[inline]
    pub fn write_free_record_offset(&mut self, offset: RecordOffset) -> Result<()> {
        self.write_u64_le(offset.into())
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<RecordSize> {
        self.read_u32_le().map(RecordSize::new)
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: RecordSize) -> Result<()> {
        self.write_u32_le(record_size.into())
    }
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_u64_le().map(RecordOffset::new)
    }
    #[inline]
    pub fn write_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        self.write_u64_le(record_offset.into())
    }
    //
    #[inline]
    pub fn read_free_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u64_le().map(NodeOffset::new)
    }
    #[inline]
    pub fn write_free_node_offset(&mut self, offset: NodeOffset) -> Result<()> {
        self.write_u64_le(offset.into())
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u64_le().map(NodeOffset::new)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        self.write_u64_le(node_offset.into())
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodeSize> {
        self.read_u32_le().map(NodeSize::new)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodeSize) -> Result<()> {
        self.write_u32_le(node_size.into())
    }
    #[inline]
    pub fn read_keys_count(&mut self) -> Result<KeysCount> {
        self.read_u16_le().map(KeysCount::new)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_len: KeysCount) -> Result<()> {
        self.write_u16_le(keys_len.into())
    }
}

#[cfg(feature = "vf_vu64")]
impl ReadVu64 for VarFile {
    fn read_one_byte(&mut self) -> Result<u8> {
        self.buf_file.read_one_byte()
    }
    fn read_exact_max8byte(&mut self, buf: &mut [u8]) -> Result<()> {
        debug_assert!(buf.len() <= 8, "buf.len(): {} <= 8", buf.len());
        self.buf_file.read_exact_small(buf)
    }
}

#[cfg(feature = "vf_vu64")]
impl WriteVu64 for VarFile {}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_vu64_u16(&mut self) -> Result<u16> {
        self.read_and_decode_vu64().map(|n| {
            n.try_into()
                .unwrap_or_else(|err| panic!("n:{} :{}", n, err))
        })
    }
    #[inline]
    pub fn read_vu64_u32(&mut self) -> Result<u32> {
        self.read_and_decode_vu64().map(|n| {
            n.try_into()
                .unwrap_or_else(|err| panic!("n:{} :{}", n, err))
        })
    }
    #[inline]
    pub fn read_vu64_u64(&mut self) -> Result<u64> {
        self.read_and_decode_vu64()
    }
    #[inline]
    pub fn write_vu64_u16(&mut self, value: u16) -> Result<()> {
        self.encode_and_write_vu64(value.into())
    }
    #[inline]
    pub fn write_vu64_u32(&mut self, value: u32) -> Result<()> {
        self.encode_and_write_vu64(value.into())
    }
    #[inline]
    pub fn write_vu64_u64(&mut self, value: u64) -> Result<()> {
        self.encode_and_write_vu64(value)
    }
}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_key_len(&mut self) -> Result<KeyLength> {
        self.read_vu64_u32().map(KeyLength::new)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_vu64_u32(key_len.into())
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_vu64_u32(value_len.into())
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
        self.write_u64_le(offset.into())
    }
    #[inline]
    pub fn read_record_size(&mut self) -> Result<RecordSize> {
        self.read_vu64_u32().map(RecordSize::new)
    }
    #[inline]
    pub fn write_record_size(&mut self, record_size: RecordSize) -> Result<()> {
        self.write_vu64_u32(record_size.into())
    }
    #[inline]
    pub fn read_record_offset(&mut self) -> Result<RecordOffset> {
        self.read_vu64_u64().map(RecordOffset::new)
    }
    #[inline]
    pub fn write_record_offset(&mut self, record_offset: RecordOffset) -> Result<()> {
        self.write_vu64_u64(record_offset.into())
    }
    //
    #[inline]
    pub fn read_free_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_u64_le().map(NodeOffset::new)
    }
    #[inline]
    pub fn write_free_node_offset(&mut self, offset: NodeOffset) -> Result<()> {
        self.write_u64_le(offset.into())
    }
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodeOffset> {
        self.read_vu64_u64().map(NodeOffset::new)
    }
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodeOffset) -> Result<()> {
        self.write_vu64_u64(node_offset.into())
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodeSize> {
        self.read_vu64_u32().map(NodeSize::new)
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodeSize) -> Result<()> {
        debug_assert!(!node_size.is_zero());
        self.write_vu64_u32(node_size.into())
    }
    #[inline]
    pub fn read_keys_count(&mut self) -> Result<KeysCount> {
        self.read_vu64_u16().map(KeysCount::new)
    }
    #[inline]
    pub fn write_keys_count(&mut self, keys_count: KeysCount) -> Result<()> {
        self.write_vu64_u16(keys_count.into())
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
            #[cfg(not(any(feature = "buf_stats", feature = "buf_lru")))]
            {
                #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                assert_eq!(std::mem::size_of::<VarFile>(), 76);
                #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                assert_eq!(std::mem::size_of::<VarFile>(), 88);
            }
            #[cfg(all(feature = "buf_stats", feature = "buf_lru"))]
            {
                #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                assert_eq!(std::mem::size_of::<VarFile>(), 88);
                #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                assert_eq!(std::mem::size_of::<VarFile>(), 96);
            }
            #[cfg(all(feature = "buf_stats", not(feature = "buf_lru")))]
            {
                #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                assert_eq!(std::mem::size_of::<VarFile>(), 84);
                #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                assert_eq!(std::mem::size_of::<VarFile>(), 96);
            }
            #[cfg(all(not(feature = "buf_stats"), feature = "buf_lru"))]
            {
                #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                assert_eq!(std::mem::size_of::<VarFile>(), 80);
                #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                assert_eq!(std::mem::size_of::<VarFile>(), 88);
            }
        }
    }
}
