use super::piece::PieceMgr;
use super::semtype::*;
use rabuf::{BufFile, FileSetLen, FileSync, MaybeSlice, SmallRead, SmallWrite};
use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom, Write};

#[cfg(feature = "siamese_debug")]
#[cfg(not(feature = "vf_u64u64"))]
use std::convert::TryInto;

#[cfg(feature = "vf_vu64")]
use vu64::io::{ReadVu64, WriteVu64};

/// Variable length integer access for a random access file.
#[derive(Debug)]
pub struct VarFile {
    buf_file: BufFile,
    pub(crate) piece_mgr: PieceMgr,
}

impl VarFile {
    /// Creates a new VarFile.
    #[allow(dead_code)]
    pub fn new(piece_mgr: PieceMgr, name: &str, file: File) -> Result<VarFile> {
        Ok(Self {
            buf_file: BufFile::new(name, file)?,
            piece_mgr,
        })
    }
    /// Creates a new VarFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    #[allow(dead_code)]
    pub fn with_capacity(
        piece_mgr: PieceMgr,
        name: &str,
        file: File,
        chunk_size: u32,
        max_num_chunks: u16,
    ) -> Result<VarFile> {
        debug_assert!(chunk_size == rabuf::roundup_powerof2(chunk_size));
        Ok(Self {
            buf_file: BufFile::with_capacity(name, file, chunk_size, max_num_chunks)?,
            piece_mgr,
            //piece_mgr: PieceMgr::new(free_list_offset, size_ary),
        })
    }
    /// Creates a new VarFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    #[allow(dead_code)]
    pub fn with_per_mille(
        piece_mgr: PieceMgr,
        name: &str,
        file: File,
        chunk_size: u32,
        per_mille: u16,
    ) -> Result<VarFile> {
        debug_assert!(chunk_size == rabuf::roundup_powerof2(chunk_size));
        Ok(Self {
            buf_file: BufFile::with_per_mille(name, file, chunk_size, per_mille)?,
            piece_mgr,
            //piece_mgr: PieceMgr::new(free_list_offset, size_ary),
        })
    }
    //
    #[inline]
    pub fn sync_all(&mut self) -> Result<()> {
        self.buf_file.sync_all()
    }
    //
    #[inline]
    pub fn sync_data(&mut self) -> Result<()> {
        self.buf_file.sync_data()
    }
    //
    #[inline]
    pub fn _clear(&mut self) -> Result<()> {
        self.buf_file.clear()
    }
    //
    #[cfg(feature = "buf_stats")]
    pub fn buf_stats(&self) -> Vec<(String, i64)> {
        self.buf_file.buf_stats()
    }
    //
    #[inline]
    pub fn prepare<T>(&mut self, offset: Offset<T>) -> Result<()> {
        self.buf_file.prepare(offset.into())
    }
    //
    #[inline]
    pub fn seek_from_start<T: PartialEq + Copy>(&mut self, offset: Offset<T>) -> Result<Offset<T>> {
        let pos = self
            .seek(SeekFrom::Start(offset.into()))
            .map(Offset::<T>::new)?;
        debug_assert!(pos == offset, "_pos: {} == offset: {}", pos, offset);
        self.prepare(offset)?;
        Ok(pos)
    }
    #[cfg(any(feature = "vf_vu64", feature = "vf_u64u64"))]
    #[inline]
    pub fn seek_skip_length<T: PartialEq + Copy>(
        &mut self,
        length: Length<T>,
    ) -> Result<Offset<T>> {
        let val: u32 = length.into();
        self.seek(SeekFrom::Current(val as i64))
            .map(Offset::<T>::new)
    }
    #[inline]
    pub fn _seek_skip_size<T: PartialEq + Copy>(&mut self, size: Size<T>) -> Result<Offset<T>> {
        let val: u32 = size.into();
        self.seek(SeekFrom::Current(val as i64))
            .map(Offset::<T>::new)
    }
    #[inline]
    pub fn seek_to_end<T>(&mut self) -> Result<Offset<T>> {
        self.seek(SeekFrom::End(0)).map(Offset::<T>::new)
    }
    #[inline]
    pub fn seek_position<T>(&mut self) -> Result<Offset<T>> {
        self.stream_position().map(Offset::<T>::new)
    }
    //
    #[inline]
    pub fn set_file_length<T>(&mut self, file_length: Offset<T>) -> Result<()> {
        self.buf_file.set_len(file_length.into())
    }
    #[inline]
    pub fn read_fill_buffer(&mut self) -> Result<()> {
        self.buf_file.read_fill_buffer()
    }
    //
    #[inline]
    pub fn _write_all_small(&mut self, buf: &[u8]) -> Result<()> {
        self.buf_file.write_all_small(buf)
    }
    //
    #[inline]
    pub fn _write_zero<T>(&mut self, size: Size<T>) -> Result<()> {
        self.buf_file.write_zero(size.into())
    }
    #[inline]
    pub fn write_zero_to_offset<T: PartialOrd>(&mut self, offset: Offset<T>) -> Result<()> {
        let start_offset = self.seek_position()?;
        if offset > start_offset {
            let size = offset - start_offset;
            self.buf_file.write_zero(size.into())
        } else {
            Ok(())
        }
    }
    //
    #[inline]
    pub fn write_piece_clear<T: Copy + PartialEq + PartialOrd>(
        &mut self,
        offset: PieceOffset<T>,
        size: PieceSize<T>,
    ) -> Result<()> {
        debug_assert!(!size.is_zero());
        #[cfg(debug_assertions)]
        {
            self.seek_from_start(offset)?;
            let _piece_size = self.read_piece_size()?;
            debug_assert!(
                _piece_size.is_zero() || size == _piece_size,
                "size: {} == _piece_size: {}, offset: {}",
                size,
                _piece_size,
                offset
            );
        }
        self.seek_from_start(offset)?;
        self.write_piece_size(size)?;
        self.write_zero_to_offset(offset + size)?;
        Ok(())
    }
    //
    #[inline]
    pub fn write_node_clear(&mut self, offset: NodePieceOffset, size: NodePieceSize) -> Result<()> {
        debug_assert!(!size.is_zero());
        #[cfg(debug_assertions)]
        {
            self.seek_from_start(offset)?;
            let _piece_size = self.read_piece_size()?;
            debug_assert!(
                _piece_size.is_zero() || size == _piece_size,
                "size: {} == _piece_size: {}, offset: {}",
                size,
                _piece_size,
                offset
            );
        }
        self.seek_from_start(offset)?;
        self.write_node_size(size)?;
        self.write_zero_to_offset(offset + size)?;
        Ok(())
    }
}

impl Read for VarFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.buf_file.read(buf)
    }
}

impl Write for VarFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buf_file.write(buf)
    }
    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.buf_file.flush()
    }
}

impl Seek for VarFile {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.buf_file.seek(pos)
    }
}

impl rabuf::SmallRead for VarFile {
    #[inline]
    fn read_u8(&mut self) -> Result<u8> {
        self.buf_file.read_u8()
    }
    #[inline]
    fn read_u16_le(&mut self) -> Result<u16> {
        self.buf_file.read_u16_le()
    }
    #[inline]
    fn read_u32_le(&mut self) -> Result<u32> {
        self.buf_file.read_u32_le()
    }
    #[inline]
    fn read_u64_le(&mut self) -> Result<u64> {
        self.buf_file.read_u64_le()
    }
    #[inline]
    fn read_max_8_bytes(&mut self, size: usize) -> Result<u64> {
        self.buf_file.read_max_8_bytes(size)
    }
    #[inline]
    fn read_exact_small(&mut self, buf: &mut [u8]) -> Result<()> {
        self.buf_file.read_exact_small(buf)
    }
    #[inline]
    fn read_exact_maybeslice(&mut self, size: usize) -> Result<MaybeSlice> {
        self.buf_file.read_exact_maybeslice(size)
    }
}

impl rabuf::SmallWrite for VarFile {
    #[inline]
    fn write_u8(&mut self, val: u8) -> Result<()> {
        self.buf_file.write_u8(val)
    }
    #[inline]
    fn write_u16_le(&mut self, val: u16) -> Result<()> {
        self.buf_file.write_u16_le(val)
    }
    #[inline]
    fn write_u32_le(&mut self, val: u32) -> Result<()> {
        self.buf_file.write_u32_le(val)
    }
    #[inline]
    fn write_u64_le(&mut self, val: u64) -> Result<()> {
        self.buf_file.write_u64_le(val)
    }
    #[inline]
    fn write_u64_le_slice(&mut self, val_slice: &[u64]) -> Result<()> {
        self.buf_file.write_u64_le_slice(val_slice)
    }
    #[inline]
    fn write_u64_le_slice2(&mut self, val_slice1: &[u64], val_slice2: &[u64]) -> Result<()> {
        self.buf_file.write_u64_le_slice2(val_slice1, val_slice2)
    }
    #[inline]
    fn write_all_small(&mut self, buf: &[u8]) -> Result<()> {
        self.buf_file.write_all_small(buf)
    }
    #[inline]
    fn write_zero(&mut self, size: u32) -> Result<()> {
        self.buf_file.write_zero(size)
    }
}

#[cfg(feature = "vf_node_u32")]
impl VarFile {
    #[inline]
    pub fn read_piece_offset_u32<T>(&mut self) -> Result<PieceOffset<T>> {
        self.read_u32_le().map(|o| PieceOffset::<T>::new(o as u64))
    }
    #[inline]
    pub fn write_piece_offset_u32<T: Copy>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        debug_assert!(piece_offset.as_value() <= u32::MAX as u64);
        #[cfg(feature = "siamese_debug")]
        let val = piece_offset
            .try_into()
            .unwrap_or_else(|err| panic!("piece_offset: {}: {}", piece_offset.as_value(), err));
        #[cfg(not(feature = "siamese_debug"))]
        let val = piece_offset.as_value() as u32;
        //
        self.write_u32_le(val)
    }
    #[inline]
    pub fn read_node_offset_u32(&mut self) -> Result<NodePieceOffset> {
        self.read_u32_le().map(|n| NodePieceOffset::new(n as u64))
    }
    #[inline]
    pub fn write_node_offset_u32(&mut self, node_offset: NodePieceOffset) -> Result<()> {
        debug_assert!(node_offset.as_value() <= u32::MAX as u64);
        #[cfg(feature = "siamese_debug")]
        let val = node_offset
            .try_into()
            .unwrap_or_else(|err| panic!("node_offset: {}: {}", node_offset.as_value(), err));
        #[cfg(not(feature = "siamese_debug"))]
        let val = node_offset.as_value() as u32;
        //
        self.write_u32_le(val)
    }
}

#[cfg(feature = "vf_node_u64")]
impl VarFile {
    #[inline]
    pub fn read_piece_offset_u64<T>(&mut self) -> Result<PieceOffset<T>> {
        self.read_u64_le().map(PieceOffset::new)
    }
    #[cfg(not(feature = "idx_straight"))]
    #[inline]
    pub fn write_piece_offset_u64<T: Copy>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        self.write_u64_le(piece_offset.into())
    }
    #[cfg(feature = "idx_straight")]
    #[inline]
    pub fn write_piece_offset_u64_slice<T: Copy>(
        &mut self,
        piece_offset_slice: &[PieceOffset<T>],
    ) -> Result<()> {
        let u64_slice =
            unsafe { std::mem::transmute::<&[PieceOffset<T>], &[u64]>(piece_offset_slice) };
        self.write_u64_le_slice(u64_slice)
    }
    #[inline]
    pub fn read_node_offset_u64(&mut self) -> Result<NodePieceOffset> {
        self.read_u64_le().map(NodePieceOffset::new)
    }
    #[cfg(not(feature = "idx_straight"))]
    #[inline]
    pub fn write_node_offset_u64(&mut self, node_offset: NodePieceOffset) -> Result<()> {
        self.write_u64_le(node_offset.into())
    }
    #[cfg(feature = "idx_straight")]
    #[inline]
    pub fn _write_node_offset_u64_slice(
        &mut self,
        node_offset_slice: &[NodePieceOffset],
    ) -> Result<()> {
        let u64_slice =
            unsafe { std::mem::transmute::<&[NodePieceOffset], &[u64]>(node_offset_slice) };
        self.write_u64_le_slice(u64_slice)
    }
    #[cfg(feature = "idx_straight")]
    #[inline]
    pub fn write_piece_offset_and_node_offset_u64_slice<T: Copy>(
        &mut self,
        piece_offset_slice: &[PieceOffset<T>],
        node_offset_slice: &[NodePieceOffset],
    ) -> Result<()> {
        let u64_slice1 =
            unsafe { std::mem::transmute::<&[PieceOffset<T>], &[u64]>(piece_offset_slice) };
        let u64_slice2 =
            unsafe { std::mem::transmute::<&[NodePieceOffset], &[u64]>(node_offset_slice) };
        self.write_u64_le_slice2(u64_slice1, u64_slice2)
    }
}

#[cfg(feature = "vf_u32u32")]
#[cfg(any(feature = "htx", feature = "idx_straight"))]
impl VarFile {
    #[inline]
    pub fn read_value_piece_offset<T>(&mut self) -> Result<PieceOffset<T>> {
        self.read_u32_le().map(|o| PieceOffset::<T>::new(o as u64))
    }
    #[inline]
    pub fn write_value_piece_offset<T: Copy>(
        &mut self,
        piece_offset: PieceOffset<T>,
    ) -> Result<()> {
        debug_assert!(piece_offset.as_value() <= u32::MAX as u64);
        #[cfg(feature = "siamese_debug")]
        let val = piece_offset
            .try_into()
            .unwrap_or_else(|err| panic!("piece_offset: {}: {}", piece_offset.as_value(), err));
        #[cfg(not(feature = "siamese_debug"))]
        let val = piece_offset.as_value() as u32;
        //
        self.write_u32_le(val)
    }
}

#[cfg(any(feature = "vf_u64u64", feature = "vf_vu64"))]
#[cfg(any(feature = "htx", feature = "idx_straight"))]
impl VarFile {
    #[inline]
    pub fn read_value_piece_offset<T>(&mut self) -> Result<PieceOffset<T>> {
        self.read_u64_le().map(PieceOffset::new)
    }
    #[inline]
    pub fn write_value_piece_offset<T>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        self.write_u64_le(piece_offset.as_value())
    }
}

#[cfg(feature = "vf_u32u32")]
impl VarFile {
    #[inline]
    pub fn read_free_piece_offset<T>(&mut self) -> Result<Offset<T>> {
        self.read_u32_le().map(|o| Offset::<T>::new(o as u64))
    }
    #[inline]
    pub fn write_free_piece_offset<T: Copy>(&mut self, offset: Offset<T>) -> Result<()> {
        debug_assert!(offset.as_value() <= u32::MAX as u64);
        #[cfg(feature = "siamese_debug")]
        let val = offset
            .try_into()
            .unwrap_or_else(|err| panic!("piece_offset: {}: {}", offset.as_value(), err));
        #[cfg(not(feature = "siamese_debug"))]
        let val = offset.as_value() as u32;
        //
        self.write_u32_le(val)
    }
    //
    #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
    #[inline]
    pub fn read_piece_offset<T>(&mut self) -> Result<PieceOffset<T>> {
        self.read_u32_le().map(|o| PieceOffset::<T>::new(o as u64))
    }
    #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
    #[inline]
    pub fn write_piece_offset<T: Copy>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        debug_assert!(piece_offset.as_value() <= u32::MAX as u64);
        #[cfg(feature = "siamese_debug")]
        let val = piece_offset
            .try_into()
            .unwrap_or_else(|err| panic!("piece_offset: {}: {}", piece_offset.as_value(), err));
        #[cfg(not(feature = "siamese_debug"))]
        let val = piece_offset.as_value() as u32;
        //
        self.write_u32_le(val)
    }
    #[inline]
    pub fn read_piece_size<T>(&mut self) -> Result<PieceSize<T>> {
        self.read_u32_le().map(PieceSize::<T>::new)
    }
    #[inline]
    pub fn write_piece_size<T>(&mut self, piece_size: PieceSize<T>) -> Result<()> {
        self.write_u32_le(piece_size.into())
    }
    //
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
    pub fn _read_node_offset(&mut self) -> Result<NodePieceOffset> {
        self.read_u32_le().map(|n| NodePieceOffset::new(n as u64))
    }
    #[inline]
    pub fn _write_node_offset(&mut self, node_offset: NodePieceOffset) -> Result<()> {
        debug_assert!(node_offset.as_value() <= u32::MAX as u64);
        #[cfg(feature = "siamese_debug")]
        let val = node_offset
            .try_into()
            .unwrap_or_else(|err| panic!("node_offset: {}: {}", node_offset.as_value(), err));
        #[cfg(not(feature = "siamese_debug"))]
        let val = node_offset.as_value() as u32;
        //
        self.write_u32_le(val)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodePieceSize> {
        self.read_u16_le().map(|n| NodePieceSize::new(n as u32))
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodePieceSize) -> Result<()> {
        debug_assert!(node_size.as_value() <= u16::MAX as u32);
        self.write_u16_le(node_size.as_value() as u16)
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
    pub fn read_free_piece_offset<T>(&mut self) -> Result<Offset<T>> {
        self.read_u64_le().map(Offset::<T>::new)
    }
    #[inline]
    pub fn write_free_piece_offset<T>(&mut self, offset: Offset<T>) -> Result<()> {
        self.write_u64_le(offset.into())
    }
    //
    #[cfg(any(
        not(any(feature = "htx", feature = "idx_straight")),
        not(any(feature = "vf_node_u32", feature = "vf_node_u64"))
    ))]
    #[inline]
    pub fn read_piece_offset<T>(&mut self) -> Result<PieceOffset<T>> {
        self.read_u64_le().map(PieceOffset::<T>::new)
    }
    #[cfg(any(
        not(any(feature = "htx", feature = "idx_straight")),
        not(any(feature = "vf_node_u32", feature = "vf_node_u64"))
    ))]
    #[inline]
    pub fn write_piece_offset<T>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        self.write_u64_le(piece_offset.into())
    }
    #[inline]
    pub fn read_piece_size<T>(&mut self) -> Result<PieceSize<T>> {
        self.read_u32_le().map(PieceSize::<T>::new)
    }
    #[inline]
    pub fn write_piece_size<T>(&mut self, piece_size: PieceSize<T>) -> Result<()> {
        self.write_u32_le(piece_size.into())
    }
    //
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
    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodePieceOffset> {
        self.read_u64_le().map(NodePieceOffset::new)
    }
    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodePieceOffset) -> Result<()> {
        self.write_u64_le(node_offset.into())
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodePieceSize> {
        self.read_u16_le().map(|n| NodePieceSize::new(n as u32))
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodePieceSize) -> Result<()> {
        debug_assert!(node_size.as_value() <= u16::MAX as u32);
        self.write_u16_le(node_size.as_value() as u16)
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
    #[inline]
    fn read_one_byte(&mut self) -> Result<u8> {
        self.buf_file.read_u8()
    }
    #[inline]
    fn read_exact_max8byte(&mut self, buf: &mut [u8]) -> Result<()> {
        debug_assert!(buf.len() <= 8, "buf.len(): {} <= 8", buf.len());
        self.buf_file.read_exact_small(buf)
    }
    /// reads `vu64` bytes and decods it to `u64`
    fn read_and_decode_vu64(&mut self) -> Result<u64> {
        /*
        let mut buf = [0u8; vu64::MAX_BYTES-1];
        let byte_1st = self.buf_file.read_u8()?;
        let len = vu64::decoded_len(byte_1st);
        if len > 1 {
            self.buf_file.read_exact_small(&mut buf[..len as usize - 1])?;
        }
        match vu64::decode_with_first_and_follow(len, byte_1st, &buf[..len as usize - 1]) {
            Ok(i) => Ok(i),
            Err(err) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{}", err),
            )),
        }
        */
        /*
        let byte_1st = self.buf_file.read_u8()?;
        let len = vu64::decoded_len(byte_1st);
        let maybe_slice = self.buf_file.read_exact_maybeslice(len as usize - 1)?;
        match vu64::decode_with_first_and_follow(len, byte_1st, &maybe_slice) {
            Ok(i) => Ok(i),
            Err(err) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{}", err),
            )),
        }
        */
        let byte_1st = self.buf_file.read_u8()?;
        if byte_1st < 128 {
            Ok(byte_1st as u64)
        } else {
            let len = vu64::decoded_len(byte_1st);
            let follow_len = len as usize - 1;
            let max_8_bytes = match follow_len {
                0 => 0,
                1 => self.buf_file.read_u8()? as u64,
                2 => self.buf_file.read_u16_le()? as u64,
                //4 => self.buf_file.read_u32_le()? as u64,
                _ => self.buf_file.read_max_8_bytes(follow_len)?,
            };
            match vu64::decode_with_first_and_follow_le(len, byte_1st, max_8_bytes) {
                Ok(i) => Ok(i),
                Err(err) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("{}", err),
                )),
            }
        }
        /*
         */
    }
}

#[cfg(feature = "vf_vu64")]
impl WriteVu64 for VarFile {}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_vu64_u16(&mut self) -> Result<u16> {
        #[cfg(feature = "siamese_debug")]
        let r = self.read_and_decode_vu64().map(|n| {
            n.try_into()
                .unwrap_or_else(|err| panic!("n:{} :{}", n, err))
        });
        #[cfg(not(feature = "siamese_debug"))]
        let r = self.read_and_decode_vu64().map(|n| n as u16);
        r
    }
    #[inline]
    pub fn read_vu64_u32(&mut self) -> Result<u32> {
        #[cfg(feature = "siamese_debug")]
        let r = self.read_and_decode_vu64().map(|n| {
            n.try_into()
                .unwrap_or_else(|err| panic!("n:{} :{}", n, err))
        });
        #[cfg(not(feature = "siamese_debug"))]
        let r = self.read_and_decode_vu64().map(|n| n as u32);
        r
    }
    #[inline]
    pub fn _read_vu64_u64(&mut self) -> Result<u64> {
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
    pub fn _write_vu64_u64(&mut self, value: u64) -> Result<()> {
        self.encode_and_write_vu64(value)
    }
}

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn read_free_piece_offset<T>(&mut self) -> Result<Offset<T>> {
        self.read_u64_le().map(Offset::<T>::new)
    }
    #[inline]
    pub fn write_free_piece_offset<T>(&mut self, offset: Offset<T>) -> Result<()> {
        self.write_u64_le(offset.into())
    }
    //
    #[cfg(any(
        not(any(feature = "htx", feature = "idx_straight")),
        not(any(feature = "vf_node_u32", feature = "vf_node_u64"))
    ))]
    #[inline]
    pub fn read_piece_offset<T>(&mut self) -> Result<PieceOffset<T>> {
        self._read_vu64_u64().map(|v| PieceOffset::<T>::new(v * 8))
    }
    #[cfg(any(
        not(any(feature = "htx", feature = "idx_straight")),
        not(any(feature = "vf_node_u32", feature = "vf_node_u64"))
    ))]
    #[inline]
    pub fn write_piece_offset<T>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        let v: u64 = piece_offset.into();

        debug_assert!(v % 8 == 0);
        self._write_vu64_u64(v / 8)
    }
    /*
    #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
    #[inline]
    pub fn read_piece_offset<T>(&mut self) -> Result<PieceOffset<T>> {
        self._read_vu64_u64().map(PieceOffset::<T>::new)
    }
    #[cfg(not(any(feature = "htx", feature = "idx_straight")))]
    #[inline]
    pub fn write_piece_offset<T>(&mut self, piece_offset: PieceOffset<T>) -> Result<()> {
        let v: u64 = piece_offset.into();
        self._write_vu64_u64(v)
    }
    */
    #[inline]
    pub fn read_piece_size<T>(&mut self) -> Result<PieceSize<T>> {
        self.read_vu64_u32().map(|v| PieceSize::<T>::new(v * 8))
    }
    #[inline]
    pub fn write_piece_size<T>(&mut self, piece_size: PieceSize<T>) -> Result<()> {
        let v: u32 = piece_size.into();
        debug_assert!(v % 8 == 0);
        self.write_vu64_u32(v / 8)
    }
    //
    #[inline]
    pub fn read_key_len(&mut self) -> Result<KeyLength> {
        self.read_vu64_u32().map(KeyLength::new)
    }
    #[inline]
    pub fn write_key_len(&mut self, key_len: KeyLength) -> Result<()> {
        self.write_vu64_u32(key_len.into())
    }
    #[inline]
    pub fn read_value_len(&mut self) -> Result<ValueLength> {
        self.read_vu64_u32().map(ValueLength::new)
    }
    #[inline]
    pub fn write_value_len(&mut self, value_len: ValueLength) -> Result<()> {
        self.write_vu64_u32(value_len.into())
    }
    //
    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
    #[inline]
    pub fn read_node_offset(&mut self) -> Result<NodePieceOffset> {
        self._read_vu64_u64().map(|a| NodePieceOffset::new(a * 8))
    }
    #[cfg(not(any(feature = "vf_node_u32", feature = "vf_node_u64")))]
    #[inline]
    pub fn write_node_offset(&mut self, node_offset: NodePieceOffset) -> Result<()> {
        let v: u64 = node_offset.into();
        debug_assert!(v % 8 == 0);
        self._write_vu64_u64(v / 8)
    }
    #[inline]
    pub fn read_node_size(&mut self) -> Result<NodePieceSize> {
        self.read_vu64_u32().map(|v| NodePieceSize::new(v * 8))
    }
    #[inline]
    pub fn write_node_size(&mut self, node_size: NodePieceSize) -> Result<()> {
        debug_assert!(!node_size.is_zero());
        let v: u32 = node_size.into();
        debug_assert!(v % 8 == 0);
        self.write_vu64_u32(v / 8)
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

#[cfg(feature = "vf_vu64")]
impl VarFile {
    #[inline]
    pub fn seek_skip_to_piece_key<T: Copy + PartialEq>(
        &mut self,
        offset: PieceOffset<T>,
    ) -> Result<PieceOffset<T>> {
        self.seek_from_start(offset)?;
        let byte_1st = self.buf_file.read_u8()?;
        let piece_size_len = vu64::decoded_len(byte_1st);
        if piece_size_len > 1 {
            self.seek_skip_length(KeyLength::new((piece_size_len - 1).into()))?;
        }
        //
        self.seek_position()
    }
    #[inline]
    pub fn seek_skip_to_piece_value<T: Copy + PartialEq>(
        &mut self,
        offset: PieceOffset<T>,
    ) -> Result<PieceOffset<T>> {
        self.seek_from_start(offset)?;
        let byte_1st = self.buf_file.read_u8()?;
        let piece_size_len = vu64::decoded_len(byte_1st);
        if piece_size_len > 1 {
            self.seek_skip_length(KeyLength::new((piece_size_len - 1).into()))?;
        }
        //
        self.seek_position()
    }
}

#[cfg(any(feature = "vf_u32u32", feature = "vf_u64u64"))]
impl VarFile {
    #[inline]
    pub fn seek_skip_to_piece_key<T: Copy + PartialEq>(
        &mut self,
        offset: PieceOffset<T>,
    ) -> Result<PieceOffset<T>> {
        /*
        self.seek_from_start(offset)?;
        self.seek_skip_length(KeyLength::new(4))?;
        //
        self.seek_position()
        */
        self.seek_from_start(offset + PieceSize::<T>::new(4))?;
        self.seek_position()
    }
    #[inline]
    pub fn seek_skip_to_piece_value<T: Copy + PartialEq>(
        &mut self,
        offset: PieceOffset<T>,
    ) -> Result<PieceOffset<T>> {
        /*
        self.seek_from_start(offset)?;
        self.seek_skip_length(KeyLength::new(4))?;
        //
        self.seek_position()
        */
        self.seek_from_start(offset + PieceSize::<T>::new(4))?;
        self.seek_position()
    }
}

//--
#[cfg(not(windows))]
#[cfg(test)]
mod debug {
    use super::VarFile;
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            #[cfg(not(feature = "buf_hash_turbo"))]
            {
                #[cfg(not(feature = "buf_stats"))]
                {
                    #[cfg(not(feature = "buf_print_hits"))]
                    assert_eq!(std::mem::size_of::<VarFile>(), 176);
                    #[cfg(feature = "buf_print_hits")]
                    assert_eq!(std::mem::size_of::<VarFile>(), 200);
                }
                #[cfg(feature = "buf_stats")]
                assert_eq!(std::mem::size_of::<VarFile>(), 184);
            }
            #[cfg(feature = "buf_hash_turbo")]
            {
                #[cfg(not(feature = "buf_stats"))]
                {
                    #[cfg(not(feature = "buf_print_hits"))]
                    assert_eq!(std::mem::size_of::<VarFile>(), 184);
                    #[cfg(feature = "buf_print_hits")]
                    assert_eq!(std::mem::size_of::<VarFile>(), 208);
                }
                #[cfg(feature = "buf_stats")]
                assert_eq!(std::mem::size_of::<VarFile>(), 184);
            }
        }
        #[cfg(target_pointer_width = "32")]
        {
            #[cfg(not(feature = "buf_hash_turbo"))]
            {
                #[cfg(not(any(feature = "buf_stats", feature = "buf_lru")))]
                {
                    #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                    {
                        #[cfg(not(feature = "buf_print_hits"))]
                        assert_eq!(std::mem::size_of::<VarFile>(), 108);
                        #[cfg(feature = "buf_print_hits")]
                        assert_eq!(std::mem::size_of::<VarFile>(), 132);
                    }
                    #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                    {
                        #[cfg(not(feature = "buf_print_hits"))]
                        assert_eq!(std::mem::size_of::<VarFile>(), 120);
                        #[cfg(feature = "buf_print_hits")]
                        assert_eq!(std::mem::size_of::<VarFile>(), 144);
                    }
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
                    assert_eq!(std::mem::size_of::<VarFile>(), 116);
                    #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                    assert_eq!(std::mem::size_of::<VarFile>(), 128);
                }
                #[cfg(all(not(feature = "buf_stats"), feature = "buf_lru"))]
                {
                    #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                    assert_eq!(std::mem::size_of::<VarFile>(), 80);
                    #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                    assert_eq!(std::mem::size_of::<VarFile>(), 88);
                }
            }
            #[cfg(feature = "buf_hash_turbo")]
            {
                #[cfg(not(any(feature = "buf_stats", feature = "buf_lru")))]
                {
                    #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                    {
                        #[cfg(not(feature = "buf_print_hits"))]
                        assert_eq!(std::mem::size_of::<VarFile>(), 112);
                        #[cfg(feature = "buf_print_hits")]
                        assert_eq!(std::mem::size_of::<VarFile>(), 164);
                    }
                    #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                    {
                        #[cfg(not(feature = "buf_print_hits"))]
                        assert_eq!(std::mem::size_of::<VarFile>(), 120);
                        #[cfg(feature = "buf_print_hits")]
                        assert_eq!(std::mem::size_of::<VarFile>(), 176);
                    }
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
                    assert_eq!(std::mem::size_of::<VarFile>(), 116);
                    #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                    assert_eq!(std::mem::size_of::<VarFile>(), 128);
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
}
