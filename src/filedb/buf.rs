use std::fs::File;
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};

/// Chunk size MUST be a power of 2.
const CHUNK_SIZE: u32 = 1024;
const DEFAULT_NUM_CHUNKS: u16 = 16;

/// Chunk buffer for reading or writing.
#[derive(Debug)]
struct Chunk {
    /// chunk data. it is a buffer for reading or writing.
    pub data: Vec<u8>,
    /// chunk offset. it is a offset from start of the file.
    offset: u64,
    /// uses counter. counts up if we read or write chunk.
    uses: u64,
    /// dirty flag. we should write the chunk to the file.
    dirty: bool,
}

impl Chunk {
    fn new(offset: u64, end_pos: u64, chunk_size: usize, file: &mut File) -> Result<Chunk> {
        file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; chunk_size];
        if offset != end_pos {
            let end_off = (end_pos - offset) as usize;
            let buf = if end_off >= chunk_size {
                &mut data[0..]
            } else {
                &mut data[0..end_off]
            };
            file.read_exact(buf)?;
        }
        Ok(Chunk {
            data,
            offset,
            uses: 0,
            dirty: false,
        })
    }
    //
    fn write(&mut self, end_pos: u64, file: &mut File) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let end_off = (end_pos - self.offset) as usize;
        let buf = if end_off >= self.data.len() {
            &self.data[0..]
        } else {
            &self.data[0..end_off]
        };
        file.write_all(buf)?;
        self.dirty = false;
        Ok(())
    }
}

/// Implements key-value sorted vec.
/// the key is the offset from start the file.
/// the value is the index of BufFile::data.
#[derive(Debug)]
struct OffIdx {
    vec: Vec<(u64, usize)>,
}
impl OffIdx {
    fn with_capacity(cap: usize) -> Self {
        Self {
            vec: Vec::with_capacity(cap),
        }
    }
    fn get(&mut self, offset: u64) -> Option<usize> {
        if let Ok(x) = self.vec.binary_search_by(|a| a.0.cmp(&offset)) {
            Some(self.vec[x].1)
        } else {
            None
        }
    }
    fn insert(&mut self, offset: u64, idx: usize) {
        match self.vec.binary_search_by(|a| a.0.cmp(&offset)) {
            Ok(x) => {
                self.vec[x].1 = idx;
            }
            Err(x) => {
                self.vec.insert(x, (offset, idx));
            }
        }
    }
    fn remove(&mut self, offset: &u64) -> Option<usize> {
        match self.vec.binary_search_by(|a| a.0.cmp(offset)) {
            Ok(x) => Some(self.vec.remove(x).1),
            Err(_x) => None,
        }
    }
    fn clear(&mut self) {
        self.vec.clear();
    }
}

/// Buffer for a random access file.
#[derive(Debug)]
pub struct BufFile {
    /// The maximum number of chunk
    max_num_chunks: usize,
    /// Chunk buffer size in bytes.
    chunk_size: usize,
    /// Chunk offset mask.
    chunk_mask: u64,
    /// Contains the actual chunks
    chunks: Vec<Chunk>,
    /// Used to quickly map a file index to an array index (to index self.dat)
    map: OffIdx,
    /// The file to be written to and read from
    file: File,
    /// The current position of the file.
    pos: u64,
    /// The file offset that is the end of the file.
    end: u64,
    //
    fetch_cache: Option<(u64, usize)>,
}

// ref.) http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
fn roundup_powerof2(mut v: u32) -> u32 {
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v += 1;
    v
}

impl BufFile {
    /// Creates a new BufFile.
    pub fn new(file: File) -> Result<BufFile> {
        Self::with_capacity(DEFAULT_NUM_CHUNKS, CHUNK_SIZE, file)
    }
    /// Creates a new BufFile with the specified number of chunks.
    /// chunk_size is MUST power of 2.
    pub fn with_capacity(max_num_chunks: u16, chunk_size: u32, mut file: File) -> Result<BufFile> {
        debug_assert!(chunk_size == roundup_powerof2(chunk_size));
        let max_num_chunks = max_num_chunks as usize;
        let chunk_mask = !(chunk_size as u64 - 1);
        let chunk_size = chunk_size as usize;
        let end = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;
        //
        Ok(BufFile {
            max_num_chunks,
            chunk_size,
            chunk_mask,
            chunks: Vec::with_capacity(max_num_chunks),
            map: OffIdx::with_capacity(max_num_chunks),
            file,
            pos: 0,
            end,
            fetch_cache: None,
        })
    }
    ///
    pub fn sync_all(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_all()
    }
    ///
    pub fn sync_data(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_data()
    }
    ///
    pub fn clear_buf(&mut self) -> Result<()> {
        self.flush()?;
        self.fetch_cache = None;
        self.chunks.clear();
        self.map.clear();
        Ok(())
    }
}

impl BufFile {
    //
    fn fetch_chunk(&mut self, offset: u64) -> Result<&mut Chunk> {
        let offset = offset & self.chunk_mask;
        if let Some((off, idx)) = self.fetch_cache {
            if off == offset {
                return Ok(&mut self.chunks[idx]);
            }
        }
        let idx = if let Some(x) = self.map.get(offset) {
            x
        } else {
            self.add_chunk(offset)?
        };
        self.fetch_cache = Some((offset, idx));
        Ok(&mut self.chunks[idx])
    }
    //
    fn add_chunk(&mut self, offset: u64) -> Result<usize> {
        self.fetch_cache = None;
        if self.chunks.len() < self.max_num_chunks {
            let new_idx = self.chunks.len();
            match Chunk::new(offset, self.end, self.chunk_size, &mut self.file) {
                Ok(x) => {
                    self.map.insert(offset, new_idx);
                    self.chunks.push(x);
                    Ok(new_idx)
                }
                Err(e) => Err(e),
            }
        } else {
            // LFU: Least Frequently Used
            // find the minimum uses counter.
            let mut min_idx = 0;
            if self.chunks[min_idx].uses != 0 {
                for i in 1..self.max_num_chunks {
                    if self.chunks[i].uses < self.chunks[min_idx].uses {
                        min_idx = i;
                        if self.chunks[min_idx].uses == 0 {
                            break;
                        }
                    }
                }
            }
            // clear all uses counter
            self.chunks.iter_mut().for_each(|chunk| {
                chunk.uses = 0;
            });
            // Make a new chunk, write the old chunk to disk, replace old chunk
            match Chunk::new(offset, self.end, self.chunk_size, &mut self.file) {
                Ok(x) => {
                    self.chunks[min_idx].write(self.end, &mut self.file)?;
                    self.file.seek(SeekFrom::Start(self.pos))?;
                    self.map.remove(&self.chunks[min_idx].offset);
                    self.map.insert(offset, min_idx);
                    self.chunks[min_idx] = x;
                    Ok(min_idx)
                }
                Err(err) => Err(err),
            }
        }
    }
}

impl Read for BufFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let curr = self.pos;
        let len = {
            let chunk = self.fetch_chunk(curr)?;
            chunk.uses += 1;
            let mut data_slice = &chunk.data[(curr - chunk.offset) as usize..];
            data_slice.read(buf)?
        };
        self.pos += len as u64;
        Ok(len)
    }
}

impl Write for BufFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let curr = self.pos;
        let len = {
            let chunk = self.fetch_chunk(curr)?;
            chunk.uses += 1;
            chunk.dirty = true;
            let mut data_slice = &mut chunk.data[(curr - chunk.offset) as usize..];
            data_slice.write(buf)?
        };
        self.pos += len as u64;
        if self.end < self.pos {
            self.end = self.pos;
        }
        Ok(len)
    }
    fn flush(&mut self) -> Result<()> {
        for chunk in self.chunks.iter_mut() {
            chunk.write(self.end, &mut self.file)?;
        }
        Ok(())
    }
}

impl Seek for BufFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(x) => x,
            SeekFrom::End(x) => {
                if x < 0 {
                    self.end - (-x) as u64
                } else {
                    // weren't automatically extended beyond the end.
                    self.end - x as u64
                }
            }
            SeekFrom::Current(x) => {
                if x < 0 {
                    self.pos - (-x) as u64
                } else {
                    self.pos + x as u64
                }
            }
        };
        if new_pos <= self.end {
            let _ = self.fetch_chunk(new_pos)?;
            self.pos = new_pos;
            Ok(new_pos)
        } else {
            Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "You tried to seek over the end of the file: {} < {}",
                    self.end, new_pos
                ),
            ))
        }
    }
}

impl Drop for BufFile {
    /// Write all of the chunks to disk before closing the file.
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

//--
mod debug {
    #[test]
    fn test_size_of() {
        use super::{BufFile, Chunk};
        //
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<BufFile>(), 120);
            assert_eq!(std::mem::size_of::<Chunk>(), 48);
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 16);
            assert_eq!(std::mem::size_of::<Vec<Chunk>>(), 24);
            assert_eq!(std::mem::size_of::<Vec<u8>>(), 24);
        }
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<BufFile>(), 76);
            assert_eq!(std::mem::size_of::<Chunk>(), 32);
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 12);
            assert_eq!(std::mem::size_of::<Vec<Chunk>>(), 12);
            assert_eq!(std::mem::size_of::<Vec<u8>>(), 12);
        }
    }
}
