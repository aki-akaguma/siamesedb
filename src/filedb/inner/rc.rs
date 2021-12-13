use super::dat::Record;
use super::dbxxx::FileDbXxxInnerKT;
use super::semtype::*;
use super::vfile::VarFile;
use std::io::Result;
use std::rc::Rc;

const CACHE_SIZE: usize = 64;

#[derive(Debug)]
struct RecordCacheBean<KT: FileDbXxxInnerKT> {
    record: Rc<Record<KT>>,
    record_offset: RecordOffset,
    record_size: RecordSize,
    dirty: bool,
}

impl<KT: FileDbXxxInnerKT> RecordCacheBean<KT> {
    fn new(record: Rc<Record<KT>>, record_size: RecordSize, dirty: bool) -> Self {
        let record_offset = record.offset;
        Self {
            record,
            record_offset,
            record_size,
            dirty,
        }
    }
}

#[derive(Debug)]
pub struct RecordCache<KT: FileDbXxxInnerKT> {
    cache: Vec<RecordCacheBean<KT>>,
    cache_size: usize,
}

impl<KT: FileDbXxxInnerKT> RecordCache<KT> {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            cache: Vec::with_capacity(cache_size),
            cache_size,
        }
    }
}

impl<KT: FileDbXxxInnerKT> Default for RecordCache<KT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<KT: FileDbXxxInnerKT> RecordCache<KT> {
    pub fn flush(&mut self, file: &mut VarFile) -> Result<()> {
        for rcb in &mut self.cache {
            write_record(file, rcb)?;
        }
        Ok(())
    }
    #[inline]
    pub fn clear(&mut self, file: &mut VarFile) -> Result<()> {
        self.flush(file)?;
        self.cache.clear();
        Ok(())
    }
    #[inline]
    pub fn _is_empty(&self) -> bool {
        self._len() == 0
    }
    #[inline]
    pub fn _len(&self) -> usize {
        self.cache.len()
    }
    #[inline]
    pub fn get(&mut self, record_offset: &RecordOffset) -> Option<Rc<Record<KT>>> {
        match self
            .cache
            .binary_search_by_key(record_offset, |rcb| rcb.record_offset)
        {
            Ok(k) => {
                let rcb = self.cache.get_mut(k).unwrap();
                Some(rcb.record.clone())
            }
            Err(_k) => None,
        }
    }
    #[inline]
    pub fn get_record_size(&mut self, record_offset: &RecordOffset) -> Option<RecordSize> {
        match self
            .cache
            .binary_search_by_key(record_offset, |rcb| rcb.record_offset)
        {
            Ok(k) => {
                let rcb = self.cache.get_mut(k).unwrap();
                Some(rcb.record_size)
            }
            Err(_k) => None,
        }
    }
    pub fn put(
        &mut self,
        file: &mut VarFile,
        record: Record<KT>,
        record_size: RecordSize,
        dirty: bool,
    ) -> Result<Record<KT>> {
        debug_assert!(record_size.is_valid());
        match self
            .cache
            .binary_search_by_key(&record.offset, |rcb| rcb.record_offset)
        {
            Ok(k) => {
                let rcb = self.cache.get_mut(k).unwrap();
                rcb.record = Rc::new(record);
                rcb.record_size = record_size;
                if dirty {
                    rcb.dirty = true;
                }
                Ok(rcb.record.as_ref().clone())
            }
            Err(k) => {
                let k = if self.cache.len() > self.cache_size {
                    // all clear cache algorithm
                    self.clear(file)?;
                    0
                } else {
                    k
                };
                let r = Rc::new(record);
                self.cache
                    .insert(k, RecordCacheBean::new(r, record_size, dirty));
                let rcb = self.cache.get_mut(k).unwrap();
                Ok(rcb.record.as_ref().clone())
            }
        }
    }
    pub fn delete(&mut self, record_offset: &RecordOffset) -> Option<RecordSize> {
        match self
            .cache
            .binary_search_by_key(record_offset, |rcb| rcb.record_offset)
        {
            Ok(k) => {
                let rcb = self.cache.remove(k);
                Some(rcb.record_size)
            }
            Err(_k) => None,
        }
    }
}

#[inline]
fn write_record<KT: FileDbXxxInnerKT>(
    file: &mut VarFile,
    rcb: &mut RecordCacheBean<KT>,
) -> Result<()> {
    if rcb.dirty {
        rcb.record.dat_write_record_one(file)?;
        rcb.dirty = false;
    }
    Ok(())
}
