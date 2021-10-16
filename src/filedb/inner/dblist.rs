use std::collections::BTreeMap;
use std::io::Result;

use super::super::super::DbList;
use super::super::{FileDbNode, KeyType};
use super::{dat, idx, unu};

#[derive(Debug)]
pub struct FileDbListInner {
    parent: FileDbNode,
    mem: BTreeMap<u64, (u64, Vec<u8>)>,
    dirty: bool,
    //
    dat_file: dat::DatFile,
    idx_file: idx::IdxFile,
    unu_file: unu::UnuFile,
}

impl FileDbListInner {
    pub fn open(parent: FileDbNode, ks_name: &str) -> Result<FileDbListInner> {
        let path = {
            let rc = parent.0.upgrade().expect("FileDbNode is already disposed");
            let locked = rc.borrow();
            locked.path.clone()
        };
        //
        let dat_file = dat::DatFile::open(&path, ks_name, KeyType::Int)?;
        let idx_file = idx::IdxFile::open(&path, ks_name, KeyType::Int)?;
        let unu_file = unu::UnuFile::open(&path, ks_name, KeyType::Int)?;
        Ok(Self {
            parent,
            dat_file,
            idx_file,
            unu_file,
            mem: BTreeMap::new(),
            dirty: false,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

impl DbList for FileDbListInner {
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let r = self.mem.get(&key).map(|val| val.1.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key, (0, value.to_vec()));
        Ok(())
    }
    fn delete(&mut self, key: u64) -> Result<()> {
        let _ = self.mem.remove(&key);
        Ok(())
    }
    fn sync_all(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data and meta
            self.dat_file.sync_all()?;
            self.idx_file.sync_all()?;
            self.unu_file.sync_all()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        if self.is_dirty() {
            // save all data
            self.dat_file.sync_data()?;
            self.idx_file.sync_data()?;
            self.unu_file.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }
}
