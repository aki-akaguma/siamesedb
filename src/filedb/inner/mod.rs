use std::collections::BTreeMap;
use std::io::Result;
use std::path::{Path, PathBuf};

use super::super::{DbList, DbMap};
use super::{FileDbList, FileDbMap, FileDbNode};

pub(crate) mod dbxxx;

mod dat;
mod idx;

mod buf;
mod vfile;

#[cfg(feature = "vf_vu64")]
pub mod vu64;

mod kc;

#[derive(Debug)]
pub struct FileDbInner {
    parent: Option<FileDbNode>,
    //
    db_maps: BTreeMap<String, FileDbMap>,
    db_lists: BTreeMap<String, FileDbList>,
    //
    path: PathBuf,
}

impl FileDbInner {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<FileDbInner> {
        if !path.as_ref().is_dir() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(FileDbInner {
            parent: None,
            db_maps: BTreeMap::new(),
            db_lists: BTreeMap::new(),
            path: path.as_ref().to_path_buf(),
        })
    }
    pub fn sync_all(&self) -> Result<()> {
        let keys: Vec<_> = self.db_maps.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_map(&a).unwrap();
            b.sync_all()?;
        }
        let keys: Vec<_> = self.db_lists.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_list(&a).unwrap();
            b.sync_all()?;
        }
        Ok(())
    }
    pub fn sync_data(&self) -> Result<()> {
        let keys: Vec<_> = self.db_maps.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_map(&a).unwrap();
            b.sync_data()?;
        }
        let keys: Vec<_> = self.db_lists.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_list(&a).unwrap();
            b.sync_data()?;
        }
        Ok(())
    }
    /*<CHACHA>
    pub fn record_iter(&mut self) -> Result<RecordIter> {
        RecordIter::new(self.file.clone())
    }
    */
}

impl FileDbInner {
    pub(crate) fn _parent(&self) -> Option<FileDbNode> {
        self.parent.clone()
    }
    pub fn db_map(&self, name: &str) -> Option<FileDbMap> {
        self.db_maps.get(name).cloned()
    }
    pub fn db_list(&self, name: &str) -> Option<FileDbList> {
        self.db_lists.get(name).cloned()
    }
    pub fn db_map_insert(&mut self, name: &str, child: FileDbMap) -> Option<FileDbMap> {
        self.db_maps.insert(name.to_string(), child)
    }
    pub fn db_list_insert(&mut self, name: &str, child: FileDbList) -> Option<FileDbList> {
        self.db_lists.insert(name.to_string(), child)
    }
}
