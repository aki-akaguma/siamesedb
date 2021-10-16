use std::collections::BTreeMap;
use std::io::Result;
use std::path::{Path, PathBuf};

use super::{FileDbList, FileDbMap, FileDbNode};

pub(crate) mod dblist;
pub(crate) mod dbmap;

mod dat;
mod idx;
mod unu;

mod buf;
mod vfile;

#[cfg(feature = "vf_vu64")]
mod vu64;

#[cfg(feature = "key_cache")]
mod kc;

#[derive(Debug)]
pub struct FileDbInner {
    parent: Option<FileDbNode>,
    //
    db_maps: BTreeMap<String, FileDbMap>,
    db_lists: BTreeMap<String, FileDbList>,
    //
    path: PathBuf,
    dirty: bool,
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
            dirty: false,
        })
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    pub fn sync_all(&self) -> Result<()> {
        if self.is_dirty() {
            // save all data
        }
        Ok(())
    }
    pub fn sync_data(&self) -> Result<()> {
        if self.is_dirty() {
            // save all data
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
    pub fn parent(&self) -> Option<FileDbNode> {
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
