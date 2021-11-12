use super::super::{DbMapString, DbMapU64};
use super::{FileDbMapString, FileDbMapU64};
use std::collections::BTreeMap;
use std::io::Result;
use std::path::{Path, PathBuf};

pub(crate) mod dbxxx;
pub(crate) mod semtype;

mod dat;
mod idx;
mod kc;
mod vfile;

#[cfg(feature = "node_cache")]
mod nc;

#[derive(Debug)]
pub struct FileDbInner {
    db_maps: BTreeMap<String, FileDbMapString>,
    db_lists: BTreeMap<String, FileDbMapU64>,
    //
    path: PathBuf,
}

impl FileDbInner {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<FileDbInner> {
        if !path.as_ref().is_dir() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(FileDbInner {
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
    pub fn db_map(&self, name: &str) -> Option<FileDbMapString> {
        self.db_maps.get(name).cloned()
    }
    pub fn db_list(&self, name: &str) -> Option<FileDbMapU64> {
        self.db_lists.get(name).cloned()
    }
    pub fn db_map_insert(&mut self, name: &str, child: FileDbMapString) -> Option<FileDbMapString> {
        self.db_maps.insert(name.to_string(), child)
    }
    pub fn db_list_insert(&mut self, name: &str, child: FileDbMapU64) -> Option<FileDbMapU64> {
        self.db_lists.insert(name.to_string(), child)
    }
}
