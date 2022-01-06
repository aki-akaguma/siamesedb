use super::super::DbXxx;
use super::{FileDbMapDbBytes, FileDbMapDbInt, FileDbMapString, FileDbParams};
use std::collections::BTreeMap;
use std::io::Result;
use std::path::{Path, PathBuf};

pub(crate) mod dbxxx;
pub(crate) mod semtype;

mod piece;
mod tr;

mod idx;
mod key;
mod val;
mod vfile;

#[cfg(feature = "htx")]
mod htx;

#[cfg(feature = "node_cache")]
mod nc;

#[cfg(feature = "node_cache")]
mod offidx;

#[derive(Debug)]
pub struct FileDbInner {
    db_maps_bytes: BTreeMap<String, FileDbMapDbBytes>,
    db_maps_dbint: BTreeMap<String, FileDbMapDbInt>,
    db_maps: BTreeMap<String, FileDbMapString>,
    //db_lists: BTreeMap<String, FileDbMapDbInt>,
    //
    path: PathBuf,
}

impl FileDbInner {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<FileDbInner> {
        let path = path.as_ref();
        if !path.is_dir() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(FileDbInner {
            db_maps_bytes: BTreeMap::new(),
            db_maps_dbint: BTreeMap::new(),
            db_maps: BTreeMap::new(),
            //db_lists: BTreeMap::new(),
            path: path.to_path_buf(),
        })
    }
    #[inline]
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
    pub fn sync_all(&self) -> Result<()> {
        let keys: Vec<_> = self.db_maps.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_map(&a).unwrap();
            b.sync_all()?;
        }
        /*
        let keys: Vec<_> = self.db_lists.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_list(&a).unwrap();
            b.sync_all()?;
        }
        */
        Ok(())
    }
    pub fn sync_data(&self) -> Result<()> {
        let keys: Vec<_> = self.db_maps.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_map(&a).unwrap();
            b.sync_data()?;
        }
        /*
        let keys: Vec<_> = self.db_lists.keys().cloned().collect();
        for a in keys {
            let mut b = self.db_list(&a).unwrap();
            b.sync_data()?;
        }
        */
        Ok(())
    }
}

impl FileDbInner {
    #[inline]
    pub fn db_map_bytes(&self, name: &str) -> Option<FileDbMapDbBytes> {
        self.db_maps_bytes.get(name).cloned()
    }
    #[inline]
    pub fn db_map_dbint(&self, name: &str) -> Option<FileDbMapDbInt> {
        self.db_maps_dbint.get(name).cloned()
    }
    #[inline]
    pub fn db_map(&self, name: &str) -> Option<FileDbMapString> {
        self.db_maps.get(name).cloned()
    }
    /*
    #[inline]
    pub fn db_list(&self, name: &str) -> Option<FileDbMapDbInt> {
        self.db_lists.get(name).cloned()
    }
    */
    #[inline]
    pub fn db_map_bytes_insert(
        &mut self,
        name: &str,
        child: FileDbMapDbBytes,
    ) -> Option<FileDbMapDbBytes> {
        self.db_maps_bytes.insert(name.to_string(), child)
    }
    #[inline]
    pub fn db_map_dbint_insert(
        &mut self,
        name: &str,
        child: FileDbMapDbInt,
    ) -> Option<FileDbMapDbInt> {
        self.db_maps_dbint.insert(name.to_string(), child)
    }
    #[inline]
    pub fn db_map_insert(&mut self, name: &str, child: FileDbMapString) -> Option<FileDbMapString> {
        self.db_maps.insert(name.to_string(), child)
    }
    /*
    #[inline]
    pub fn db_list_insert(&mut self, name: &str, child: FileDbMapDbInt) -> Option<FileDbMapDbInt> {
        self.db_lists.insert(name.to_string(), child)
    }
    */
}

impl FileDbInner {
    pub(super) fn create_db_map(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapString = FileDbMapString::open(self.path(), name, params)?;
        let _ = self.db_map_insert(name, child);
        Ok(())
    }
    /*
    pub(super) fn create_db_list(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapDbInt = FileDbMapDbInt::open(self.path(), name, params)?;
        let _ = self.db_list_insert(name, child);
        Ok(())
    }
    */
    pub(super) fn create_db_map_bytes(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapDbBytes = FileDbMapDbBytes::open(self.path(), name, params)?;
        let _ = self.db_map_bytes_insert(name, child);
        Ok(())
    }
    pub(super) fn create_db_map_dbint(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapDbInt = FileDbMapDbInt::open(self.path(), name, params)?;
        let _ = self.db_map_dbint_insert(name, child);
        Ok(())
    }
}
