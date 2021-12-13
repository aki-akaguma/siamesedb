use super::super::DbXxx;
use super::{FileDbMapBytes, FileDbMapString, FileDbMapU64, FileDbParams};
use std::collections::BTreeMap;
use std::io::Result;
use std::path::{Path, PathBuf};

pub(crate) mod dbxxx;
pub(crate) mod semtype;

mod offidx;
mod tr;

mod dat;
mod idx;
mod vfile;

#[cfg(feature = "key_cache")]
mod kc;

#[cfg(feature = "node_cache")]
mod nc;

#[cfg(feature = "record_cache")]
mod rc;

#[derive(Debug)]
pub struct FileDbInner {
    db_maps_bytes: BTreeMap<String, FileDbMapBytes>,
    db_maps: BTreeMap<String, FileDbMapString>,
    db_lists: BTreeMap<String, FileDbMapU64>,
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
            db_maps: BTreeMap::new(),
            db_lists: BTreeMap::new(),
            path: path.to_path_buf(),
        })
    }
    pub fn path(&self) -> &Path {
        self.path.as_path()
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
    #[inline]
    pub fn db_map_bytes(&self, name: &str) -> Option<FileDbMapBytes> {
        self.db_maps_bytes.get(name).cloned()
    }
    #[inline]
    pub fn db_map(&self, name: &str) -> Option<FileDbMapString> {
        self.db_maps.get(name).cloned()
    }
    #[inline]
    pub fn db_list(&self, name: &str) -> Option<FileDbMapU64> {
        self.db_lists.get(name).cloned()
    }
    #[inline]
    pub fn db_map_bytes_insert(
        &mut self,
        name: &str,
        child: FileDbMapBytes,
    ) -> Option<FileDbMapBytes> {
        self.db_maps_bytes.insert(name.to_string(), child)
    }
    #[inline]
    pub fn db_map_insert(&mut self, name: &str, child: FileDbMapString) -> Option<FileDbMapString> {
        self.db_maps.insert(name.to_string(), child)
    }
    #[inline]
    pub fn db_list_insert(&mut self, name: &str, child: FileDbMapU64) -> Option<FileDbMapU64> {
        self.db_lists.insert(name.to_string(), child)
    }
}

impl FileDbInner {
    pub(super) fn create_db_map(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapString = FileDbMapString::open(self.path(), name, params)?;
        let _ = self.db_map_insert(name, child);
        Ok(())
    }
    pub(super) fn create_db_list(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapU64 = FileDbMapU64::open(self.path(), name, params)?;
        let _ = self.db_list_insert(name, child);
        Ok(())
    }
    pub(super) fn create_db_map_bytes(&mut self, name: &str, params: FileDbParams) -> Result<()> {
        let child: FileDbMapBytes = FileDbMapBytes::open(self.path(), name, params)?;
        let _ = self.db_map_bytes_insert(name, child);
        Ok(())
    }
}
