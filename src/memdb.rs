use super::{DbBytes, DbInt, DbMapDbBytes, DbMapDbInt, DbMapString, DbString, DbXxx};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Result;
use std::rc::{Rc, Weak};

#[derive(Debug, Clone)]
pub struct MemoryDb<'a>(Rc<RefCell<MemoryDbInner<'a>>>);

#[derive(Debug, Clone)]
pub(crate) struct MemoryDbNode<'a>(Weak<RefCell<MemoryDbInner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbMapString<'a>(Rc<RefCell<MemoryDbMapStringInner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbMapDbInt<'a>(Rc<RefCell<MemoryDbMapDbIntInner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbMapDbBytes<'a>(Rc<RefCell<MemoryDbMapDbBytesInner<'a>>>);

impl<'a> MemoryDb<'a> {
    pub fn open() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbInner::open())))
    }
    fn to_node(&self) -> MemoryDbNode<'a> {
        MemoryDbNode(Rc::downgrade(&self.0))
    }
    pub fn db_map_string(&'a self, name: &str) -> MemoryDbMapString<'a> {
        if let Some(m) = RefCell::borrow(&self.0).db_maps.get(name) {
            return m.clone();
        }
        //
        let x = self.to_node();
        x.create_db_map(name);
        //
        match RefCell::borrow_mut(&self.0).db_maps.get(name) {
            Some(m) => m.clone(),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_map_dbint(&'a self, name: &str) -> MemoryDbMapDbInt<'a> {
        if let Some(m) = RefCell::borrow(&self.0).db_maps_dbint.get(name) {
            return m.clone();
        }
        //
        let x = self.to_node();
        x.create_db_list(name);
        //
        match RefCell::borrow(&self.0).db_maps_dbint.get(name) {
            Some(m) => m.clone(),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_map_bytes(&'a self, name: &str) -> MemoryDbMapDbBytes<'a> {
        if let Some(m) = RefCell::borrow(&self.0).db_maps_bytes.get(name) {
            return m.clone();
        }
        //
        let x = self.to_node();
        x.create_db_map_bytes(name);
        //
        match RefCell::borrow_mut(&self.0).db_maps_bytes.get(name) {
            Some(m) => m.clone(),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
}

impl<'a> MemoryDbNode<'a> {
    fn create_db_map(&self, name: &str) {
        let child: MemoryDbMapString<'a> = MemoryDbMapString::new();
        let rc = self.0.upgrade().expect("MemoryDbNode is already disposed");
        {
            let mut child_locked = child.0.borrow_mut();
            assert!(
                child_locked.parent.is_none(),
                "Cannot have multiple parents"
            );
            child_locked.parent = Some(self.clone())
        }
        let mut locked = rc.borrow_mut();
        let _ = locked.db_maps.insert(name.to_string(), child);
    }
    fn create_db_list(&self, name: &str) {
        let child: MemoryDbMapDbInt<'a> = MemoryDbMapDbInt::new();
        let rc = self.0.upgrade().expect("MemoryDbNode is already disposed");
        {
            let mut child_locked = child.0.borrow_mut();
            assert!(
                child_locked.parent.is_none(),
                "Cannot have multiple parents"
            );
            child_locked.parent = Some(self.clone())
        }
        let mut locked = rc.borrow_mut();
        let _ = locked.db_maps_dbint.insert(name.to_string(), child);
    }
    fn create_db_map_bytes(&self, name: &str) {
        let child: MemoryDbMapDbBytes<'a> = MemoryDbMapDbBytes::new();
        let rc = self.0.upgrade().expect("MemoryDbNode is already disposed");
        {
            let mut child_locked = child.0.borrow_mut();
            assert!(
                child_locked.parent.is_none(),
                "Cannot have multiple parents"
            );
            child_locked.parent = Some(self.clone())
        }
        let mut locked = rc.borrow_mut();
        let _ = locked.db_maps_bytes.insert(name.to_string(), child);
    }
    fn read_fill_buffer(&self) {}
    fn flush(&self) {}
    fn sync_all(&self) {}
    fn sync_data(&self) {}
}

impl<'a> MemoryDbMapString<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbMapStringInner::new())))
    }
}

impl<'a> DbXxx<DbString> for MemoryDbMapString<'a> {
    fn get_kt(&mut self, key: &DbString) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).get_kt(key)
    }
    fn put_kt(&mut self, key: &DbString, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put_kt(key, value)
    }
    fn del_kt(&mut self, key: &DbString) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).del_kt(key)
    }
    fn read_fill_buffer(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).read_fill_buffer()
    }
    fn flush(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).flush()
    }
    fn sync_all(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_data()
    }
}

impl<'a> DbMapString for MemoryDbMapString<'a> {}

impl<'a> MemoryDbMapDbInt<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbMapDbIntInner::new())))
    }
}

impl<'a> DbXxx<DbInt> for MemoryDbMapDbInt<'a> {
    fn get_kt(&mut self, key: &DbInt) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).get_kt(key)
    }
    fn put_kt(&mut self, key: &DbInt, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put_kt(key, value)
    }
    fn del_kt(&mut self, key: &DbInt) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).del_kt(key)
    }
    fn read_fill_buffer(&mut self) -> Result<()> {
        self.0.borrow_mut().read_fill_buffer()
    }
    fn flush(&mut self) -> Result<()> {
        self.0.borrow_mut().flush()
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

impl<'a> DbMapDbInt for MemoryDbMapDbInt<'a> {}

impl<'a> MemoryDbMapDbBytes<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbMapDbBytesInner::new())))
    }
}

impl<'a> DbXxx<DbBytes> for MemoryDbMapDbBytes<'a> {
    fn get_kt(&mut self, key: &DbBytes) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).get_kt(key)
    }
    fn put_kt(&mut self, key: &DbBytes, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put_kt(key, value)
    }
    fn del_kt(&mut self, key: &DbBytes) -> Result<Option<Vec<u8>>> {
        RefCell::borrow_mut(&self.0).del_kt(key)
    }
    fn read_fill_buffer(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).read_fill_buffer()
    }
    fn flush(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).flush()
    }
    fn sync_all(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        RefCell::borrow_mut(&self.0).sync_data()
    }
}

impl<'a> DbMapDbBytes for MemoryDbMapDbBytes<'a> {}

//--

#[derive(Debug)]
pub(crate) struct MemoryDbInner<'a> {
    db_maps: BTreeMap<String, MemoryDbMapString<'a>>,
    db_maps_dbint: BTreeMap<String, MemoryDbMapDbInt<'a>>,
    db_maps_bytes: BTreeMap<String, MemoryDbMapDbBytes<'a>>,
}

impl<'a> MemoryDbInner<'a> {
    pub fn open() -> MemoryDbInner<'a> {
        MemoryDbInner {
            db_maps: BTreeMap::new(),
            db_maps_dbint: BTreeMap::new(),
            db_maps_bytes: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryDbMapStringInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<String, Vec<u8>>,
}

impl<'a> MemoryDbMapStringInner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbXxx<DbString> for MemoryDbMapStringInner<'a> {
    fn get_kt(&mut self, key: &DbString) -> Result<Option<Vec<u8>>> {
        let key_s = String::from_utf8_lossy(key).to_string();
        let r = self.mem.get(&key_s).map(|val| val.to_vec());
        Ok(r)
    }
    fn put_kt(&mut self, key: &DbString, value: &[u8]) -> Result<()> {
        let key_s = String::from_utf8_lossy(key).to_string();
        let _ = self.mem.insert(key_s, value.to_vec());
        Ok(())
    }
    fn del_kt(&mut self, key: &DbString) -> Result<Option<Vec<u8>>> {
        let key_s = String::from_utf8_lossy(key).to_string();
        let r = self.mem.remove(&key_s);
        Ok(r)
    }
    fn read_fill_buffer(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.read_fill_buffer()
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.flush()
        }
        Ok(())
    }
    fn sync_all(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.sync_all()
        }
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.sync_data()
        }
        Ok(())
    }
}
impl<'a> DbMapString for MemoryDbMapStringInner<'a> {}

#[derive(Debug)]
pub(crate) struct MemoryDbMapDbIntInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<DbInt, Vec<u8>>,
}

impl<'a> MemoryDbMapDbIntInner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbXxx<DbInt> for MemoryDbMapDbIntInner<'a> {
    fn get_kt(&mut self, key: &DbInt) -> Result<Option<Vec<u8>>> {
        let r = self.mem.get(key).cloned();
        Ok(r)
    }
    fn put_kt(&mut self, key: &DbInt, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key.clone(), value.to_vec());
        Ok(())
    }
    fn del_kt(&mut self, key: &DbInt) -> Result<Option<Vec<u8>>> {
        let r = self.mem.remove(key);
        Ok(r)
    }
    fn read_fill_buffer(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.read_fill_buffer()
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.flush()
        }
        Ok(())
    }
    fn sync_all(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.sync_all()
        }
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.sync_data()
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct MemoryDbMapDbBytesInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<DbBytes, Vec<u8>>,
}

impl<'a> MemoryDbMapDbBytesInner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbXxx<DbBytes> for MemoryDbMapDbBytesInner<'a> {
    fn get_kt(&mut self, key: &DbBytes) -> Result<Option<Vec<u8>>> {
        let r = self.mem.get(&(key.into())).cloned();
        Ok(r)
    }
    fn put_kt(&mut self, key: &DbBytes, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key.into(), value.to_vec());
        Ok(())
    }
    fn del_kt(&mut self, key: &DbBytes) -> Result<Option<Vec<u8>>> {
        let r = self.mem.remove(&(key.into()));
        Ok(r)
    }
    fn read_fill_buffer(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.read_fill_buffer()
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.flush()
        }
        Ok(())
    }
    fn sync_all(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.sync_all()
        }
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        if let Some(p) = self.parent.as_ref() {
            p.sync_data()
        }
        Ok(())
    }
}
impl<'a> DbMapDbBytes for MemoryDbMapDbBytesInner<'a> {}

//--
#[cfg(test)]
mod debug {
    #[test]
    fn test_size_of() {
        use super::{MemoryDb, MemoryDbMapDbInt, MemoryDbMapString};
        use super::{
            MemoryDbInner, MemoryDbMapDbBytesInner, MemoryDbMapDbIntInner, MemoryDbMapStringInner,
        };
        //
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<MemoryDb>(), 8);
            assert_eq!(std::mem::size_of::<MemoryDbMapString>(), 8);
            assert_eq!(std::mem::size_of::<MemoryDbMapDbInt>(), 8);
            //
            assert_eq!(std::mem::size_of::<MemoryDbInner>(), 72);
            assert_eq!(std::mem::size_of::<MemoryDbMapStringInner>(), 32);
            assert_eq!(std::mem::size_of::<MemoryDbMapDbIntInner>(), 32);
            assert_eq!(std::mem::size_of::<MemoryDbMapDbBytesInner>(), 32);
        }
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<MemoryDb>(), 4);
            assert_eq!(std::mem::size_of::<MemoryDbMapString>(), 4);
            assert_eq!(std::mem::size_of::<MemoryDbMapDbInt>(), 4);
            //
            assert_eq!(std::mem::size_of::<MemoryDbInner>(), 36);
            assert_eq!(std::mem::size_of::<MemoryDbMapStringInner>(), 16);
            assert_eq!(std::mem::size_of::<MemoryDbMapDbIntInner>(), 16);
            assert_eq!(std::mem::size_of::<MemoryDbMapDbBytesInner>(), 16);
        }
    }
}
