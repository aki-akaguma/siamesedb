use super::{DbList, DbMap};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Result;
use std::rc::{Rc, Weak};

// https://qiita.com/qnighy/items/4bbbb20e71cf4ae527b9

#[derive(Debug, Clone)]
pub struct MemoryDb<'a>(Rc<RefCell<MemoryDbInner<'a>>>);

#[derive(Debug, Clone)]
pub(crate) struct MemoryDbNode<'a>(Weak<RefCell<MemoryDbInner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbMap<'a>(Rc<RefCell<MemoryDbMapInner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbList<'a>(Rc<RefCell<MemoryDbListInner<'a>>>);

impl<'a> MemoryDb<'a> {
    pub fn open() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbInner::open())))
    }
    fn to_node(&self) -> MemoryDbNode<'a> {
        MemoryDbNode(Rc::downgrade(&self.0))
    }
    pub fn db_map(&'a self, name: &str) -> MemoryDbMap<'a> {
        if let Some(m) = self.0.borrow().db_maps.get(name) {
            return m.clone();
        }
        //
        let x = self.to_node();
        x.create_db_map(name);
        //
        match self.0.borrow().db_maps.get(name) {
            Some(m) => m.clone(),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_list(&'a self, name: &str) -> MemoryDbList<'a> {
        if let Some(m) = self.0.borrow().db_lists.get(name) {
            return m.clone();
        }
        //
        let x = self.to_node();
        x.create_db_list(name);
        //
        match self.0.borrow().db_lists.get(name) {
            Some(m) => m.clone(),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
}

impl<'a> MemoryDbNode<'a> {
    pub(crate) fn _parent(&self) -> Option<Self> {
        let rc = self.0.upgrade().expect("MemoryDbNode is already dispose");
        let locked = rc.borrow();
        locked.parent.clone()
    }
    fn create_db_map(&self, name: &str) {
        let child: MemoryDbMap<'a> = MemoryDbMap::new();
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
        let child: MemoryDbList<'a> = MemoryDbList::new();
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
        let _ = locked.db_lists.insert(name.to_string(), child);
    }
    fn sync_all(&self) {}
    fn sync_data(&self) {}
}

impl<'a> MemoryDbMap<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbMapInner::new())))
    }
}

impl<'a> DbMap for MemoryDbMap<'a> {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        self.0.borrow_mut().get(key)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(key, value)
    }
    fn delete(&mut self, key: &str) -> Result<()> {
        self.0.borrow_mut().delete(key)
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

impl<'a> MemoryDbList<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbListInner::new())))
    }
}

impl<'a> DbList for MemoryDbList<'a> {
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        self.0.borrow_mut().get(key)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(key, value)
    }
    fn delete(&mut self, key: u64) -> Result<()> {
        self.0.borrow_mut().delete(key)
    }
    fn sync_all(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_all()
    }
    fn sync_data(&mut self) -> Result<()> {
        self.0.borrow_mut().sync_data()
    }
}

//--

#[derive(Debug)]
pub(crate) struct MemoryDbInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    db_maps: BTreeMap<String, MemoryDbMap<'a>>,
    db_lists: BTreeMap<String, MemoryDbList<'a>>,
}

impl<'a> MemoryDbInner<'a> {
    pub fn open() -> MemoryDbInner<'a> {
        MemoryDbInner {
            parent: None,
            db_maps: BTreeMap::new(),
            db_lists: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryDbMapInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<String, Vec<u8>>,
}

impl<'a> MemoryDbMapInner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbMap for MemoryDbMapInner<'a> {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let r = self.mem.get(key).map(|val| val.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key.to_string(), value.to_vec());
        Ok(())
    }
    fn delete(&mut self, key: &str) -> Result<()> {
        self.mem.remove(key);
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
pub(crate) struct MemoryDbListInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<u64, Vec<u8>>,
}

impl<'a> MemoryDbListInner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbList for MemoryDbListInner<'a> {
    fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let r = self.mem.get(&key).map(|val| val.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key, value.to_vec());
        Ok(())
    }
    fn delete(&mut self, key: u64) -> Result<()> {
        let _ = self.mem.remove(&key);
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

//--
mod debug {
    #[test]
    fn test_size_of() {
        use super::{MemoryDb, MemoryDbList, MemoryDbMap};
        use super::{MemoryDbInner, MemoryDbListInner, MemoryDbMapInner};
        //
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<MemoryDb>(), 8);
            assert_eq!(std::mem::size_of::<MemoryDbMap>(), 8);
            assert_eq!(std::mem::size_of::<MemoryDbList>(), 8);
            //
            assert_eq!(std::mem::size_of::<MemoryDbInner>(), 56);
            assert_eq!(std::mem::size_of::<MemoryDbMapInner>(), 32);
            assert_eq!(std::mem::size_of::<MemoryDbListInner>(), 32);
        }
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<MemoryDb>(), 4);
            assert_eq!(std::mem::size_of::<MemoryDbMap>(), 4);
            assert_eq!(std::mem::size_of::<MemoryDbList>(), 4);
            //
            assert_eq!(std::mem::size_of::<MemoryDbInner>(), 28);
            assert_eq!(std::mem::size_of::<MemoryDbMapInner>(), 16);
            assert_eq!(std::mem::size_of::<MemoryDbListInner>(), 16);
        }
    }
}
