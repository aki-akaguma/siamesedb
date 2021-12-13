use super::{Bytes, DbMapBytes, DbMapString, DbMapU64, DbXxx};
use std::borrow::Borrow;
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
pub struct MemoryDbMapString<'a>(Rc<RefCell<MemoryDbMapStringInner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbMapU64<'a>(Rc<RefCell<MemoryDbMapU64Inner<'a>>>);

#[derive(Debug, Clone)]
pub struct MemoryDbMapBytes<'a>(Rc<RefCell<MemoryDbMapBytesInner<'a>>>);

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
    pub fn db_map_u64(&'a self, name: &str) -> MemoryDbMapU64<'a> {
        if let Some(m) = RefCell::borrow(&self.0).db_lists.get(name) {
            return m.clone();
        }
        //
        let x = self.to_node();
        x.create_db_list(name);
        //
        match RefCell::borrow(&self.0).db_lists.get(name) {
            Some(m) => m.clone(),
            None => panic!("Cannot create db_maps: {}", name),
        }
    }
    pub fn db_map_bytes(&'a self, name: &str) -> MemoryDbMapBytes<'a> {
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
        let child: MemoryDbMapU64<'a> = MemoryDbMapU64::new();
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
    fn create_db_map_bytes(&self, name: &str) {
        let child: MemoryDbMapBytes<'a> = MemoryDbMapBytes::new();
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

impl<'a> DbXxx<String> for MemoryDbMapString<'a> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).get(&(*key.borrow()))
    }
    fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put(key, value)
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).delete(&(*key.borrow()))
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

impl<'a> MemoryDbMapU64<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbMapU64Inner::new())))
    }
}

impl<'a> DbXxx<u64> for MemoryDbMapU64<'a> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        u64: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.borrow_mut().get(&(*key.borrow()))
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        self.0.borrow_mut().put(key, value)
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        u64: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.borrow_mut().delete(&(*key.borrow()))
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

impl<'a> DbMapU64 for MemoryDbMapU64<'a> {}

impl<'a> MemoryDbMapBytes<'a> {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MemoryDbMapBytesInner::new())))
    }
}

impl<'a> DbXxx<Bytes> for MemoryDbMapBytes<'a> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        Bytes: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).get(&(*key.borrow()))
    }
    fn put(&mut self, key: Bytes, value: &[u8]) -> Result<()> {
        RefCell::borrow_mut(&self.0).put(key, value)
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        Bytes: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        RefCell::borrow_mut(&self.0).delete(&(*key.borrow()))
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

impl<'a> DbMapBytes for MemoryDbMapBytes<'a> {}

//--

#[derive(Debug)]
pub(crate) struct MemoryDbInner<'a> {
    db_maps: BTreeMap<String, MemoryDbMapString<'a>>,
    db_lists: BTreeMap<String, MemoryDbMapU64<'a>>,
    db_maps_bytes: BTreeMap<String, MemoryDbMapBytes<'a>>,
}

impl<'a> MemoryDbInner<'a> {
    pub fn open() -> MemoryDbInner<'a> {
        MemoryDbInner {
            db_maps: BTreeMap::new(),
            db_lists: BTreeMap::new(),
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

impl<'a> DbXxx<String> for MemoryDbMapStringInner<'a> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let r = self.mem.get(&(*key.borrow())).map(|val| val.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key, value.to_vec());
        Ok(())
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        String: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.remove(&(*key.borrow()));
        Ok(())
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
pub(crate) struct MemoryDbMapU64Inner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<u64, Vec<u8>>,
}

impl<'a> MemoryDbMapU64Inner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbXxx<u64> for MemoryDbMapU64Inner<'a> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        u64: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let r = self.mem.get(&(*key.borrow())).map(|val| val.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: u64, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key, value.to_vec());
        Ok(())
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        u64: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let _ = self.mem.remove(&(*key.borrow()));
        Ok(())
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
pub(crate) struct MemoryDbMapBytesInner<'a> {
    parent: Option<MemoryDbNode<'a>>,
    mem: BTreeMap<Bytes, Vec<u8>>,
}

impl<'a> MemoryDbMapBytesInner<'a> {
    fn new() -> Self {
        Self {
            parent: None,
            mem: BTreeMap::new(),
        }
    }
}

impl<'a> DbXxx<Bytes> for MemoryDbMapBytesInner<'a> {
    fn get<Q>(&mut self, key: &Q) -> Result<Option<Vec<u8>>>
    where
        Bytes: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let r = self.mem.get(&(*key.borrow())).map(|val| val.to_vec());
        Ok(r)
    }
    fn put(&mut self, key: Bytes, value: &[u8]) -> Result<()> {
        let _ = self.mem.insert(key, value.to_vec());
        Ok(())
    }
    fn delete<Q>(&mut self, key: &Q) -> Result<()>
    where
        Bytes: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.mem.remove(&(*key.borrow()));
        Ok(())
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
impl<'a> DbMapBytes for MemoryDbMapBytesInner<'a> {}

//--
#[cfg(test)]
mod debug {
    #[test]
    fn test_size_of() {
        use super::{MemoryDb, MemoryDbMapString, MemoryDbMapU64};
        use super::{
            MemoryDbInner, MemoryDbMapBytesInner, MemoryDbMapStringInner, MemoryDbMapU64Inner,
        };
        //
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<MemoryDb>(), 8);
            assert_eq!(std::mem::size_of::<MemoryDbMapString>(), 8);
            assert_eq!(std::mem::size_of::<MemoryDbMapU64>(), 8);
            //
            assert_eq!(std::mem::size_of::<MemoryDbInner>(), 72);
            assert_eq!(std::mem::size_of::<MemoryDbMapStringInner>(), 32);
            assert_eq!(std::mem::size_of::<MemoryDbMapU64Inner>(), 32);
            assert_eq!(std::mem::size_of::<MemoryDbMapBytesInner>(), 32);
        }
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<MemoryDb>(), 4);
            assert_eq!(std::mem::size_of::<MemoryDbMapString>(), 4);
            assert_eq!(std::mem::size_of::<MemoryDbMapU64>(), 4);
            //
            assert_eq!(std::mem::size_of::<MemoryDbInner>(), 36);
            assert_eq!(std::mem::size_of::<MemoryDbMapStringInner>(), 16);
            assert_eq!(std::mem::size_of::<MemoryDbMapU64Inner>(), 16);
            assert_eq!(std::mem::size_of::<MemoryDbMapBytesInner>(), 16);
        }
    }
}
