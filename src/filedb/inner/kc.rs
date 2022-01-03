use super::semtype::*;
use std::collections::HashMap;
use std::rc::Rc;

//const CACHE_SIZE: usize = 32;
//const CACHE_SIZE: usize = 64;
const CACHE_SIZE: usize = 128;

//const CACHE_SIZE: usize = 256;
//const CACHE_SIZE: usize = 384;
//const CACHE_SIZE: usize = 1024;
//const CACHE_SIZE: usize = 10*1024*1024;

#[derive(Debug)]
struct KeyCacheBean<KT> {
    pub key: Option<Rc<KT>>,
    key_record_offset: KeyRecordOffset,
}

impl<KT> KeyCacheBean<KT> {
    fn new(key_record_offset: KeyRecordOffset, key: Rc<KT>) -> Self {
        Self {
            key: Some(key),
            key_record_offset,
        }
    }
}

#[derive(Debug)]
pub struct KeyCache<KT> {
    map: HashMap<KeyRecordOffset, KeyCacheBean<KT>>,
    cache_size: usize,
    #[cfg(feature = "kc_print_hits")]
    count_of_hits: u64,
    #[cfg(feature = "kc_print_hits")]
    count_of_miss: u64,
}

impl<KT> KeyCache<KT> {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            map: HashMap::new(),
            cache_size,
            #[cfg(feature = "kc_print_hits")]
            count_of_hits: 0,
            #[cfg(feature = "kc_print_hits")]
            count_of_miss: 0,
        }
    }
}

impl<KT> Default for KeyCache<KT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<KT> KeyCache<KT> {
    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
    }
}

pub trait KeyCacheTrait<KT> {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize;
    fn get(&mut self, offset: &KeyRecordOffset) -> Option<Rc<KT>>;
    fn put(&mut self, offset: &KeyRecordOffset, key: KT) -> Rc<KT>;
    fn delete(&mut self, offset: &KeyRecordOffset);
}

impl<KT> KeyCacheTrait<KT> for KeyCache<KT> {
    #[inline]
    fn len(&self) -> usize {
        self.map.len()
    }
    #[inline]
    fn get(&mut self, offset: &KeyRecordOffset) -> Option<Rc<KT>> {
        match self.map.get(offset) {
            Some(kcb) => {
                #[cfg(feature = "kc_print_hits")]
                {
                    self.count_of_hits += 1;
                }
                debug_assert!(
                    kcb.key_record_offset == *offset,
                    "kcb.key_record_offset: {} == *offset: {}",
                    kcb.key_record_offset.as_value(),
                    offset.as_value()
                );
                if kcb.key.is_some() {
                    Some(kcb.key.as_ref().unwrap().clone())
                } else {
                    None
                }
            }
            None => {
                #[cfg(feature = "kc_print_hits")]
                {
                    self.count_of_miss += 1;
                }
                None
            }
        }
    }
    fn put(&mut self, offset: &KeyRecordOffset, key: KT) -> Rc<KT> {
        match self.map.get_mut(offset) {
            Some(kcb) => {
                debug_assert!(kcb.key_record_offset == *offset);
                kcb.key = Some(Rc::new(key));
                kcb.key.as_ref().unwrap().clone()
            }
            None => {
                if self.map.len() > self.cache_size {
                    // all clear cache algorithm
                    self.clear();
                }
                let key = Rc::new(key);
                self.map
                    .insert(*offset, KeyCacheBean::new(*offset, key.clone()));
                key
            }
        }
    }
    fn delete(&mut self, offset: &KeyRecordOffset) {
        let _ = self.map.remove(offset);
    }
}

#[cfg(feature = "kc_print_hits")]
impl<KT> Drop for KeyCache<KT> {
    fn drop(&mut self) {
        let total = self.count_of_hits + self.count_of_miss;
        eprintln!(
            "key cache hits: {}/{} [{:.2}%]",
            self.count_of_hits,
            total,
            self.count_of_hits as f64 * 100.0 / total as f64
        );
    }
}

//--
#[cfg(test)]
mod debug {
    use super::KeyCacheBean;
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<KeyCacheBean<String>>(), 16);
            assert_eq!(std::mem::size_of::<KeyCacheBean<u64>>(), 16);
        }
        #[cfg(target_pointer_width = "32")]
        {
            #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
            assert_eq!(std::mem::size_of::<KeyCacheBean<String>>(), 12);
            #[cfg(any(target_arch = "arm", target_arch = "mips"))]
            assert_eq!(std::mem::size_of::<KeyCacheBean<String>>(), 16);
            //
            #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
            assert_eq!(std::mem::size_of::<KeyCacheBean<u64>>(), 12);
            #[cfg(any(target_arch = "arm", target_arch = "mips"))]
            assert_eq!(std::mem::size_of::<KeyCacheBean<u64>>(), 16);
        }
    }
}
