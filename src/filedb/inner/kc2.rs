use super::offidx::OffsetIndex;
use super::semtype::*;
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
    #[cfg(any(feature = "kc_lfu", feature = "kc_lru"))]
    uses: u32,
}

impl<KT> KeyCacheBean<KT> {
    fn new(key_record_offset: KeyRecordOffset, key: Rc<KT>) -> Self {
        Self {
            key: Some(key),
            key_record_offset,
            #[cfg(any(feature = "kc_lfu", feature = "kc_lru"))]
            uses: 0,
        }
    }
}

#[derive(Debug)]
pub struct KeyCache<KT> {
    vec: Vec<KeyCacheBean<KT>>,
    map: OffsetIndex,
    cache_size: usize,
    #[cfg(feature = "kc_print_hits")]
    count_of_hits: u64,
    #[cfg(feature = "kc_print_hits")]
    count_of_miss: u64,
    #[cfg(feature = "kc_lru")]
    uses_cnt: u32,
}

impl<KT> KeyCache<KT> {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            vec: Vec::with_capacity(cache_size),
            map: OffsetIndex::with_capacity(cache_size),
            cache_size,
            #[cfg(feature = "kc_print_hits")]
            count_of_hits: 0,
            #[cfg(feature = "kc_print_hits")]
            count_of_miss: 0,
            #[cfg(feature = "kc_lru")]
            uses_cnt: 0,
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
    fn touch(&mut self, _cache_idx: usize) {
        #[cfg(not(any(feature = "kc_lfu", feature = "kc_lru")))]
        {}
        #[cfg(feature = "kc_lfu")]
        {
            self.cache[_cache_idx].uses += 1;
        }
        #[cfg(feature = "kc_lru")]
        {
            self.uses_cnt += 1;
            self.cache[_cache_idx].uses = self.uses_cnt;
        }
    }
    fn detach_cache(&mut self, _k: usize) -> usize {
        #[cfg(not(any(feature = "kc_lfu", feature = "kc_lru")))]
        {
            // all clear cache algorithm
            self.clear();
            0
        }
        /*
         */
        /*
        let half = self.cache.len() / 2;
        if _k < half {
            let _rest = self.cache.split_off(half);
            _k
        } else {
            let _rest = self.cache.split_off(half);
            self.cache.clear();
            self.cache = _rest;
            _k - half
        }
        */
        #[cfg(any(feature = "kc_lfu", feature = "kc_lru"))]
        {
            // the LFU/LRU half clear
            let mut vec: Vec<(u32, u32)> = self
                .cache
                .iter()
                .enumerate()
                .map(|(idx, a)| (idx as u32, a.uses))
                .collect();
            vec.sort_by(|a, b| match b.1.cmp(&a.1) {
                std::cmp::Ordering::Equal => b.0.cmp(&a.0),
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
            });
            let half = vec.len() / 2;
            let _rest = vec.split_off(half);
            vec.sort_by(|a, b| a.0.cmp(&b.0));
            let mut k = _k as u32;
            while let Some((idx, _uses)) = vec.pop() {
                let _kcb = self.cache.remove(idx as usize);
                if idx < _k as u32 {
                    k -= 1;
                }
            }
            // clear all uses counter
            self.cache.iter_mut().for_each(|kcb| {
                kcb.uses = 0;
            });
            #[cfg(feature = "kc_lru")]
            {
                // clear LRU(: Least Reacently Used) counter
                self.uses_cnt = 0;
            }
            k as usize
        }
        /*
        // LFU: Least Frequently Used
        let min_idx = {
            // find the minimum uses counter.
            let mut min_idx = 0;
            let mut min_uses = self.cache[min_idx].uses;
            if min_uses != 0 {
                for i in 1..self.cache_size {
                    if self.cache[i].uses < min_uses {
                        min_idx = i;
                        min_uses = self.cache[min_idx].uses;
                        if min_uses == 0 {
                            break;
                        }
                    }
                }
            }
            // clear all uses counter
            self.cache.iter_mut().for_each(|ncb| {
                ncb.uses = 0;
            });
            #[cfg(feature = "kc_lru")]
            {
                // clear LRU(: Least Reacently Used) counter
                self.uses_cnt = 0;
            }
            min_idx
        };
        // Make a new chunk, write the old cache to disk, replace old cache
        let _kcb = self.cache.remove(min_idx);
        if _k <= min_idx {
            _k
        } else {
            _k - 1
        }
        */
    }
    #[inline]
    pub fn clear(&mut self) {
        self.vec.clear();
        self.map.clear();
        //
        #[cfg(feature = "kc_lru")]
        {
            self.uses_cnt = 0;
        }
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
        self.vec.len()
    }
    #[inline]
    fn get(&mut self, offset: &KeyRecordOffset) -> Option<Rc<KT>> {
        match self.map.get(&offset.as_value()) {
            Some(idx) => {
                #[cfg(feature = "kc_print_hits")]
                {
                    self.count_of_hits += 1;
                }
                self.touch(idx);
                let kcb = self.vec.get_mut(idx).unwrap();
                //let kcb = unsafe { self.vec.get_unchecked_mut(idx) };
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
                #[cfg(feature = "nc_print_hits")]
                {
                    self.count_of_miss += 1;
                }
                None
            }
        }
    }
    fn put(&mut self, offset: &KeyRecordOffset, key: KT) -> Rc<KT> {
        match self.map.get(&offset.as_value()) {
            Some(idx) => {
                self.touch(idx);
                let kcb = unsafe { self.vec.get_unchecked_mut(idx) };
                debug_assert!(kcb.key_record_offset == *offset);
                kcb.key = Some(Rc::new(key));
                kcb.key.as_ref().unwrap().clone()
            }
            None => {
                if self.vec.len() > self.cache_size {
                    // all clear cache algorithm
                    self.clear();
                    /*
                     */
                    //self.detach_cache(k, file)?
                }
                let k = self.vec.len();
                self.vec.push(KeyCacheBean::new(*offset, Rc::new(key)));
                self.map.insert(&offset.as_value(), k);
                self.touch(k);
                let kcb = unsafe { self.vec.get_unchecked_mut(k) };
                debug_assert!(kcb.key_record_offset == *offset);
                kcb.key.as_ref().unwrap().clone()
            }
        }
    }
    fn delete(&mut self, offset: &KeyRecordOffset) {
        match self.map.remove(&offset.as_value()) {
            Some(idx) => {
                let kcb = self.vec.get_mut(idx).unwrap();
                kcb.key = None;
            }
            None => (),
        }
    }
}

#[cfg(feature = "kc_print_hits")]
impl<KT> Drop for KeyCache<KT> {
    fn drop(&mut self) {
        eprintln!(
            "key cache hits: {}%",
            self.count_of_hits * 100 / (self.count_of_hits + self.count_of_miss)
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
