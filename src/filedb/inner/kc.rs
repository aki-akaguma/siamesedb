use super::semtype::*;
use std::rc::Rc;

const CACHE_SIZE: usize = 128;

#[derive(Debug)]
struct KeyCacheBean<KT> {
    pub key_string: Rc<KT>,
    record_offset: RecordOffset,
    #[cfg(any(feature = "kc_lfu", feature = "kc_lru"))]
    uses: u32,
}

impl<KT> KeyCacheBean<KT> {
    fn new(record_offset: RecordOffset, key_string: Rc<KT>) -> Self {
        Self {
            record_offset,
            key_string,
            #[cfg(any(feature = "kc_lfu", feature = "kc_lru"))]
            uses: 0,
        }
    }
}

#[derive(Debug)]
pub struct KeyCache<KT> {
    cache: Vec<KeyCacheBean<KT>>,
    cache_size: usize,
    #[cfg(feature = "kc_lru")]
    uses_cnt: u32,
}

impl<KT> KeyCache<KT> {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            cache: Vec::with_capacity(cache_size),
            cache_size,
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
}

pub trait KeyCacheTrait<KT> {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize;
    fn get(&mut self, offset: &RecordOffset) -> Option<Rc<KT>>;
    fn put(&mut self, offset: &RecordOffset, key: KT) -> Rc<KT>;
    fn delete(&mut self, offset: &RecordOffset);
    fn clear(&mut self);
}

impl<KT> KeyCacheTrait<KT> for KeyCache<KT> {
    fn len(&self) -> usize {
        self.cache.len()
    }
    fn get(&mut self, offset: &RecordOffset) -> Option<Rc<KT>> {
        match self.cache.binary_search_by_key(offset, |a| a.record_offset) {
            Ok(k) => {
                self.touch(k);
                let a = self.cache.get_mut(k).unwrap();
                Some(a.key_string.clone())
            }
            Err(_k) => None,
        }
    }
    fn put(&mut self, offset: &RecordOffset, key: KT) -> Rc<KT> {
        match self.cache.binary_search_by_key(offset, |a| a.record_offset) {
            Ok(k) => {
                self.touch(k);
                let a = self.cache.get_mut(k).unwrap();
                a.key_string = Rc::new(key);
                a.key_string.clone()
            }
            Err(k) => {
                let k = if self.cache.len() > self.cache_size {
                    self.detach_cache(k)
                } else {
                    k
                };
                let r = Rc::new(key);
                self.cache.insert(k, KeyCacheBean::new(*offset, r.clone()));
                self.touch(k);
                r
            }
        }
    }
    fn delete(&mut self, offset: &RecordOffset) {
        match self.cache.binary_search_by_key(offset, |a| a.record_offset) {
            Ok(k) => {
                let _kcb = self.cache.remove(k);
            }
            Err(_k) => (),
        }
    }
    fn clear(&mut self) {
        self.cache.clear();
        #[cfg(feature = "kc_lru")]
        {
            self.uses_cnt = 0;
        }
    }
}
