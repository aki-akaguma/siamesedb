use std::rc::Rc;

const CACHE_SIZE: usize = 128;

#[derive(Debug)]
struct KeyCacheBean {
    pub key_string: Rc<String>,
    key_offset: u64,
    uses: u64,
}

impl KeyCacheBean {
    fn new(key_offset: u64, key_string: Rc<String>) -> Self {
        Self {
            key_offset,
            key_string,
            uses: 0,
        }
    }
}

#[derive(Debug)]
pub struct KeyCache {
    cache: Vec<KeyCacheBean>,
}

impl KeyCache {
    pub fn new() -> Self {
        Self {
            cache: Vec::with_capacity(CACHE_SIZE),
        }
    }
}

impl Default for KeyCache {
    fn default() -> Self {
        Self::new()
    }
}

pub trait KeyCacheTrait {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize;
    fn get(&mut self, offset: &u64) -> Option<Rc<String>>;
    fn put(&mut self, offset: &u64, key: String) -> Option<Rc<String>>;
    fn delete(&mut self, offset: &u64);
    fn clear(&mut self);
}

impl KeyCacheTrait for KeyCache {
    fn len(&self) -> usize {
        self.cache.len()
    }
    fn get(&mut self, offset: &u64) -> Option<Rc<String>> {
        match self.cache.binary_search_by_key(offset, |a| a.key_offset) {
            Ok(k) => {
                let a = self.cache.get_mut(k).unwrap();
                a.uses += 1;
                Some(a.key_string.clone())
            }
            Err(_k) => None,
        }
    }
    fn put(&mut self, offset: &u64, key: String) -> Option<Rc<String>> {
        match self.cache.binary_search_by_key(offset, |a| a.key_offset) {
            Ok(k) => {
                let a = self.cache.get_mut(k).unwrap();
                a.uses += 1;
                a.key_string = Rc::new(key);
                Some(a.key_string.clone())
            }
            Err(k) => {
                let k = if self.cache.len() > CACHE_SIZE {
                    /*
                    // find the minimum uses counter.
                    let mut min_idx = 0;
                    unsafe {
                        if self.cache.get_unchecked(min_idx).uses != 0 {
                            for i in 1..self.cache.len() {
                                let i_uses = self.cache.get_unchecked(i).uses;
                                let min_idx_uses = self.cache.get_unchecked(min_idx).uses;
                                if i_uses < min_idx_uses {
                                    min_idx = i;
                                    if self.cache.get_unchecked(min_idx).uses == 0 {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    // clear all uses counter
                    self.cache.iter_mut().for_each(|bean| {
                        bean.uses = 0;
                    });
                    self.cache.remove(min_idx);
                    if k <= min_idx {
                        k
                    } else {
                        k - 1
                    }
                    */
                    /*
                    self.cache.pop();
                    if k < self.cache.len() {
                        k
                    } else {
                        k - 1
                    }
                    */
                    self.cache.clear();
                    0
                } else {
                    k
                };
                let r = Rc::new(key);
                self.cache.insert(k, KeyCacheBean::new(*offset, r.clone()));
                Some(r)
            }
        }
    }
    fn delete(&mut self, offset: &u64) {
        match self.cache.binary_search_by_key(offset, |a| a.key_offset) {
            Ok(k) => {
                self.cache.remove(k);
            }
            Err(_k) => (),
        }
    }
    fn clear(&mut self) {
        self.cache.clear();
    }
}
