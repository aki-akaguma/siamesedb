use super::idx::IdxNode;
use super::semtype::*;
use super::vfile::VarFile;
use std::io::Result;
use std::rc::Rc;

const CACHE_SIZE: usize = 64;

#[derive(Debug)]
struct NodeCacheBean {
    node: Rc<IdxNode>,
    node_offset: NodeOffset,
    node_size: NodeSize,
    dirty: bool,
    uses: u32,
}

impl NodeCacheBean {
    fn new(node: Rc<IdxNode>, node_size: NodeSize, dirty: bool) -> Self {
        let node_offset = node.offset;
        Self {
            node,
            node_offset,
            node_size,
            dirty,
            uses: 0,
        }
    }
}

#[derive(Debug)]
pub struct NodeCache {
    cache: Vec<NodeCacheBean>,
    cache_size: usize,
    #[cfg(feature = "nc_lru")]
    uses_cnt: u32,
}

impl NodeCache {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            cache: Vec::with_capacity(cache_size),
            cache_size,
            #[cfg(feature = "nc_lru")]
            uses_cnt: 0,
        }
    }
}

impl Default for NodeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeCache {
    #[inline]
    fn touch(&mut self, cache_idx: usize) {
        #[cfg(not(feature = "nc_lru"))]
        {
            self.cache[cache_idx].uses += 1;
        }
        #[cfg(feature = "nc_lru")]
        {
            self.uses_cnt += 1;
            self.cache[cache_idx].uses = self.uses_cnt;
        }
    }
    pub fn flush(&mut self, file: &mut VarFile) -> Result<()> {
        for ncb in &mut self.cache {
            write_node(file, ncb)?;
        }
        Ok(())
    }
    pub fn clear(&mut self, file: &mut VarFile) -> Result<()> {
        self.flush(file)?;
        self.cache.clear();
        Ok(())
    }
    pub fn _is_empty(&self) -> bool {
        self._len() == 0
    }
    pub fn _len(&self) -> usize {
        self.cache.len()
    }
    pub fn get(&mut self, offset: &NodeOffset) -> Option<Rc<IdxNode>> {
        match self
            .cache
            .binary_search_by_key(offset, |ncb| ncb.node_offset)
        {
            Ok(k) => {
                self.touch(k);
                let ncb = self.cache.get_mut(k).unwrap();
                Some(ncb.node.clone())
            }
            Err(_k) => None,
        }
    }
    pub fn get_node_size(&mut self, offset: &NodeOffset) -> Option<NodeSize> {
        match self
            .cache
            .binary_search_by_key(offset, |ncb| ncb.node_offset)
        {
            Ok(k) => {
                self.touch(k);
                let ncb = self.cache.get_mut(k).unwrap();
                Some(ncb.node_size)
            }
            Err(_k) => None,
        }
    }
    pub fn put(
        &mut self,
        file: &mut VarFile,
        node: IdxNode,
        node_size: NodeSize,
        dirty: bool,
    ) -> Result<IdxNode> {
        match self
            .cache
            .binary_search_by_key(&node.offset, |ncb| ncb.node_offset)
        {
            Ok(k) => {
                self.touch(k);
                let ncb = self.cache.get_mut(k).unwrap();
                ncb.node = Rc::new(node);
                ncb.node_size = node_size;
                if dirty {
                    ncb.dirty = true;
                }
                Ok(ncb.node.as_ref().clone())
            }
            Err(k) => {
                let k = if self.cache.len() > self.cache_size {
                    /*
                    // all clear cache algorithm
                    self.clear(file)?;
                    0
                    */
                    self.detach_cache(k, file)?
                } else {
                    k
                };
                let r = Rc::new(node);
                self.cache
                    .insert(k, NodeCacheBean::new(r, node_size, dirty));
                self.touch(k);
                let ncb = self.cache.get_mut(k).unwrap();
                Ok(ncb.node.as_ref().clone())
            }
        }
    }
    fn detach_cache(&mut self, _k: usize, file: &mut VarFile) -> Result<usize> {
        // all clear cache algorithm
        self.clear(file)?;
        Ok(0)
        /*
         */
        /*
        let half = self.cache.len() / 2;
        if _k < half {
            while self.cache.len() > half {
                let mut ncb = self.cache.remove(half);
                write_node(file, &mut ncb)?;
            }
            Ok(_k)
        } else {
            let mut k = _k;
            while self.cache.len() > half +1 {
                let mut ncb = self.cache.remove(0);
                write_node(file, &mut ncb)?;
                k -= 1;
            }
            Ok(k)
        }
        */
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
            #[cfg(feature = "nc_lru")]
            {
                // clear LRU(: Least Reacently Used) counter
                self.uses_cnt = 0;
            }
            min_idx
        };
        // Make a new chunk, write the old cache to disk, replace old cache
        let mut ncb = self.cache.remove(min_idx);
        write_node(file, &mut ncb)?;
        if _k <= min_idx {
            Ok(_k)
        } else {
            Ok(_k - 1)
        }
        */
    }
    pub fn delete(&mut self, node_offset: &NodeOffset) -> Option<NodeSize> {
        match self
            .cache
            .binary_search_by_key(node_offset, |ncb| ncb.node_offset)
        {
            Ok(k) => {
                let ncb = self.cache.remove(k);
                Some(ncb.node_size)
            }
            Err(_k) => None,
        }
    }
}

fn write_node(file: &mut VarFile, ncb: &mut NodeCacheBean) -> Result<()> {
    if ncb.dirty {
        //file.write_node_clear(ncb.node_offset, ncb.node_size)?;
        debug_assert!(ncb.node_offset == ncb.node.offset);
        debug_assert!(ncb.node_size == ncb.node.size);
        ncb.node.idx_write_node_one(file)?;
        ncb.dirty = false;
    }
    Ok(())
}
