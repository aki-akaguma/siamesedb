use super::offidx::OffsetIndex;
use super::semtype::*;
use super::tr::IdxNode;
use super::vfile::VarFile;
use std::io::Result;

//const CACHE_SIZE: usize = 64;
const CACHE_SIZE: usize = 128;
//const CACHE_SIZE: usize = 256;
//const CACHE_SIZE: usize = 1024;
//const CACHE_SIZE: usize = 10*1024*1024;

#[derive(Debug)]
struct NodeCacheBean {
    node: Option<IdxNode>,
    node_offset: NodeOffset,
    node_size: NodeSize,
    dirty: bool,
    #[cfg(any(feture = "nc_lru", feature = "nc_lfu"))]
    uses: u32,
}

impl NodeCacheBean {
    fn new(node: IdxNode, node_size: NodeSize, dirty: bool) -> Self {
        let node_offset = node.get_ref().offset();
        Self {
            node: Some(node),
            node_offset,
            node_size,
            dirty,
            #[cfg(any(feture = "nc_lru", feature = "nc_lfu"))]
            uses: 0,
        }
    }
}

#[derive(Debug)]
pub struct NodeCache {
    vec: Vec<NodeCacheBean>,
    map: OffsetIndex,
    cache_size: usize,
    #[cfg(feature = "nc_print_hits")]
    count_of_hits: u64,
    #[cfg(feature = "nc_print_hits")]
    count_of_miss: u64,
    #[cfg(feature = "nc_lru")]
    uses_cnt: u32,
}

impl NodeCache {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            vec: Vec::with_capacity(cache_size),
            map: OffsetIndex::with_capacity(cache_size),
            cache_size,
            #[cfg(feature = "nc_print_hits")]
            count_of_hits: 0,
            #[cfg(feature = "nc_print_hits")]
            count_of_miss: 0,
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
    fn touch(&mut self, _cache_idx: usize) {
        #[cfg(any(feture = "nc_lru", feature = "nc_lfu"))]
        {
            #[cfg(feature = "nc_lfu")]
            {
                self.cache[_cache_idx].uses += 1;
            }
            #[cfg(feature = "nc_lru")]
            {
                self.uses_cnt += 1;
                self.cache[_cache_idx].uses = self.uses_cnt;
            }
        }
    }
    #[inline]
    pub fn flush(&mut self, file: &mut VarFile) -> Result<()> {
        for &(_, idx) in self.map.vec.iter() {
            let ncb = self.vec.get_mut(idx).unwrap();
            write_node(file, ncb)?;
        }
        Ok(())
    }
    #[inline]
    pub fn clear(&mut self, file: &mut VarFile) -> Result<()> {
        self.flush(file)?;
        self.vec.clear();
        self.map.clear();
        Ok(())
    }
    #[inline]
    pub fn _is_empty(&self) -> bool {
        self._len() == 0
    }
    #[inline]
    pub fn _len(&self) -> usize {
        self.vec.len()
    }
    #[inline]
    pub fn get(&mut self, offset: &NodeOffset) -> Option<IdxNode> {
        match self.map.get(&offset.as_value()) {
            Some(idx) => {
                #[cfg(feature = "nc_print_hits")]
                {
                    self.count_of_hits += 1;
                }
                self.touch(idx);
                let ncb = unsafe { self.vec.get_unchecked_mut(idx) };
                debug_assert!(
                    ncb.node_offset == *offset,
                    "ncb.node_offset: {} == *offset: {}",
                    ncb.node_offset.as_value(),
                    offset.as_value()
                );
                if ncb.node.is_some() {
                    debug_assert!(ncb.node.as_ref().unwrap().get_ref().offset() == *offset);
                    Some(ncb.node.as_ref().unwrap().clone())
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
    #[inline]
    pub fn get_node_size(&mut self, offset: &NodeOffset) -> Option<NodeSize> {
        match self.map.get(&offset.as_value()) {
            Some(idx) => {
                #[cfg(feature = "nc_print_hits")]
                {
                    self.count_of_hits += 1;
                }
                self.touch(idx);
                let ncb = self.vec.get_mut(idx).unwrap();
                Some(ncb.node_size)
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
    pub fn put(
        &mut self,
        file: &mut VarFile,
        node: IdxNode,
        node_size: NodeSize,
        dirty: bool,
    ) -> Result<IdxNode> {
        let node_offset = node.get_ref().offset();
        match self.map.get(&node_offset.as_value()) {
            Some(idx) => {
                self.touch(idx);
                let ncb = unsafe { self.vec.get_unchecked_mut(idx) };
                debug_assert!(ncb.node_offset == node_offset);
                if ncb.node.is_some() {
                    debug_assert!(ncb.node.as_ref().unwrap().get_ref().offset() == node_offset);
                }
                ncb.node = Some(node);
                ncb.node_size = node_size;
                if dirty {
                    ncb.dirty = true;
                }
                Ok(ncb.node.as_ref().unwrap().clone())
            }
            None => {
                if self.vec.len() > self.cache_size {
                    // all clear cache algorithm
                    self.clear(file)?;
                    /*
                     */
                    //self.detach_cache(k, file)?
                }
                let k = self.vec.len();
                self.vec.push(NodeCacheBean::new(node, node_size, dirty));
                self.map.insert(&node_offset.as_value(), k);
                self.touch(k);
                let ncb = unsafe { self.vec.get_unchecked_mut(k) };
                debug_assert!(ncb.node_offset == node_offset);
                debug_assert!(ncb.node.as_ref().unwrap().get_ref().offset() == node_offset);
                Ok(ncb.node.as_ref().unwrap().clone())
            }
        }
    }
    fn _detach_cache(&mut self, _k: usize, file: &mut VarFile) -> Result<usize> {
        eprintln!("detach_cache!!");
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
        match self.map.remove(&node_offset.as_value()) {
            Some(idx) => {
                let ncb = self.vec.get_mut(idx).unwrap();
                ncb.node = None;
                Some(ncb.node_size)
            }
            None => None,
        }
    }
}

#[inline]
fn write_node(file: &mut VarFile, ncb: &mut NodeCacheBean) -> Result<()> {
    if ncb.dirty {
        //file.write_node_clear(ncb.node_offset, ncb.node_size)?;
        if ncb.node.is_some() {
            debug_assert!(ncb.node_offset == ncb.node.as_ref().unwrap().get_ref().offset());
            debug_assert!(ncb.node_size == ncb.node.as_ref().unwrap().get_ref().size());
            ncb.node.as_mut().unwrap().idx_write_node_one(file)?;
        }
        ncb.dirty = false;
    }
    Ok(())
}

#[cfg(feature = "nc_print_hits")]
impl Drop for NodeCache {
    fn drop(&mut self) {
        eprintln!(
            "node cache hits: {}%",
            self.count_of_hits * 100 / (self.count_of_hits + self.count_of_miss)
        );
    }
}

//--
#[cfg(test)]
mod debug {
    use super::NodeCacheBean;
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            #[cfg(not(any(feture = "nc_lru", feature = "nc_lfu")))]
            assert_eq!(std::mem::size_of::<NodeCacheBean>(), 24);
            #[cfg(any(feture = "nc_lru", feature = "nc_lfu"))]
            assert_eq!(std::mem::size_of::<NodeCacheBean>(), 32);
        }
        #[cfg(target_pointer_width = "32")]
        {
            #[cfg(not(any(feture = "nc_lru", feature = "nc_lfu")))]
            {
                #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
                assert_eq!(std::mem::size_of::<NodeCacheBean>(), 20);
                #[cfg(any(target_arch = "arm", target_arch = "mips"))]
                assert_eq!(std::mem::size_of::<NodeCacheBean>(), 24);
            }
            #[cfg(any(feture = "nc_lru", feature = "nc_lfu"))]
            assert_eq!(std::mem::size_of::<NodeCacheBean>(), 32);
        }
    }
}
