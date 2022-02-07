use super::offidx::OffsetIndex;
use super::semtype::*;
use super::tr::IdxNode;
use super::vfile::VarFile;
use std::io::Result;

#[cfg(not(feature = "nc_large"))]
#[cfg(not(any(feature = "nc_lru", feature = "nc_lfu")))]
const CACHE_SIZE: usize = 16;

#[cfg(not(feature = "nc_large"))]
#[cfg(any(feature = "nc_lru", feature = "nc_lfu"))]
const CACHE_SIZE: usize = 512;

//const CACHE_SIZE: usize = 8;
//const CACHE_SIZE: usize = 16; // +
//const CACHE_SIZE: usize = 32;
//const CACHE_SIZE: usize = 48;
//const CACHE_SIZE: usize = 64;   // +
//const CACHE_SIZE: usize = 128;
//const CACHE_SIZE: usize = 256;
//const CACHE_SIZE: usize = 512;
//const CACHE_SIZE: usize = 1024;

#[cfg(feature = "nc_large")]
const CACHE_SIZE: usize = 10 * 1024 * 1024;

#[derive(Debug)]
struct NodeCacheBean {
    node: Option<IdxNode>,
    node_offset: NodePieceOffset,
    node_size: NodePieceSize,
    dirty: bool,
    #[cfg(any(feature = "nc_lru", feature = "nc_lfu"))]
    uses: u32,
}

impl NodeCacheBean {
    fn new(node: IdxNode, node_size: NodePieceSize, dirty: bool) -> Self {
        let node_offset = node.get_ref().offset();
        Self {
            node: Some(node),
            node_offset,
            node_size,
            dirty,
            #[cfg(any(feature = "nc_lru", feature = "nc_lfu"))]
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
        #[cfg(feature = "nc_lfu")]
        {
            #[cfg(feature = "siamese_debug")]
            {
                self.vec[_cache_idx].uses += 1;
            }
            #[cfg(not(feature = "siamese_debug"))]
            unsafe {
                let u_ptr = self.vec.as_mut_ptr().add(_cache_idx);
                (*u_ptr).uses += 1;
            }
        }
        #[cfg(feature = "nc_lru")]
        {
            self.uses_cnt += 1;
            self.vec[_cache_idx].uses = self.uses_cnt;
        }
    }
    #[inline]
    pub fn flush(&mut self, file: &mut VarFile) -> Result<()> {
        #[cfg(feature = "oi_hash_turbo")]
        /*
        {
            let mut off_vec: Vec<u64> = self.map.map.keys().map(|&a| a).collect();
            off_vec.sort_unstable();
            for off in off_vec.iter() {
                let idx = self.map.get(off).unwrap();
                let ncb = self.vec.get_mut(idx).unwrap();
                write_node(file, ncb)?;
            }
        }
        */
        {
            for ncb in self.vec.iter_mut() {
                write_node(file, ncb)?;
            }
        }
        #[cfg(not(feature = "oi_hash_turbo"))]
        {
            for &(_, idx) in self.map.vec.iter() {
                let ncb = self.vec.get_mut(idx).unwrap();
                write_node(file, ncb)?;
            }
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
    pub fn get(&mut self, offset: &NodePieceOffset) -> Option<IdxNode> {
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
    pub fn get_node_size(&mut self, offset: &NodePieceOffset) -> Option<NodePieceSize> {
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
        node_size: NodePieceSize,
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
                    #[cfg(not(any(feature = "nc_lfu", feature = "nc_lru")))]
                    {
                        // all clear cache algorithm
                        self.clear(file)?;
                    }
                    #[cfg(any(feature = "nc_lfu", feature = "nc_lru"))]
                    {
                        let k = self.detach_cache()?;
                        //
                        let mut ncb = self.vec.swap_remove(k);
                        write_node(file, &mut ncb)?;
                        self.map.remove(&ncb.node_offset.as_value());
                        //
                        let off = self.vec[k].node_offset;
                        self.map.insert(&off.as_value(), k);
                    }
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
    #[cfg(any(feature = "nc_lfu", feature = "nc_lru"))]
    fn detach_cache(&mut self) -> Result<usize> {
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
        // LFU: Least Frequently Used
        let min_idx = {
            // find the minimum uses counter.
            #[cfg(feature = "siamese_debug")]
            let min_idx = {
                let mut min_idx = 0;
                let mut min_uses = self.vec[min_idx].uses;
                if min_uses != 0 {
                    for i in 1..self.cache_size {
                        if self.vec[i].uses < min_uses {
                            min_idx = i;
                            min_uses = self.vec[min_idx].uses;
                            if min_uses == 0 {
                                break;
                            }
                        }
                    }
                }
                min_idx
            };
            #[cfg(not(feature = "siamese_debug"))]
            let min_idx = unsafe {
                let u_ptr = self.vec.as_mut_ptr();
                let mut min_idx = 0;
                let mut min_uses = (*u_ptr.add(min_idx)).uses;
                if min_uses != 0 {
                    for i in 1..self.cache_size {
                        if (*u_ptr.add(i)).uses < min_uses {
                            min_idx = i;
                            min_uses = (*u_ptr.add(min_idx)).uses;
                            if min_uses == 0 {
                                break;
                            }
                        }
                    }
                }
                min_idx
            };
            // clear all uses counter
            self.vec.iter_mut().for_each(|ncb| {
                ncb.uses = 0;
            });
            #[cfg(feature = "nc_lru")]
            {
                // clear LRU(: Least Reacently Used) counter
                self.uses_cnt = 0;
            }
            min_idx
        };
        //
        Ok(min_idx)
    }
    pub fn delete(&mut self, node_offset: &NodePieceOffset) -> Option<NodePieceSize> {
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
        let total = self.count_of_hits + self.count_of_miss;
        eprintln!(
            "node cache hits: {}/{} [{:.2}%]",
            self.count_of_hits,
            total,
            self.count_of_hits as f64 * 100.0 / total as f64
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
