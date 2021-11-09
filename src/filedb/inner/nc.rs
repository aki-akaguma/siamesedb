use super::idx::{idx_write_node_one, IdxNode};
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
}

impl NodeCacheBean {
    fn new(node: Rc<IdxNode>, node_size: NodeSize, dirty: bool) -> Self {
        let node_offset = node.offset;
        Self {
            node,
            node_offset,
            node_size,
            dirty,
        }
    }
}

#[derive(Debug)]
pub struct NodeCache {
    cache: Vec<NodeCacheBean>,
    cache_size: usize,
}

impl NodeCache {
    pub fn new() -> Self {
        Self::with_cache_size(CACHE_SIZE)
    }
    pub fn with_cache_size(cache_size: usize) -> Self {
        Self {
            cache: Vec::with_capacity(cache_size),
            cache_size,
        }
    }
}

impl Default for NodeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeCache {
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
            .binary_search_by_key(&offset.as_value(), |ncb| ncb.node_offset.as_value())
        {
            Ok(k) => {
                let ncb = self.cache.get_mut(k).unwrap();
                Some(ncb.node.clone())
            }
            Err(_k) => None,
        }
    }
    pub fn get_node_size(&mut self, offset: &NodeOffset) -> Option<NodeSize> {
        match self
            .cache
            .binary_search_by_key(&offset.as_value(), |ncb| ncb.node_offset.as_value())
        {
            Ok(k) => {
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
            .binary_search_by_key(&node.offset.as_value(), |ncb| ncb.node_offset.as_value())
        {
            Ok(k) => {
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
                    // all clear cache algorithm
                    self.clear(file)?;
                    0
                } else {
                    k
                };
                let r = Rc::new(node.clone());
                self.cache
                    .insert(k, NodeCacheBean::new(r, node_size, dirty));
                Ok(node)
            }
        }
    }
    pub fn delete(&mut self, node_offset: &NodeOffset) -> Option<NodeSize> {
        match self
            .cache
            .binary_search_by_key(&node_offset.as_value(), |ncb| ncb.node_offset.as_value())
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
        file.write_node_clear(ncb.node_offset, ncb.node_size)?;
        idx_write_node_one(file, &ncb.node)?;
        ncb.dirty = false;
    }
    Ok(())
}
