#[cfg(feature = "offidx_btreemap")]
use std::collections::BTreeMap;

/// Implements key-value sorted vec.
/// the key is the offset from start the file.
/// the value is the index of vec<>.
#[derive(Debug)]
pub(crate) struct OffsetIndex {
    #[cfg(not(feature = "offidx_btreemap"))]
    pub(crate) vec: Vec<(u64, usize)>,
    #[cfg(feature = "offidx_btreemap")]
    btm: BTreeMap<u64, usize>,
}
impl OffsetIndex {
    pub(crate) fn with_capacity(_cap: usize) -> Self {
        Self {
            #[cfg(not(feature = "offidx_btreemap"))]
            vec: Vec::with_capacity(_cap),
            #[cfg(feature = "offidx_btreemap")]
            btm: BTreeMap::new(),
        }
    }
    pub(crate) fn get(&mut self, offset: &u64) -> Option<usize> {
        #[cfg(not(feature = "offidx_btreemap"))]
        {
            if let Some((x_offset, _)) = self.vec.first() {
                if offset < x_offset {
                    return None;
                }
            }
            if let Some((x_offset, _)) = self.vec.last() {
                if offset > x_offset {
                    return None;
                }
            }
            if let Ok(x) = self.vec.binary_search_by(|a| a.0.cmp(offset)) {
                Some(self.vec[x].1)
            } else {
                None
            }
        }
        #[cfg(feature = "offidx_btreemap")]
        if let Some(x) = self.btm.get(offset) {
            Some(*x)
        } else {
            None
        }
    }
    pub(crate) fn insert(&mut self, offset: &u64, idx: usize) {
        #[cfg(not(feature = "offidx_btreemap"))]
        match self.vec.binary_search_by(|a| a.0.cmp(offset)) {
            Ok(x) => {
                self.vec[x].1 = idx;
            }
            Err(x) => {
                self.vec.insert(x, (*offset, idx));
            }
        }
        #[cfg(feature = "offidx_btreemap")]
        let _ = self.btm.insert(*offset, idx);
    }
    pub(crate) fn remove(&mut self, offset: &u64) -> Option<usize> {
        #[cfg(not(feature = "offidx_btreemap"))]
        match self.vec.binary_search_by(|a| a.0.cmp(offset)) {
            Ok(x) => Some(self.vec.remove(x).1),
            Err(_x) => None,
        }
        #[cfg(feature = "offidx_btreemap")]
        self.btm.remove(offset)
    }
    pub(crate) fn clear(&mut self) {
        #[cfg(not(feature = "offidx_btreemap"))]
        self.vec.clear();
        #[cfg(feature = "offidx_btreemap")]
        self.btm.clear();
    }
}

//--
#[cfg(test)]
mod debug {
    use super::OffsetIndex;
    //
    #[test]
    fn test_size_of() {
        #[cfg(target_pointer_width = "64")]
        {
            #[cfg(not(feature = "offidx_btreemap"))]
            {
                assert_eq!(std::mem::size_of::<OffsetIndex>(), 24);
                assert_eq!(std::mem::size_of::<(u64, usize)>(), 16);
            }
            #[cfg(feature = "offidx_btreemap")]
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 24);
        }
    }
}
