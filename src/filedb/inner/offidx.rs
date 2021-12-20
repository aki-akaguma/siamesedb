/// Implements key-value sorted vec.
/// the key is the offset from start the file.
/// the value is the index of vec<>.
#[derive(Debug)]
pub(crate) struct OffsetIndex {
    pub(crate) vec: Vec<(u64, usize)>,
}
impl OffsetIndex {
    pub(crate) fn with_capacity(_cap: usize) -> Self {
        Self {
            vec: Vec::with_capacity(_cap),
        }
    }
    #[inline]
    pub(crate) fn get(&mut self, offset: &u64) -> Option<usize> {
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
    #[inline]
    pub(crate) fn insert(&mut self, offset: &u64, idx: usize) {
        match self.vec.binary_search_by(|a| a.0.cmp(offset)) {
            Ok(x) => {
                self.vec[x].1 = idx;
            }
            Err(x) => {
                self.vec.insert(x, (*offset, idx));
            }
        }
    }
    #[inline]
    pub(crate) fn remove(&mut self, offset: &u64) -> Option<usize> {
        match self.vec.binary_search_by(|a| a.0.cmp(offset)) {
            Ok(x) => Some(self.vec.remove(x).1),
            Err(_x) => None,
        }
    }
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.vec.clear();
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
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 24);
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 16);
        }
        #[cfg(target_pointer_width = "32")]
        {
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 12);
            #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 12);
            #[cfg(any(target_arch = "arm", target_arch = "mips"))]
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 16);
        }
    }
}
