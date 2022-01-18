#[cfg(feature = "oi_hash_turbo")]
use std::collections::HashMap;

#[cfg(feature = "oi_myhash")]
use std::hash::BuildHasherDefault;

#[cfg(feature = "oi_myhash")]
use std::hash::Hasher;

#[cfg(feature = "oi_myhash")]
#[derive(Debug, Default)]
struct MyHasher(u64);

#[cfg(feature = "oi_myhash")]
impl Hasher for MyHasher {
    fn write(&mut self, bytes: &[u8]) {
        let bytes_len = bytes.len();
        if bytes_len == 8 {
            let mut ary = [0u8; 8];
            ary.copy_from_slice(bytes);
            let mut a = u64::from_ne_bytes(ary);
            a = a ^ a >> 12;
            a = a ^ a << 25;
            a = a ^ a >> 27;
            self.0 = a;
        } else {
            for i in 0..bytes.len() {
                let a = unsafe { *bytes.get_unchecked(i) };
                self.0 = self.0.wrapping_add(a as u64);
            }
        }
    }
    fn write_u64(&mut self, val: u64) {
        let mut a = val;
        a = a ^ a >> 12;
        a = a ^ a << 25;
        a = a ^ a >> 27;
        self.0 = a;
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Implements key-value sorted vec.
/// the key is the offset from start the file.
/// the value is the index of vec<>.
#[derive(Debug)]
pub(crate) struct OffsetIndex {
    pub(crate) vec: Vec<(u64, usize)>,
    #[cfg(feature = "oi_hash_turbo")]
    #[cfg(not(feature = "oi_myhash"))]
    map: HashMap<u64, usize>,
    #[cfg(feature = "oi_hash_turbo")]
    #[cfg(feature = "oi_myhash")]
    map: HashMap<u64, usize, BuildHasherDefault<MyHasher>>,
}
impl OffsetIndex {
    pub(crate) fn with_capacity(_cap: usize) -> Self {
        Self {
            vec: Vec::with_capacity(_cap),
            #[cfg(feature = "oi_hash_turbo")]
            #[cfg(not(feature = "oi_myhash"))]
            map: HashMap::with_capacity(_cap),
            #[cfg(feature = "oi_hash_turbo")]
            #[cfg(feature = "oi_myhash")]
            map: HashMap::with_capacity_and_hasher(_cap, Default::default()),
        }
    }
    #[inline]
    pub(crate) fn get(&mut self, offset: &u64) -> Option<usize> {
        #[cfg(feature = "oi_hash_turbo")]
        {
            self.map.get(offset).map(|&o| o)
        }
        #[cfg(not(feature = "oi_hash_turbo"))]
        {
            let slice = &self.vec;
            if let Ok(x) = slice.binary_search_by(|a| a.0.cmp(offset)) {
                Some(slice[x].1)
            } else {
                None
            }
        }
        /*
        let slice = &self.vec;
        if slice.is_empty() {
            return None;
        }
        if *offset < slice[0].0 {
            return None;
        }
        if *offset > slice[slice.len() - 1].0 {
            return None;
        }
        if let Ok(x) = slice.binary_search_by(|a| a.0.cmp(offset)) {
            Some(slice[x].1)
        } else {
            None
        }
        */
    }
    #[inline]
    pub(crate) fn insert(&mut self, offset: &u64, idx: usize) {
        #[cfg(feature = "oi_hash_turbo")]
        {
            let _ = self.map.insert(*offset, idx);
        }
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
        #[cfg(feature = "oi_hash_turbo")]
        {
            let _ = self.map.remove(offset);
        }
        match self.vec.binary_search_by(|a| a.0.cmp(offset)) {
            Ok(x) => Some(self.vec.remove(x).1),
            Err(_x) => None,
        }
    }
    #[inline]
    pub(crate) fn clear(&mut self) {
        #[cfg(feature = "oi_hash_turbo")]
        {
            self.map.clear();
        }
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
            #[cfg(not(feature = "oi_hash_turbo"))]
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 24);
            #[cfg(feature = "oi_hash_turbo")]
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 56);
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 16);
        }
        #[cfg(target_pointer_width = "32")]
        {
            #[cfg(not(feature = "oi_hash_turbo"))]
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 12);
            #[cfg(feature = "oi_hash_turbo")]
            assert_eq!(std::mem::size_of::<OffsetIndex>(), 28);
            #[cfg(not(any(target_arch = "arm", target_arch = "mips")))]
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 12);
            #[cfg(any(target_arch = "arm", target_arch = "mips"))]
            assert_eq!(std::mem::size_of::<(u64, usize)>(), 16);
        }
    }
}
