use super::super::super::{DbMapKeyType, HashValue};
use super::FileDbMap;
use std::fmt::{Display, Error, Formatter};
use std::ops::Deref;

/// DbBytes Map in a file databse.
pub type FileDbMapDbBytes = FileDbMap<DbBytes>;

/// db-key type. `&[u8]` can be used as keys.
#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct DbBytes(Vec<u8>);

impl DbMapKeyType for DbBytes {
    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        DbBytes(bytes.to_vec())
    }
    #[inline]
    fn signature() -> [u8; 8] {
        *b"bytes\0\0\0"
    }
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
    fn cmp_u8(&self, other: &[u8]) -> std::cmp::Ordering {
        self.0.as_slice().cmp(other)
    }
}
impl HashValue for DbBytes {}

impl Display for DbBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let ss = String::from_utf8_lossy(&self.0).to_string();
        write!(f, "'{}'", ss)
    }
}

impl Deref for DbBytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &<Self as Deref>::Target {
        &self.0
    }
}

/*
impl Borrow<[u8]> for DbBytes {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}
*/

impl From<&[u8]> for DbBytes {
    #[inline]
    fn from(a: &[u8]) -> Self {
        DbBytes(a.to_vec())
    }
}

impl From<Vec<u8>> for DbBytes {
    #[inline]
    fn from(a: Vec<u8>) -> Self {
        DbBytes(a)
    }
}

impl From<&str> for DbBytes {
    #[inline]
    fn from(a: &str) -> Self {
        DbBytes(a.as_bytes().to_vec())
    }
}

impl From<String> for DbBytes {
    #[inline]
    fn from(a: String) -> Self {
        DbBytes(a.into_bytes())
    }
}

impl From<&String> for DbBytes {
    #[inline]
    fn from(a: &String) -> Self {
        DbBytes(a.as_bytes().to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for DbBytes {
    #[inline]
    fn from(a: &[u8; N]) -> Self {
        DbBytes(a.to_vec())
    }
}

impl From<u64> for DbBytes {
    #[inline]
    fn from(a: u64) -> Self {
        DbBytes(a.to_be_bytes().to_vec())
    }
}

impl From<&u64> for DbBytes {
    #[inline]
    fn from(a: &u64) -> Self {
        DbBytes(a.to_be_bytes().to_vec())
    }
}

/*
impl From<DbBytes> for DbBytes {
    #[inline]
    fn from(a: DbBytes) -> Self {
        DbBytes(a.0)
    }
}
*/

impl From<&DbBytes> for DbBytes {
    #[inline]
    fn from(a: &DbBytes) -> Self {
        DbBytes(a.0.clone())
    }
}
