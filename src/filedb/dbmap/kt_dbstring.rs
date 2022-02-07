use super::super::super::{DbMapKeyType, HashValue};
use super::FileDbMap;
use std::fmt::{Display, Error, Formatter};
use std::ops::Deref;

/// DbBytes Map in a file databse.
pub type FileDbMapDbString = FileDbMap<DbString>;

/// db-key type. `String` can be used as key.
#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct DbString(Vec<u8>);

impl DbMapKeyType for DbString {
    #[inline]
    fn from_bytes(bytes: &[u8]) -> Self {
        DbString(bytes.to_vec())
    }
    #[inline]
    fn signature() -> [u8; 8] {
        *b"string\0\0"
    }
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
    #[inline]
    fn cmp_u8(&self, other: &[u8]) -> std::cmp::Ordering {
        self.0.as_slice().cmp(other)
    }
}
impl HashValue for DbString {}

impl Display for DbString {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let ss = String::from_utf8_lossy(&self.0).to_string();
        write!(f, "'{}'", ss)
    }
}

impl Deref for DbString {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &<Self as Deref>::Target {
        &self.0
    }
}

/*
impl Borrow<[u8]> for DbString {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}
*/

impl From<&[u8]> for DbString {
    #[inline]
    fn from(a: &[u8]) -> Self {
        DbString(a.to_vec())
    }
}

impl From<Vec<u8>> for DbString {
    #[inline]
    fn from(a: Vec<u8>) -> Self {
        DbString(a)
    }
}

impl From<&str> for DbString {
    #[inline]
    fn from(a: &str) -> Self {
        DbString(a.as_bytes().to_vec())
    }
}

impl From<String> for DbString {
    #[inline]
    fn from(a: String) -> Self {
        DbString(a.into_bytes())
    }
}

impl From<&String> for DbString {
    #[inline]
    fn from(a: &String) -> Self {
        DbString(a.as_bytes().to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for DbString {
    #[inline]
    fn from(a: &[u8; N]) -> Self {
        DbString(a.to_vec())
    }
}

impl From<u64> for DbString {
    #[inline]
    fn from(a: u64) -> Self {
        DbString(a.to_be_bytes().to_vec())
    }
}

impl From<&u64> for DbString {
    #[inline]
    fn from(a: &u64) -> Self {
        DbString(a.to_be_bytes().to_vec())
    }
}

/*
impl From<DbString> for DbString {
    #[inline]
    fn from(a: DbString) -> Self {
        DbString(a.0)
    }
}
*/

impl From<&DbString> for DbString {
    #[inline]
    fn from(a: &DbString) -> Self {
        DbString(a.0.clone())
    }
}
