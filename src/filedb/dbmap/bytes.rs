//use std::borrow::Borrow;
use std::fmt::{Display, Error, Formatter};
use std::ops::Deref;

/// Bytes
/// New type pattern of `Vec<u8>`.
#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Bytes(pub(crate) Vec<u8>);

impl Display for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let ss = String::from_utf8_lossy(&self.0).to_string();
        write!(f, "'{}'", ss)
    }
}

impl Deref for Bytes {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &<Self as Deref>::Target {
        &self.0
    }
}
/*
impl Borrow<[u8]> for Bytes {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}
*/
impl From<&[u8]> for Bytes {
    #[inline]
    fn from(a: &[u8]) -> Self {
        Bytes(a.to_vec())
    }
}

impl From<Vec<u8>> for Bytes {
    #[inline]
    fn from(a: Vec<u8>) -> Self {
        Bytes(a)
    }
}

impl From<&str> for Bytes {
    #[inline]
    fn from(a: &str) -> Self {
        Bytes(a.as_bytes().to_vec())
    }
}

impl From<String> for Bytes {
    #[inline]
    fn from(a: String) -> Self {
        Bytes(a.into_bytes())
    }
}

impl From<&String> for Bytes {
    #[inline]
    fn from(a: &String) -> Self {
        Bytes(a.as_bytes().to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for Bytes {
    #[inline]
    fn from(a: &[u8; N]) -> Self {
        Bytes(a.to_vec())
    }
}
