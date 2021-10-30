#![allow(dead_code)]
/**
Semantic Type

defines several semantic types. They are written in the New Type pattern.
*/
use std::cmp::{Ordering, PartialOrd};
use std::convert::{Infallible, TryFrom, TryInto};
use std::fmt::Display;
use std::marker::PhantomData;
use std::num::TryFromIntError;

pub type RecordOffset = Offset<Record>;
pub type NodeOffset = Offset<Node>;

pub type RecordSize = Size<Record>;
pub type NodeSize = Size<Node>;

pub type KeyLength = Length<Key>;
pub type ValueLength = Length<Value>;

pub type KeysCount = Count<Key>;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Record();
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Node();
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Key();
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Value();

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct Offset<T> {
    val: u64,
    _phantom: PhantomData<T>,
}

impl<T> Offset<T> {
    pub fn new(val: u64) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    pub fn as_value(&self) -> u64 {
        self.val
    }
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Offset<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Size<T> {
    val: u32,
    _phantom: PhantomData<T>,
}

impl<T> Size<T> {
    pub fn new(val: u32) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    pub fn as_value(&self) -> u32 {
        self.val
    }
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Size<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

impl<T> PartialEq<Self> for Size<T> {
    fn eq(&self, other: &Self) -> bool {
        self.val.eq(&other.val)
    }
}

impl<T> PartialOrd<Self> for Size<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.val.partial_cmp(&other.val)
    }
}

impl<T> Eq for Size<T> {}

impl<T> Ord for Size<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.val.cmp(&other.val)
    }
}

impl<T> TryFrom<Size<T>> for usize {
    type Error = TryFromIntError;
    fn try_from(value: Size<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct Length<T> {
    val: u32,
    _phantom: PhantomData<T>,
}

impl<T> Length<T> {
    pub fn new(val: u32) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    pub fn as_value(&self) -> u32 {
        self.val
    }
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Length<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

impl<T> TryFrom<Length<T>> for usize {
    type Error = TryFromIntError;
    fn try_from(value: Length<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct Count<T> {
    val: u16,
    _phantom: PhantomData<T>,
}

impl<T> Count<T> {
    pub fn new(val: u16) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    pub fn as_value(&self) -> u16 {
        self.val
    }
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Count<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

impl<T> TryFrom<Count<T>> for usize {
    type Error = Infallible;
    fn try_from(value: Count<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

//--
#[cfg(test)]
mod debug {
    use super::{
        KeyLength, KeysCount, NodeOffset, NodeSize, RecordOffset, RecordSize, ValueLength,
    };
    //
    #[test]
    fn test_size_of() {
        assert_eq!(std::mem::size_of::<RecordOffset>(), 8);
        assert_eq!(std::mem::size_of::<NodeOffset>(), 8);
        assert_eq!(std::mem::size_of::<RecordSize>(), 4);
        assert_eq!(std::mem::size_of::<NodeSize>(), 4);
        assert_eq!(std::mem::size_of::<KeyLength>(), 4);
        assert_eq!(std::mem::size_of::<ValueLength>(), 4);
        assert_eq!(std::mem::size_of::<KeysCount>(), 2);
    }
}
