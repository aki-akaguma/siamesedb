/*!
Semantic Type

defines several semantic types. They are written in the New Type pattern.
*/
use std::cmp::PartialOrd;
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::marker::PhantomData;
use std::num::TryFromIntError;

#[cfg(not(any(target_pointer_width = "64", target_pointer_width = "32")))]
use std::convert::Infallible;

pub type PieceOffset<T> = Offset<Piece<T>>;
pub type KeyPieceOffset = PieceOffset<Key>;
pub type ValuePieceOffset = PieceOffset<Value>;
pub type NodePieceOffset = Offset<Piece<Node>>;

pub type PieceSize<T> = Size<Piece<T>>;
pub type KeyPieceSize = PieceSize<Key>;
pub type ValuePieceSize = PieceSize<Value>;
pub type NodePieceSize = Size<Piece<Node>>;

pub type KeyLength = Length<Key>;
pub type ValueLength = Length<Value>;

pub type KeysCount = Count<Key>;

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Piece<T> {
    _phantom: PhantomData<fn() -> T>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Node;

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Key;

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Value;

/// The file offset of key-file, value-file, idx-file.
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Offset<T> {
    val: u64,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Offset<T> {
    #[inline]
    pub fn new(val: u64) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    #[inline]
    pub fn as_value(&self) -> u64 {
        self.val
    }
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Offset<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

impl<T> TryFrom<Offset<T>> for u32 {
    type Error = TryFromIntError;
    #[inline]
    fn try_from(value: Offset<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

impl<T> From<Offset<T>> for u64 {
    #[inline]
    fn from(value: Offset<T>) -> Self {
        value.val
    }
}

impl<T> std::ops::Add<Size<T>> for Offset<T> {
    type Output = Offset<T>;
    #[inline]
    fn add(self, rhs: Size<T>) -> Self::Output {
        Offset::new(self.val + rhs.val as u64)
    }
}

impl<T> std::ops::Add<PieceSize<T>> for Offset<T> {
    type Output = Offset<T>;
    #[inline]
    fn add(self, rhs: PieceSize<T>) -> Self::Output {
        Offset::new(self.val + rhs.val as u64)
    }
}

impl<T> std::ops::Sub<Offset<T>> for Offset<T> {
    type Output = Size<T>;
    #[inline]
    fn sub(self, rhs: Offset<T>) -> Self::Output {
        let val = self.val - rhs.val;
        Size::new(val.try_into().unwrap_or_else(|err| {
            panic!(
                "{} - {} = {} : {}",
                self.as_value(),
                rhs.as_value(),
                val,
                err
            )
        }))
    }
}

/// The size of key-piece, value-piece, node-piece
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Size<T> {
    val: u32,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Size<T> {
    #[inline]
    pub fn new(val: u32) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    #[inline]
    pub fn as_value(&self) -> u32 {
        self.val
    }
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Size<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

#[cfg(not(any(target_pointer_width = "64", target_pointer_width = "32")))]
impl<T> TryFrom<Size<T>> for usize {
    type Error = TryFromIntError;
    #[inline]
    fn try_from(value: Size<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

#[cfg(any(target_pointer_width = "64", target_pointer_width = "32"))]
impl<T> From<Size<T>> for usize {
    #[inline]
    fn from(value: Size<T>) -> Self {
        value.val as usize
    }
}

impl<T> From<Size<T>> for u32 {
    #[inline]
    fn from(value: Size<T>) -> Self {
        value.val
    }
}

/// The byte length of key or value
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Length<T> {
    val: u32,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Length<T> {
    #[inline]
    pub fn new(val: u32) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    #[inline]
    pub fn as_value(&self) -> u32 {
        self.val
    }
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Length<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

#[cfg(not(any(target_pointer_width = "64", target_pointer_width = "32")))]
impl<T> TryFrom<Length<T>> for usize {
    type Error = TryFromIntError;
    #[inline]
    fn try_from(value: Length<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

#[cfg(any(target_pointer_width = "64", target_pointer_width = "32"))]
impl<T> From<Length<T>> for usize {
    #[inline]
    fn from(value: Length<T>) -> Self {
        value.val as usize
    }
}

impl<T> From<Length<T>> for u32 {
    #[inline]
    fn from(value: Length<T>) -> Self {
        value.val
    }
}

/// The count of keys
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Count<T> {
    val: u16,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Count<T> {
    #[inline]
    pub fn new(val: u16) -> Self {
        Self {
            val,
            _phantom: PhantomData,
        }
    }
    #[inline]
    pub fn as_value(&self) -> u16 {
        self.val
    }
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.val == 0
    }
}

impl<T> Display for Count<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.val.fmt(formatter)
    }
}

#[cfg(not(any(target_pointer_width = "64", target_pointer_width = "32")))]
impl<T> TryFrom<Count<T>> for usize {
    type Error = Infallible;
    #[inline]
    fn try_from(value: Count<T>) -> Result<Self, Self::Error> {
        value.val.try_into()
    }
}

#[cfg(any(target_pointer_width = "64", target_pointer_width = "32"))]
impl<T> From<Count<T>> for usize {
    #[inline]
    fn from(value: Count<T>) -> Self {
        value.val as usize
    }
}

impl<T> From<Count<T>> for u16 {
    #[inline]
    fn from(value: Count<T>) -> Self {
        value.val
    }
}

//--
#[cfg(test)]
mod debug {
    use super::{
        KeyLength, KeyPieceOffset, KeyPieceSize, KeysCount, NodePieceOffset, NodePieceSize,
        ValueLength, ValuePieceOffset, ValuePieceSize,
    };
    //
    #[test]
    fn test_size_of() {
        assert_eq!(std::mem::size_of::<KeyPieceOffset>(), 8);
        assert_eq!(std::mem::size_of::<ValuePieceOffset>(), 8);
        assert_eq!(std::mem::size_of::<NodePieceOffset>(), 8);
        assert_eq!(std::mem::size_of::<KeyPieceSize>(), 4);
        assert_eq!(std::mem::size_of::<ValuePieceSize>(), 4);
        assert_eq!(std::mem::size_of::<NodePieceSize>(), 4);
        assert_eq!(std::mem::size_of::<KeyLength>(), 4);
        assert_eq!(std::mem::size_of::<ValueLength>(), 4);
        assert_eq!(std::mem::size_of::<KeysCount>(), 2);
    }
}
