/*!
The variable length integer encoding of u64.
This is a simple and fast encoder/decoder.

## format pattern

| Prefix     | Precision | Total Bytes |
|------------|-----------|-------------|
| `0xxxxxxx` | 7 bits    | 1 byte      |
| `10xxxxxx` | 14 bits   | 2 bytes     |
| `110xxxxx` | 21 bits   | 3 bytes     |
| `1110xxxx` | 28 bits   | 4 bytes     |
| `11110xxx` | 35 bits   | 5 bytes     |
| `111110xx` | 42 bits   | 6 bytes     |
| `1111110x` | 49 bits   | 7 bytes     |
| `11111110` | 56 bits   | 8 bytes     |
| `11111111` | 64 bits   | 9 bytes     |

This format is like [`vint64`](https://crates.io/crates/vint64),
but 0x00 is represented by 0x00.
*/
use core::convert::{TryFrom, TryInto};
use core::fmt::{self, Debug, Display};

/// Maximum length of a `v64` in bytes
pub const MAX_BYTES: usize = 9;

/// `v64`: serialized variable-length 64-bit integers.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct V64 {
    /// Encoded length in bytes
    length: u8,
    /// Serialized variable-length integer
    bytes: [u8; MAX_BYTES],
}

impl AsRef<[u8]> for V64 {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.length as usize]
    }
}

impl Debug for V64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes_ref = self.as_ref();
        write!(f, "V64({})", decode(bytes_ref).unwrap())
    }
}

impl From<u64> for V64 {
    #[inline]
    fn from(value: u64) -> V64 {
        encode(value)
    }
}

/*
impl From<i64> for V64 {
    #[inline]
    fn from(value: i64) -> V64 {
        signed::zigzag::encode(value).into()
    }
}
*/

impl TryFrom<&[u8]> for V64 {
    type Error = Error;

    #[inline]
    fn try_from(slice: &[u8]) -> Result<Self, Error> {
        decode(slice).map(V64::from)
    }
}

/// Get the length of an encoded `v64` for the given value in bytes.
pub fn encoded_len(value: u64) -> usize {
    match value.leading_zeros() {
        0..=7 => 9,
        8..=14 => 8,
        15..=21 => 7,
        22..=28 => 6,
        29..=35 => 5,
        36..=42 => 4,
        43..=49 => 3,
        50..=56 => 2,
        57..=64 => 1,
        _ => {
            // SAFETY:
            //
            // The `leading_zeros` intrinsic returns the number of bits that
            // contain a zero bit. The result will always be in the range of
            // 0..=64 for a `u64`, so the above pattern is exhaustive, however
            // it is not exhaustive over the return type of `u32`. Because of
            // this, we mark the "uncovered" part of the match as unreachable
            // for performance reasons.
            #[allow(unsafe_code)]
            unsafe {
                core::hint::unreachable_unchecked()
            }
        }
    }
}

/// Get the length of a `v64` from the first byte.
///
/// NOTE: The returned value is inclusive of the first byte itself.
#[inline]
pub fn decoded_len(byte: u8) -> usize {
    byte.leading_ones() as usize + 1
}

/// Encode an unsigned 64-bit integer as `v64`.
#[inline]
pub fn encode(value: u64) -> V64 {
    let mut bytes = [0u8; MAX_BYTES];
    let length = encoded_len(value);
    //
    if length == 1 {
        // 1-byte special case
        bytes[0] = value as u8;
    } else if length < 8 {
        let encoded = value << length as u64;
        bytes[..8].copy_from_slice(&encoded.to_le_bytes());
        let b1st = bytes[0];
        bytes[0] = !((!(b1st >> 1)) >> (length - 1));
    } else if length == 8 {
        // 8-byte special case
        bytes[1..].copy_from_slice(&value.to_le_bytes());
        bytes[0] = 0xFE;
    } else if length == 9 {
        // 9-byte special case
        bytes[1..].copy_from_slice(&value.to_le_bytes());
        bytes[0] = 0xFF;
    } else {
        #[allow(unsafe_code)]
        unsafe {
            core::hint::unreachable_unchecked()
        }
    }
    //
    V64 {
        bytes,
        length: length as u8,
    }
}

/// Decode a `v64`-encoded unsigned 64-bit integer.
///
/// Accepts a mutable reference to a slice containing the `v64`.
/// Upon success, the reference is updated to begin at the byte immediately
/// after the encoded `v64`.
#[inline]
fn decode(bytes: &[u8]) -> Result<u64, Error> {
    if bytes.len() < 1 {
        return Err(Error::Truncated);
    }
    let length = decoded_len(bytes[0]);
    decode_with_length(length, bytes)
}

#[inline]
fn decode_with_length(length: usize, bytes: &[u8]) -> Result<u64, Error> {
    //let length = decoded_len(*bytes.first().ok_or(Error::Truncated)?);
    //
    if bytes.len() < length {
        return Err(Error::Truncated);
    }
    //
    let result = if length == 1 {
        // 1-byte special case
        bytes[0] as u64
    } else if length < 8 {
        let mut encoded = [0u8; 8];
        encoded[..length].copy_from_slice(&bytes[..length]);
        encoded[0] <<= length;
        u64::from_le_bytes(encoded) >> length
    } else if length == 8 {
        // 8-byte special case
        u64::from_le_bytes(bytes[1..8].try_into().unwrap())
    } else if length == 9 {
        // 9-byte special case
        u64::from_le_bytes(bytes[1..9].try_into().unwrap())
    } else {
        #[allow(unsafe_code)]
        unsafe {
            core::hint::unreachable_unchecked()
        }
    };
    //
    // Ensure there are no superfluous leading (little-endian) zeros
    if length == 1 || result >= (1 << (7 * (length - 1))) {
        Ok(result)
    } else {
        Err(Error::LeadingZeroes)
    }
}

/// Error type
#[derive(Copy, Clone, Debug)]
pub enum Error {
    /// Value contains unnecessary leading zeroes
    LeadingZeroes,

    /// Value is truncated / malformed
    Truncated,
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Error::LeadingZeroes => "leading zeroes in vint64 value",
            Error::Truncated => "truncated vint64 value",
        })
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}


#[cfg(test)]
mod tests {
    //use super::{decode, encode, signed};
    use super::{decode, encode};
    //
    #[test]
    fn encode_decode_1() {
        let val = 1234;
        assert_eq!(decode(encode(val).as_ref()).unwrap(), val);
    }
    #[test]
    fn encode_decode_2() {
        let val = 123456789;
        assert_eq!(decode(encode(val).as_ref()).unwrap(), val);
    }
    #[test]
    fn encode_zero() {
        assert_eq!(encode(0).as_ref(), &[0]);
    }
    #[test]
    fn encode_bit_pattern_examples() {
        assert_eq!(encode(0x0f0f).as_ref(), &[0x8F, 0x3c]);
        assert_eq!(encode(0x0f0f_f0f0).as_ref(), &[0xE0, 0x0f, 0xff, 0xf0]);
        assert_eq!(
            encode(0x0f0f_f0f0_0f0f).as_ref(),
            &[0xFD, 0x87, 0x07, 0x78, 0xf8, 0x87, 0x07]
        );
        assert_eq!(
            encode(0x0f0f_f0f0_0f0f_f0f0).as_ref(),
            &[0xFF, 0xf0, 0xf0, 0x0f, 0x0f, 0xf0, 0xf0, 0x0f, 0x0f]
        );
    }
    #[test]
    fn encode_maxint() {
        assert_eq!(
            encode(core::u64::MAX).as_ref(),
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
    }
    /*
    #[test]
    fn encode_signed_values() {
        assert_eq!(
            signed::encode(0x0f0f_f0f0).as_ref(),
            &[0x10, 0x3c, 0xfc, 0xc3, 0x03]
        );

        assert_eq!(
            signed::encode(-0x0f0f_f0f0).as_ref(),
            &[0xf0, 0x3b, 0xfc, 0xc3, 0x03]
        );
    }
    */
    #[test]
    fn decode_zero() {
        let slice = [0].as_ref();
        assert_eq!(decode(slice).unwrap(), 0);
    }
    #[test]
    fn decode_bit_pattern_examples() {
        let slice = [0x8F, 0x3c].as_ref();
        assert_eq!(decode(slice).unwrap(), 0x0f0f);

        let slice = [0xE0, 0x0f, 0xff, 0xf0].as_ref();
        assert_eq!(decode(slice).unwrap(), 0x0f0f_f0f0);

        let slice = [0xFD, 0x87, 0x07, 0x78, 0xf8, 0x87, 0x07].as_ref();
        assert_eq!(decode(slice).unwrap(), 0x0f0f_f0f0_0f0f);

        let slice = [0xFF, 0xf0, 0xf0, 0x0f, 0x0f, 0xf0, 0xf0, 0x0f, 0x0f].as_ref();
        assert_eq!(decode(slice).unwrap(), 0x0f0f_f0f0_0f0f_f0f0);
    }
    #[test]
    fn decode_maxint() {
        let slice = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff].as_ref();
        assert_eq!(decode(slice).unwrap(), core::u64::MAX);
    }
    #[test]
    fn decode_truncated() {
        let slice = [0xF0].as_ref();
        assert!(decode(slice).is_err());
        let slice = [0xF8, 0x0f, 0xff].as_ref();
        assert!(decode(slice).is_err());
    }
    #[test]
    fn decode_leading_ones() {
        let slice = [0xF8, 0x00, 0x00, 0x00].as_ref();
        assert!(decode(slice).is_err());
    }
    /*
    #[test]
    fn decode_signed_values() {
        let mut slice = [0x10, 0x3c, 0xfc, 0xc3, 0x03].as_ref();
        assert_eq!(signed::decode(&mut slice).unwrap(), 0x0f0f_f0f0);

        let mut slice = [0xf0, 0x3b, 0xfc, 0xc3, 0x03].as_ref();
        assert_eq!(signed::decode(&mut slice).unwrap(), -0x0f0f_f0f0);
    }
    */
}

pub fn decode_v64<R: std::io::Read + ?Sized>(
    inp: &mut R,
) -> std::io::Result<u64> {
    let mut buf = [0u8; MAX_BYTES];
    inp.read_exact(&mut buf[0..1])?;
    let byte_1st = buf[0];
    let len = decoded_len(byte_1st);
    if len > 1 {
        inp.read_exact(&mut buf[1..len])?;
    }
    match decode_with_length(len, &buf[0..len]) {
        Ok(i) => Ok(i),
        Err(err) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("{}", err),
        )),
    }
}
