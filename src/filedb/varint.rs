// ref.) https://en.wikipedia.org/wiki/LEB128

/*
pub fn encode_varint(val: u64) -> Vec<u8> {
    let mut length = 1;
    let mut v = val;
    while v >= 1 << 7 {
        length += 1;
        v >>= 7;
    }
    //
    let mut r = Vec::with_capacity(9);
    for i in 0..length {
        v = val >> (7 * i) & 0x7F;
        if i + 1 != length {
            v |= 0x80;
        }
        r.push(v as u8);
    }
    r
}
*/
pub fn encode_varint(val: u64, enc_buf: &mut Vec<u8>) {
    enc_buf.clear();
    let mut length = 1;
    let mut v = val;
    while v >= 1 << 7 {
        length += 1;
        v >>= 7;
    }
    //
    for i in 0..length {
        v = val >> (7 * i) & 0x7F;
        if i + 1 != length {
            v |= 0x80;
        }
        assert!(v <= 0xFF);
        enc_buf.push(v as u8);
    }
}

pub fn decode_varint<R: std::io::Read>(inp: &mut std::io::Bytes<R>) -> std::io::Result<u64> {
    let mut i = 0;
    let mut n = 0;
    loop {
        let bt = match inp.next() {
            Some(r) => r?,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "under length at decode varint.",
                ));
            }
        };
        if i > 9 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "over length at decode varint.",
            ));
        }
        n |= ((bt & 0x7F) as u64) << (7 * i);
        if bt <= 0x7F {
            return Ok(n);
        }
        i += 1;
    }
}

pub fn encode_zigzag(n: i64) -> u64 {
    (n << 1) as u64 ^ (n >> 63) as u64
}
pub fn decode_zigzag(n: u64) -> i64 {
    (n >> 1) as i64 ^ (n as i64) << 63 >> 63
}

//--
#[cfg(test)]
mod debug {
    use super::decode_varint;
    use super::encode_varint;
    use std::io::Read;
    //
    #[test]
    fn test_encode_decode_0() {
        let mut enc_buf = Vec::with_capacity(9);
        encode_varint(150, &mut enc_buf);
        assert_eq!(enc_buf, vec![0b1001_0110, 0b0000_0001]);
        //
        let buff = std::io::Cursor::new(vec![0b1001_0110, 0b0000_0001]);
        let mut bt_itr = buff.bytes();
        assert_eq!(decode_varint(&mut bt_itr).unwrap(), 150);
    }
    #[test]
    fn test_encode_decode_1() {
        let mut enc_buf = Vec::with_capacity(9);
        let mut i = 0;
        let mut n = 0;
        loop {
            encode_varint(n, &mut enc_buf);
            let buff = std::io::Cursor::new(&mut enc_buf);
            let mut bt_itr = buff.bytes();
            assert_eq!(decode_varint(&mut bt_itr).unwrap(), n);
            //
            if i < 64 {
                n = 1u64 << i;
                i += 1;
            } else {
                break;
            }
        }
    }
    #[test]
    fn test_encode_decode_10() {
        let mut enc_buf = Vec::with_capacity(9);
        encode_varint(0x7FFF_FFFF_FFFF_FFFF, &mut enc_buf);
        assert_eq!(
            enc_buf,
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]
        );
        //
        let buff = std::io::Cursor::new(vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
        let mut bt_itr = buff.bytes();
        assert_eq!(decode_varint(&mut bt_itr).unwrap(), 0x7FFF_FFFF_FFFF_FFFF);
    }
    #[test]
    fn test_encode_decode_11() {
        let mut enc_buf = Vec::with_capacity(10);
        encode_varint(0xFFFF_FFFF_FFFF_FFFF, &mut enc_buf);
        assert_eq!(
            enc_buf,
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]
        );
        //
        let buff = std::io::Cursor::new(vec![
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
        ]);
        let mut bt_itr = buff.bytes();
        assert_eq!(decode_varint(&mut bt_itr).unwrap(), 0xFFFF_FFFF_FFFF_FFFF);
    }
    #[test]
    fn test_minimum() {
        let mut enc_buf = Vec::with_capacity(9);
        encode_varint(u64::MIN, &mut enc_buf);
        assert_eq!(enc_buf, vec![0]);
        //
        let buff = std::io::Cursor::new(vec![0]);
        let mut bt_itr = buff.bytes();
        assert_eq!(decode_varint(&mut bt_itr).unwrap(), u64::MIN);
    }
    #[test]
    fn test_maximum() {
        let mut enc_buf = Vec::with_capacity(9);
        encode_varint(u64::MAX, &mut enc_buf);
        assert_eq!(
            enc_buf,
            vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1]
        );
        //
        let buff = std::io::Cursor::new(vec![255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
        let mut bt_itr = buff.bytes();
        assert_eq!(decode_varint(&mut bt_itr).unwrap(), u64::MAX);
    }
}
