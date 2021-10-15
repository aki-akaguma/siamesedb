pub fn decode_vli_len(byte_1st: u8) -> usize {
    match byte_1st {
        0..=0xF0 => 1,
        0xF1..=0xF8 => 2,
        0xF9 => 3,
        0xFA => 4,
        0xFB => 5,
        0xFC => 6,
        0xFD => 7,
        0xFE => 8,
        0xFF => 9,
    }
}

pub fn decode_vli<R: std::io::Read + ?Sized>(inp: &mut R) -> std::io::Result<u64> {
    let mut buf = [0u8; 9];
    inp.read_exact(&mut buf[0..1])?;
    let byte_1st = buf[0];
    let len = decode_vli_len(byte_1st);
    if len > 1 {
        inp.read_exact(&mut buf[1..len])?;
    }
    match len {
        1 => Ok(byte_1st as u64),
        2 => Ok(0xF0 + 0x0100 * (byte_1st as u64 - 0xF1) + buf[1] as u64),
        3 => Ok(0x08F0 + 0x0100 * (buf[1] as u64) + buf[2] as u64),
        4 => Ok(((buf[3] as u64) << (8 * 2)) + ((buf[2] as u64) << 8) + buf[1] as u64),
        5 => Ok(((buf[4] as u64) << (8 * 3))
            + ((buf[3] as u64) << (8 * 2))
            + ((buf[2] as u64) << 8)
            + buf[1] as u64),
        6 => Ok(((buf[5] as u64) << (8 * 4))
            + ((buf[4] as u64) << (8 * 3))
            + ((buf[3] as u64) << (8 * 2))
            + ((buf[2] as u64) << 8)
            + buf[1] as u64),
        7 => Ok(((buf[6] as u64) << (8 * 5))
            + ((buf[5] as u64) << (8 * 4))
            + ((buf[4] as u64) << (8 * 3))
            + ((buf[3] as u64) << (8 * 2))
            + ((buf[2] as u64) << 8)
            + buf[1] as u64),
        8 => Ok(((buf[7] as u64) << (8 * 6))
            + ((buf[6] as u64) << (8 * 5))
            + ((buf[5] as u64) << (8 * 4))
            + ((buf[4] as u64) << (8 * 3))
            + ((buf[3] as u64) << (8 * 2))
            + ((buf[2] as u64) << 8)
            + buf[1] as u64),
        9 => Ok(((buf[8] as u64) << (8 * 7))
            + ((buf[7] as u64) << (8 * 6))
            + ((buf[6] as u64) << (8 * 5))
            + ((buf[5] as u64) << (8 * 4))
            + ((buf[4] as u64) << (8 * 3))
            + ((buf[3] as u64) << (8 * 2))
            + ((buf[2] as u64) << 8)
            + buf[1] as u64),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("vli: invalid length: {}", len),
        )),
    }
}

pub fn encode_vli(val: u64) -> Vec<u8> {
    let mut vec = Vec::with_capacity(9);
    //
    match val {
        0..=0xF0 => vec.push(val as u8),
        0xF1..=0x08EF => {
            vec.push(((val - 0xF0) / 0x0100 + 0xF1) as u8);
            vec.push(((val - 0x08F0) % 0x0100) as u8);
        }
        0x08F0..=0x0001_08EF => {
            vec.push(0xF9);
            vec.push(((val - 0x08F0) / 0x0100) as u8);
            vec.push(((val - 0x08F0) % 0x0100) as u8);
        }
        0x0001_08F0..=0x00FF_FFFF => {
            vec.push(0xFA);
            vec.push((val & 0xFF) as u8);
            vec.push(((val & 0xFF00) >> 8) as u8);
            vec.push(((val & 0xFF_0000) >> (8 * 2)) as u8);
        }
        0x0100_0000..=0xFFFF_FFFF => {
            vec.push(0xFB);
            vec.push((val & 0xFF) as u8);
            vec.push(((val & 0xFF00) >> 8) as u8);
            vec.push(((val & 0xFF_0000) >> (8 * 2)) as u8);
            vec.push(((val & 0xFF00_0000) >> (8 * 3)) as u8);
        }
        0x0001_0000_0000..=0x00FF_FFFF_FFFF => {
            vec.push(0xFC);
            vec.push((val & 0xFF) as u8);
            vec.push(((val & 0xFF00) >> 8) as u8);
            vec.push(((val & 0xFF_0000) >> (8 * 2)) as u8);
            vec.push(((val & 0xFF00_0000) >> (8 * 3)) as u8);
            vec.push(((val & 0xFF_0000_0000) >> (8 * 4)) as u8);
        }
        0x0100_0000_0000..=0xFFFF_FFFF_FFFF => {
            vec.push(0xFD);
            vec.push((val & 0xFF) as u8);
            vec.push(((val & 0xFF00) >> 8) as u8);
            vec.push(((val & 0xFF_0000) >> (8 * 2)) as u8);
            vec.push(((val & 0xFF00_0000) >> (8 * 3)) as u8);
            vec.push(((val & 0xFF_0000_0000) >> (8 * 4)) as u8);
            vec.push(((val & 0xFF00_0000_0000) >> (8 * 5)) as u8);
        }
        0x0001_0000_0000_0000..=0x00FF_FFFF_FFFF_FFFF => {
            vec.push(0xFE);
            vec.push((val & 0xFF) as u8);
            vec.push(((val & 0xFF00) >> 8) as u8);
            vec.push(((val & 0xFF_0000) >> (8 * 2)) as u8);
            vec.push(((val & 0xFF00_0000) >> (8 * 3)) as u8);
            vec.push(((val & 0xFF_0000_0000) >> (8 * 4)) as u8);
            vec.push(((val & 0xFF00_0000_0000) >> (8 * 5)) as u8);
            vec.push(((val & 0xFF_0000_0000_0000) >> (8 * 6)) as u8);
        }
        _ => {
            vec.push(0xFF);
            vec.push((val & 0xFF) as u8);
            vec.push(((val & 0xFF00) >> 8) as u8);
            vec.push(((val & 0xFF_0000) >> (8 * 2)) as u8);
            vec.push(((val & 0xFF00_0000) >> (8 * 3)) as u8);
            vec.push(((val & 0xFF_0000_0000) >> (8 * 4)) as u8);
            vec.push(((val & 0xFF00_0000_0000) >> (8 * 5)) as u8);
            vec.push(((val & 0xFF_0000_0000_0000) >> (8 * 6)) as u8);
            vec.push(((val & 0xFF00_0000_0000_0000) >> (8 * 7)) as u8);
        }
    }
    //
    vec
}

//
// ref.) https://sqlite.org/src4/doc/trunk/www/varint.wiki
//
