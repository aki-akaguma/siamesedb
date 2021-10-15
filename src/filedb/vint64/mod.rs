mod error;
mod signed;
pub(crate) mod vint64;

pub(crate) use self::error::Error;
pub(crate) use self::vint64::decode;
pub(crate) use self::vint64::encoded_len;
pub(crate) use self::vint64::VInt64;

#[cfg(feature = "vf_vint64")]
pub fn decode_vint64<R: std::io::Read + ?Sized>(
    inp: &mut R,
    buf: &mut [u8; 9],
) -> std::io::Result<u64> {
    inp.read_exact(&mut buf[0..1])?;
    let byte_1st = buf[0];
    let len = vint64::decoded_len(byte_1st);
    if len > 1 {
        inp.read_exact(&mut buf[1..len])?;
    }
    match vint64::decode(&mut &buf[0..len]) {
        Ok(i) => Ok(i),
        Err(err) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("{}", err),
        )),
    }
}
