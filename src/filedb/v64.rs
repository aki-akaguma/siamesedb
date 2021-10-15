#[cfg(any(feature = "dat_v64v64", feature = "idx_v64v64"))]
pub fn decode_vint64<R: std::io::Read + ?Sized>(inp: &mut R, buf: &mut [u8; 9]) -> std::io::Result<u64> {
    //let mut buf = [0u8; 9];
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
