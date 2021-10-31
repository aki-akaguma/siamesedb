use super::buf::BufFile;
use std::io::Result;

#[cfg(feature = "vf_vu64")]
use super::vu64_io::{ReadVu64, WriteVu64};

#[cfg(feature = "vf_vu64")]
impl ReadVu64 for BufFile {
    fn read_one_byte(&mut self) -> Result<u8> {
        self.read_one_byte()
    }
    fn read_exact_max8byte(&mut self, buf: &mut [u8]) -> Result<()> {
        debug_assert!(buf.len() <= 8, "buf.len(): {} <= 8", buf.len());
        self.read_exact_small(buf)
    }
}

#[cfg(feature = "vf_vu64")]
impl WriteVu64 for BufFile {}
