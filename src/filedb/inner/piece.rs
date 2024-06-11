use super::semtype::*;
use super::vfile::VarFile;
use rabuf::{SmallRead, SmallWrite};
use std::io::Result;

/// piece manager. managing free piece list.
#[derive(Debug)]
pub struct PieceMgr {
    free_list_offset: &'static [u64],
    size_ary: &'static [u32],
}

impl PieceMgr {
    pub fn new(free_list_offset: &'static [u64], size_ary: &'static [u32]) -> Self {
        Self {
            free_list_offset,
            size_ary,
        }
    }
}

impl PieceMgr {
    pub fn free_piece_list_offset_of_header<T>(&self, piece_size: PieceSize<T>) -> u64 {
        let piece_size = piece_size.as_value();
        debug_assert!(piece_size > 0, "piece_size: {} > 0", piece_size);
        for i in 0..self.size_ary.len() {
            if self.size_ary[i] == piece_size {
                return self.free_list_offset[i];
            }
        }
        debug_assert!(
            piece_size > self.size_ary[self.size_ary.len() - 2],
            "piece_size: {} > pi_mgr.size_ary[pi_mgr.size_ary.len() - 2]: {}",
            piece_size,
            self.size_ary[self.size_ary.len() - 2]
        );
        self.free_list_offset[self.free_list_offset.len() - 1]
    }
    pub fn is_large_piece_size<T>(&self, piece_size: PieceSize<T>) -> bool {
        let piece_size = piece_size.as_value();
        piece_size >= self.size_ary[self.size_ary.len() - 1]
    }
    pub fn roundup<T>(&self, piece_size: PieceSize<T>) -> PieceSize<T> {
        let piece_size = piece_size.as_value();
        debug_assert!(piece_size > 0, "piece_size: {} > 0", piece_size);
        for &n_sz in self.size_ary.iter().take(self.size_ary.len() - 1) {
            if piece_size <= n_sz {
                return PieceSize::<T>::new(n_sz);
            }
        }
        PieceSize::<T>::new(((piece_size + 128) / 128) * 128)
    }
    pub fn can_down<T>(&self, piece_size: PieceSize<T>, need_size: PieceSize<T>) -> bool {
        let piece_size = piece_size.as_value();
        let need_size = need_size.as_value();
        debug_assert!(piece_size > 0, "piece_size: {} > 0", piece_size);
        match self.size_ary[..(self.size_ary.len() - 1)].binary_search(&need_size) {
            Ok(k) => {
                let n_sz = self.size_ary[k];
                return n_sz < piece_size;
            }
            Err(k) => {
                if k < self.size_ary.len() - 1 {
                    let n_sz = self.size_ary[k];
                    return n_sz < piece_size;
                }
            }
        }
        false
    }
}

#[allow(dead_code)]
trait PieceSizeHelper<T> {
    fn is_large_piece_size(&self, pi_mgr: &PieceMgr) -> bool;
    fn roundup(&self, pi_mgr: &PieceMgr) -> PieceSize<T>;
    fn can_down(&self, pi_mgr: &PieceMgr, need: PieceSize<T>) -> bool;
}

impl<T: Copy> PieceSizeHelper<T> for PieceSize<T> {
    fn is_large_piece_size(&self, pi_mgr: &PieceMgr) -> bool {
        pi_mgr.is_large_piece_size(*self)
    }
    fn roundup(&self, pi_mgr: &PieceMgr) -> PieceSize<T> {
        pi_mgr.roundup(*self)
    }
    fn can_down(&self, pi_mgr: &PieceMgr, need: PieceSize<T>) -> bool {
        pi_mgr.can_down(*self, need)
    }
}

impl VarFile {
    pub fn read_free_piece_offset_on_header<T: Copy + PartialEq>(
        &mut self,
        piece_size: PieceSize<T>,
    ) -> Result<PieceOffset<T>> {
        let free_offset = self.piece_mgr.free_piece_list_offset_of_header(piece_size);
        self.seek_from_start(PieceOffset::<T>::new(free_offset))?;
        self.read_u64_le().map(PieceOffset::<T>::new)
    }

    pub fn write_free_piece_offset_on_header<T: Copy + PartialEq>(
        &mut self,
        piece_size: PieceSize<T>,
        offset: PieceOffset<T>,
    ) -> Result<()> {
        let free_offset = self.piece_mgr.free_piece_list_offset_of_header(piece_size);
        self.seek_from_start(PieceOffset::<T>::new(free_offset))?;
        self.write_u64_le(offset.into())
    }

    pub fn count_of_free_piece_list<T: Copy + PartialEq>(
        &mut self,
        new_piece_size: PieceSize<T>,
    ) -> Result<u64> {
        let mut count = 0;
        let free_1st = self.read_free_piece_offset_on_header(new_piece_size)?;
        if !free_1st.is_zero() {
            let mut free_next_offset = free_1st;
            while !free_next_offset.is_zero() {
                count += 1;
                let (_piece_size, free_next) = self.read_free_piece_size_next(free_next_offset)?;
                free_next_offset = free_next;
            }
        }
        Ok(count)
    }

    pub fn push_free_piece_list<T: Copy + PartialEq + PartialOrd>(
        &mut self,
        old_piece_offset: PieceOffset<T>,
        old_piece_size: PieceSize<T>,
    ) -> Result<()> {
        if old_piece_offset.is_zero() {
            return Ok(());
        }
        debug_assert!(!old_piece_size.is_zero());
        //
        let free_1st = self.read_free_piece_offset_on_header(old_piece_size)?;
        {
            let start_offset = self.seek_from_start(old_piece_offset)?;
            self.write_piece_size(old_piece_size)?;
            self.write_key_len(KeyLength::new(0))?;
            self.write_free_piece_offset(free_1st)?;
            self.write_zero_to_offset(start_offset + old_piece_size)?;
        }
        self.write_free_piece_offset_on_header(old_piece_size, old_piece_offset)?;
        Ok(())
    }

    pub fn pop_free_piece_list<T: Copy + PartialEq + PartialOrd>(
        &mut self,
        new_piece_size: PieceSize<T>,
    ) -> Result<PieceOffset<T>> {
        let free_1st = self.read_free_piece_offset_on_header(new_piece_size)?;
        if !new_piece_size.is_large_piece_size(&self.piece_mgr) {
            if !free_1st.is_zero() {
                let free_next = {
                    let (piece_size, free_next) = self.read_free_piece_size_next(free_1st)?;
                    self.write_piece_clear(free_1st, piece_size)?;
                    free_next
                };
                self.write_free_piece_offset_on_header(new_piece_size, free_next)?;
            }
            Ok(free_1st)
        } else {
            self.pop_free_piece_list_large(new_piece_size, free_1st)
        }
    }

    fn pop_free_piece_list_large<T: Copy + PartialEq + PartialOrd>(
        &mut self,
        new_piece_size: PieceSize<T>,
        free_1st: PieceOffset<T>,
    ) -> Result<PieceOffset<T>> {
        let mut free_prev = PieceOffset::<T>::new(0);
        let mut free_curr = free_1st;
        while !free_curr.is_zero() {
            self.seek_from_start(free_curr)?;
            let (free_next, piece_size) = {
                let piece_size = self.read_piece_size()?;
                let _key_len = self.read_key_len()?;
                debug_assert!(_key_len.is_zero());
                let piece_offset = self.read_free_piece_offset()?;
                (piece_offset, piece_size)
            };
            if new_piece_size <= piece_size {
                if !free_prev.is_zero() {
                    self.seek_from_start(free_prev)?;
                    let _piece_size: PieceSize<T> = self.read_piece_size()?;
                    let _key_len = self.read_key_len()?;
                    debug_assert!(_key_len.is_zero());
                    self.write_free_piece_offset(free_next)?;
                } else {
                    self.write_free_piece_offset_on_header(new_piece_size, free_next)?;
                }
                //
                self.write_piece_clear(free_curr, piece_size)?;
                return Ok(free_curr);
            }
            free_prev = free_curr;
            free_curr = free_next;
        }
        Ok(free_curr)
    }

    pub fn read_free_piece_size_next<T: Copy + PartialEq>(
        &mut self,
        curr_free_piece: PieceOffset<T>,
    ) -> Result<(PieceSize<T>, PieceOffset<T>)> {
        self.seek_from_start(curr_free_piece)?;
        let piece_size = self.read_piece_size()?;
        let _key_len = self.read_key_len()?;
        debug_assert!(_key_len.is_zero());
        let next_offset = self.read_free_piece_offset()?;
        Ok((piece_size, next_offset))
    }
}
