pub const PAGE_SIZE: usize = 4096;
pub const HEADER_SIZE: usize = 64;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub struct Pager {
    file: File,
    pub file_length: u64,
}

pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }

    pub fn get_row_offset(&self, slot_index: usize, row_size: usize) -> usize {
        HEADER_SIZE + (slot_index * row_size)
    }

    pub fn is_slot_full(&self, slot_index: usize) -> bool {
        let byte_idx = slot_index / 8;
        let bit_idx = slot_index % 8;

        self.data[byte_idx] & (1 << bit_idx) != 0
    }

    pub fn set_slot(&mut self, slot_index: usize, occupied: bool) {
        let byte_idx = slot_index / 8;
        let bit_idx = slot_index % 8;

        if occupied {
            self.data[byte_idx] |= 1 << bit_idx;
        } else {
            self.data[byte_idx] &= !(1 << bit_idx);
        }
    }
}

impl Pager {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_length = file.metadata()?.len();
        Ok(Self { file, file_length })
    }

    pub fn read_page(&mut self, page_index: usize) -> std::io::Result<Page> {
        let mut page = Page::new();
        let offset = page_index as u64 * PAGE_SIZE as u64;

        // Jump to the right spot in the file
        self.file.seek(SeekFrom::Start(offset))?;

        // Try to read exactly 4096 bytes.
        let _ = self.file.read(&mut page.data)?;

        Ok(page)
    }

    pub fn write_page(&mut self, page_index: usize, page: &Page) -> std::io::Result<()> {
        let offset = page_index as u64 * PAGE_SIZE as u64;

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.sync_all()?;

        // Update our knowledge of the file length
        self.file_length = self.file.metadata()?.len();
        Ok(())
    }

    pub fn num_pages(&self) -> usize {
        (self.file_length as usize) / PAGE_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_page_new() {
        let page = Page::new();
        assert_eq!(page.data.len(), PAGE_SIZE);
        for b in page.data.iter() {
            assert_eq!(*b, 0);
        }
    }

    #[test]
    fn test_page_slot_management() {
        let mut page = Page::new();
        assert!(!page.is_slot_full(0));
        assert!(!page.is_slot_full(1));

        page.set_slot(0, true);
        assert!(page.is_slot_full(0));
        assert!(!page.is_slot_full(1));

        page.set_slot(1, true);
        assert!(page.is_slot_full(0));
        assert!(page.is_slot_full(1));

        page.set_slot(0, false);
        assert!(!page.is_slot_full(0));
        assert!(page.is_slot_full(1));
    }

    #[test]
    fn test_get_row_offset() {
        let page = Page::new();
        let row_size = 100;
        assert_eq!(page.get_row_offset(0, row_size), HEADER_SIZE);
        assert_eq!(page.get_row_offset(1, row_size), HEADER_SIZE + 100);
    }

    #[test]
    fn test_pager_open_and_io() {
        let file_path = "/tmp/test_pager.db";
        let _ = fs::remove_file(file_path);

        let mut pager = Pager::open(file_path).expect("Failed to open pager");
        assert_eq!(pager.num_pages(), 0);

        let mut page = Page::new();
        page.data[0] = 55;
        page.set_slot(0, true);

        pager.write_page(0, &page).expect("Failed to write page");
        assert_eq!(pager.num_pages(), 1);

        let read_page = pager.read_page(0).expect("Failed to read page");
        assert_eq!(read_page.data[0], 55);
        assert!(read_page.is_slot_full(0));

        let _ = fs::remove_file(file_path);
    }
}
