#[cfg(test)]
mod tests {
    use crate::storage::pager::{HEADER_SIZE, PAGE_SIZE, Page, Pager};
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
