mod pager;
mod record;

use crate::catalog::schema::Schema;
use crate::index::PrimaryIndex;
use crate::storage::pager::{HEADER_SIZE, PAGE_SIZE, Page, Pager};
use crate::storage::record::{Field, Row};

pub struct Table {
    pub pager: Pager,
    pub schema: Schema,
    pub index: PrimaryIndex,
}

impl Table {
    pub fn insert_row(&mut self, row: Row) -> std::io::Result<()> {
        let serialized_row = row.serialize(&self.schema);
        let mut target_page_index = None;
        let mut target_slot_index = None;
        let mut page = Page::new();

        // 1. Find a page and a slot.
        // Iterate through existing pages.
        'page_loop: for p_idx in 0..self.pager.num_pages() {
            let p = self.pager.read_page(p_idx)?;
            let max_slots = (PAGE_SIZE - HEADER_SIZE) / self.schema.row_size();

            for slot_index in 0..max_slots {
                if !p.is_slot_full(slot_index) {
                    target_page_index = Some(p_idx);
                    target_slot_index = Some(slot_index);
                    page = p;
                    break 'page_loop;
                }
            }
        }

        // 2. If no empty slot found in existing pages, create a new page
        if target_page_index.is_none() {
            let new_idx = self.pager.num_pages();
            target_page_index = Some(new_idx);
            target_slot_index = Some(0);
            page = Page::new();
        }

        let p_idx = target_page_index.unwrap();
        let s_idx = target_slot_index.unwrap();

        page.set_slot(s_idx, true);
        let offset = page.get_row_offset(s_idx, self.schema.row_size());
        page.data[offset..offset + self.schema.row_size()].copy_from_slice(&serialized_row);

        self.pager.write_page(p_idx, &page).unwrap();

        Ok(())
    }

    pub fn scan_rows(&mut self) -> std::io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        let max_slots = (PAGE_SIZE - HEADER_SIZE) / self.schema.row_size();

        for p_idx in 0..self.pager.num_pages() {
            let page = self.pager.read_page(p_idx)?;

            for s_idx in 0..max_slots {
                if page.is_slot_full(s_idx) {
                    // 1. Calculate the offset for this slot
                    let offset = page.get_row_offset(s_idx, self.schema.row_size());

                    // 2. Extract the slice of bytes representing this row
                    let row_bytes = &page.data[offset..offset + self.schema.row_size()];

                    let row = Row::deserialize(row_bytes, &self.schema);
                    rows.push(row);
                }
            }
        }

        Ok(rows)
    }
    pub fn load_index(&mut self) -> std::io::Result<()> {
        let pk_col_idx = self.schema.columns.iter().position(|c| c.is_primary);

        if let Some(col_idx) = pk_col_idx {
            let max_slots = (PAGE_SIZE - HEADER_SIZE) / self.schema.row_size();

            for p_idx in 0..self.pager.num_pages() {
                let page = self.pager.read_page(p_idx)?;
                for s_idx in 0..max_slots {
                    if page.is_slot_full(s_idx) {
                        let offset = page.get_row_offset(s_idx, self.schema.row_size());
                        let row_bytes = &page.data[offset..offset + self.schema.row_size()];
                        let row = Row::deserialize(row_bytes, &self.schema);

                        // Convert the field value to a string to use as the index key
                        let pk_value = match &row.fields[col_idx] {
                            Field::Integer(v) => v.to_string(),
                            Field::Text(v) => v.clone(),
                            Field::Boolean(v) => v.to_string(),
                        };

                        let _ = self.index.insert(pk_value, p_idx, s_idx);
                    }
                }
            }
        }
        Ok(())
    }
}
