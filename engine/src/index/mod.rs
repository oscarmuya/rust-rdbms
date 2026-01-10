use std::collections::BTreeMap;

#[derive(Debug)]
pub struct PrimaryIndex {
    // Key: The value of the Primary Key column (as a String or custom Enum)
    // Value: (Page_Index, Slot_Index)
    pub map: BTreeMap<String, (usize, usize)>,
}

impl PrimaryIndex {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, page_idx: usize, slot_idx: usize) -> Result<(), String> {
        if self.map.contains_key(&key) {
            return Err(format!("Duplicate key violation: '{}' already exists", key));
        }
        self.map.insert(key, (page_idx, slot_idx));
        Ok(())
    }
}
