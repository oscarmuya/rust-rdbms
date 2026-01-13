pub mod schema;

use serde::{Deserialize, Serialize};

use crate::catalog::schema::Schema;
use std::collections::HashMap;
use std::fs;

#[derive(Serialize, Deserialize, Default)]
pub struct CatalogData {
    pub tables: HashMap<String, Schema>,
    pub sequences: HashMap<String, i32>,
}

pub struct Catalog {
    pub tables: HashMap<String, Schema>,
    pub sequences: HashMap<String, i32>,
    path: String,
}

impl Catalog {
    pub fn load_or_create(path: &str) -> Self {
        if let Ok(data) = fs::read_to_string(path) {
            let data: CatalogData = serde_json::from_str(&data).unwrap_or_default();

            return Self {
                tables: data.tables,
                sequences: data.sequences,
                path: path.to_string(),
            };
        }
        Self {
            tables: HashMap::new(),
            path: path.to_string(),
            sequences: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, schema: Schema) {
        let name = schema.table_name.clone();
        self.tables.insert(name.clone(), schema);
        self.sequences.entry(name).or_insert(0);
        self.save();
    }

    pub fn get_next_id(&mut self, table_name: &str) -> i32 {
        let current_id = self.sequences.get(table_name).cloned().unwrap_or(0);
        let next_id = current_id + 1;
        self.sequences.insert(table_name.to_string(), next_id);
        self.save();
        next_id
    }

    pub fn save(&self) {
        let data_to_save = CatalogData {
            tables: self.tables.clone(),
            sequences: self.sequences.clone(),
        };
        let json =
            serde_json::to_string_pretty(&data_to_save).expect("Failed to serialize catalog");
        fs::write(&self.path, json).expect("Unable to save catalog file");
    }
}
