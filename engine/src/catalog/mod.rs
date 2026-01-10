pub mod schema;

use crate::catalog::schema::Schema;
use std::collections::HashMap;
use std::fs;

pub struct Catalog {
    pub tables: HashMap<String, Schema>,
    path: String,
}

impl Catalog {
    pub fn load_or_create(path: &str) -> Self {
        if let Ok(data) = fs::read_to_string(path) {
            let tables = serde_json::from_str(&data).unwrap_or_default();
            return Self {
                tables,
                path: path.to_string(),
            };
        }
        Self {
            tables: HashMap::new(),
            path: path.to_string(),
        }
    }

    pub fn add_table(&mut self, schema: Schema) {
        self.tables.insert(schema.table_name.clone(), schema);
        self.save();
    }

    fn save(&self) {
        let data = serde_json::to_string_pretty(&self.tables).unwrap();
        fs::write(&self.path, data).expect("Unable to save catalog");
    }
}
