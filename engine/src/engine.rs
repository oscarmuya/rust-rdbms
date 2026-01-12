use std::fs;

use crate::catalog::Catalog;
use crate::sql::Command;
use crate::storage::Table;
use crate::storage::pager::Pager;
use crate::storage::record::Row;

pub struct Database {
    pub catalog: Catalog,
    pub data_dir: String,
}

impl Database {
    pub fn open(data_dir: &str) -> Self {
        let catalog_path = format!("{}/catalog.json", data_dir);

        if let Err(e) = fs::create_dir_all(data_dir) {
            panic!("Failed to create data directory {}: {}", data_dir, e);
        }

        Self {
            catalog: Catalog::load_or_create(&catalog_path),
            data_dir: data_dir.to_string(),
        }
    }

    pub fn execute(&mut self, command: Command) -> Result<String, String> {
        match command {
            Command::CreateTable { name, columns } => {
                let schema = crate::catalog::schema::Schema {
                    table_name: name.clone(),
                    columns,
                };
                self.catalog.add_table(schema);
                Ok(format!("Table {} created.", name))
            }

            Command::Insert { table_name, row } => {
                // 1. Get schema from catalog
                let schema = self
                    .catalog
                    .tables
                    .get(&table_name)
                    .ok_or_else(|| format!("Table {} not found", table_name))?;

                // 2. Open the table
                let path = format!("{}/{}.db", self.data_dir, table_name);
                let pager = Pager::open(&path).map_err(|e| e.to_string())?;
                let mut table = Table {
                    pager,
                    schema: schema.clone(),
                    index: crate::index::PrimaryIndex::new(),
                };

                // 3. Warm up index (So PK violation check works)
                table.load_index().map_err(|e| e.to_string())?;

                // 4. Perform insert
                table.insert_row(row).map_err(|e| e.to_string())?;
                Ok("Inserted 1 row.".to_string())
            }

            Command::Select { table_name } => {
                let schema = self
                    .catalog
                    .tables
                    .get(&table_name)
                    .ok_or_else(|| format!("Table {} not found", table_name))?;
                let path = format!("{}/{}.db", self.data_dir, table_name);
                let pager = Pager::open(&path).map_err(|e| e.to_string())?;
                let mut table = Table {
                    pager,
                    schema: schema.clone(),
                    index: crate::index::PrimaryIndex::new(),
                };
                let rows = table.scan_rows().map_err(|e| e.to_string())?;

                if rows.is_empty() {
                    return Ok("No rows found.".to_string());
                }

                use cli_table::{Cell, Style, Table as CliTable, print_stdout};

                // Build table data
                let mut table_data: Vec<Vec<_>> = Vec::new();

                // Add header row
                let headers: Vec<_> = schema
                    .columns
                    .iter()
                    .map(|c| c.name.clone().cell().bold(true))
                    .collect();
                table_data.push(headers);

                // Add data rows
                for row in rows {
                    let values: Vec<_> = row
                        .fields
                        .iter()
                        .map(|v| format!("{:?}", v).cell())
                        .collect();
                    table_data.push(values);
                }

                let cli_table = table_data.table();

                print_stdout(cli_table).map_err(|e| e.to_string())?;

                Ok(String::new())
            }
        }
    }
}
