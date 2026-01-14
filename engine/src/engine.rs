use std::fs;

use crate::catalog::Catalog;
use crate::catalog::schema::DataType;
use crate::index::PrimaryIndex;
use crate::sql::{Command, QueryResponse, QueryResult};
use crate::storage::Table;
use crate::storage::pager::{HEADER_SIZE, PAGE_SIZE, Pager};
use crate::storage::record::{Field, Row};

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

    pub fn execute(&mut self, command: Command) -> Result<QueryResult, String> {
        match command {
            Command::CreateTable { name, columns } => {
                let schema = crate::catalog::schema::Schema {
                    table_name: name.clone(),
                    columns,
                };

                let table = self.catalog.tables.get(&name);

                if let Some(_) = table {
                    return Err(format!("Table {} already exists", &name));
                }

                self.catalog.add_table(schema);
                Ok(QueryResult::Message(
                    format!("Table {} created.", name).into(),
                ))
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
                    index: PrimaryIndex::new(),
                };

                // 3. Warm up index (So PK violation check works)
                table.load_index().map_err(|e| e.to_string())?;
                let prepared_row = self.validate_and_prepare_row(&table_name, row.fields)?;

                // 4. Perform insert
                table
                    .insert_row(prepared_row.clone())
                    .map_err(|e| e.to_string())?;
                Ok(QueryResult::Message(
                    format!("Inserted 1 row : {:?}", prepared_row).to_string(),
                ))
            }

            Command::DropTable { table_name } => {
                // 1. Remove from Catalog
                if self.catalog.tables.remove(&table_name).is_none() {
                    return Err(format!("Table {} not found", table_name));
                }
                self.catalog.sequences.remove(&table_name);
                self.catalog.save();

                // 2. Delete the physical file
                let path = format!("{}/{}.db", self.data_dir, table_name);
                if std::path::Path::new(&path).exists() {
                    std::fs::remove_file(path).map_err(|e| e.to_string())?;
                }

                Ok(QueryResult::Message(format!(
                    "Table {} dropped.",
                    table_name
                )))
            }

            Command::Delete { table_name, filter } => {
                let schema = self
                    .catalog
                    .tables
                    .get(&table_name)
                    .ok_or("Table not found")?;
                let path = format!("{}/{}.db", self.data_dir, table_name);
                let pager = Pager::open(&path).map_err(|e| e.to_string())?;
                let mut table = Table {
                    pager,
                    schema: schema.clone(),
                    index: crate::index::PrimaryIndex::new(),
                };
                table.load_index().map_err(|e| e.to_string())?;

                let mut deleted_count = 0;
                let mut targets = Vec::new();

                // TODO: Insert Index Optimization Logic
                for p_idx in 0..table.pager.num_pages() {
                    let page = table.pager.read_page(p_idx).map_err(|e| e.to_string())?;
                    for s_idx in 0..(PAGE_SIZE - HEADER_SIZE) / schema.row_size() {
                        if page.is_slot_full(s_idx) {
                            let row = table.get_row(p_idx, s_idx).map_err(|e| e.to_string())?;
                            if filter
                                .as_ref()
                                .map_or(true, |f| Row::row_matches_filter(&row, f, &schema))
                            {
                                targets.push((p_idx, s_idx, row));
                            }
                        }
                    }
                }

                // 2. Perform deletion
                for (p_idx, s_idx, row) in targets {
                    table.delete_row(p_idx, s_idx).map_err(|e| e.to_string())?;

                    // IMPORTANT: Remove from Index
                    if let Some(pk_idx) = schema.columns.iter().position(|c| c.is_primary) {
                        let pk_val = format!("{:?}", row.fields[pk_idx]);
                        table.index.map.remove(&pk_val);
                    }
                    deleted_count += 1;
                }

                Ok(QueryResult::Message(format!(
                    "Deleted {} rows.",
                    deleted_count
                )))
            }

            Command::Update {
                table_name,
                assignments,
                filter,
            } => {
                let schema = self
                    .catalog
                    .tables
                    .get(&table_name)
                    .ok_or("Table not found")?;
                let path = format!("{}/{}.db", self.data_dir, table_name);
                let pager = Pager::open(&path).map_err(|e| e.to_string())?;
                let mut table = Table {
                    pager,
                    schema: schema.clone(),
                    index: crate::index::PrimaryIndex::new(),
                };
                table.load_index().map_err(|e| e.to_string())?;

                let mut updated_count = 0;

                // 1: Find which rows to update
                let mut targets = Vec::new(); // Stores (page_idx, slot_idx, Row)

                // TODO: Add Index Optimization Logic here if filter is PK = Val]
                for p_idx in 0..table.pager.num_pages() {
                    let page = table.pager.read_page(p_idx).map_err(|e| e.to_string())?;
                    for s_idx in 0..(PAGE_SIZE - HEADER_SIZE) / schema.row_size() {
                        if page.is_slot_full(s_idx) {
                            let row = table.get_row(p_idx, s_idx).map_err(|e| e.to_string())?;
                            if filter
                                .as_ref()
                                .map_or(true, |f| Row::row_matches_filter(&row, f, &schema))
                            {
                                targets.push((p_idx, s_idx, row));
                            }
                        }
                    }
                }

                // 2: Apply Updates and Write Back
                for (p_idx, s_idx, mut row) in targets {
                    for (col_name, new_val) in &assignments {
                        let col_idx = schema
                            .columns
                            .iter()
                            .position(|c| &c.name == col_name)
                            .ok_or(format!("Column {} not found", col_name))?;

                        // Check if user is trying to update a Primary Key prevent this
                        if schema.columns[col_idx].is_primary {
                            return Err("Updating Primary Key is not allowed".to_string());
                        }

                        row.fields[col_idx] = new_val.clone();
                    }

                    table
                        .update_row(p_idx, s_idx, row)
                        .map_err(|e| e.to_string())?;
                    updated_count += 1;
                }

                Ok(QueryResult::Message(format!(
                    "Updated {} rows.",
                    updated_count
                )))
            }

            Command::Select {
                table_name,
                join,
                filter,
            } => {
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
                table.load_index().map_err(|e| e.to_string())?;

                let mut final_rows = Vec::new();
                let mut used_index = false;
                let mut merged_columns;

                // Check for optimization (fast path with index)
                if let (None, Some(f)) = (&join, &filter) {
                    let pk_col = schema.columns.iter().find(|c| c.is_primary);

                    if let Some(pk) = pk_col {
                        if f.column_name == pk.name
                            && matches!(f.operator, crate::sql::Operator::Eq)
                        {
                            // Convert filter value to string key for index lookup
                            let key = match &f.value {
                                Field::Integer(v) => v.to_string(),
                                Field::Text(v) => v.clone(),
                                Field::Boolean(v) => v.to_string(),
                            };

                            // Look up in B-Tree
                            if let Some((p_idx, s_idx)) = table.index.map.get(&key) {
                                let row =
                                    table.get_row(*p_idx, *s_idx).map_err(|e| e.to_string())?;
                                final_rows.push(row);
                            }
                            used_index = true;
                            merged_columns = schema.columns.clone();
                        } else {
                            merged_columns = schema.columns.clone();
                        }
                    } else {
                        merged_columns = schema.columns.clone();
                    }
                } else {
                    merged_columns = schema.columns.clone();
                }

                // Slow path (fallback if not optimized)
                if !used_index {
                    let mut rows = table.scan_rows().map_err(|e| e.to_string())?;

                    // Apply filter if present
                    if let Some(f) = filter {
                        rows.retain(|r| Row::row_matches_filter(r, &f, &schema));
                    }

                    // Handle join if present
                    if let Some(join_info) = join {
                        // Get right table schema and rows
                        let right_schema = self
                            .catalog
                            .tables
                            .get(&join_info.right_table)
                            .ok_or_else(|| format!("Table {} not found", join_info.right_table))?;
                        let right_path = format!("{}/{}.db", self.data_dir, join_info.right_table);
                        let right_pager = Pager::open(&right_path).map_err(|e| e.to_string())?;
                        let mut right_table = Table {
                            pager: right_pager,
                            schema: right_schema.clone(),
                            index: crate::index::PrimaryIndex::new(),
                        };
                        let right_rows = right_table.scan_rows().map_err(|e| e.to_string())?;

                        // Find column indexes
                        let left_col_idx = schema
                            .columns
                            .iter()
                            .position(|c| c.name == join_info.left_column)
                            .ok_or_else(|| {
                                format!(
                                    "Column {} not found in table {}",
                                    join_info.left_column, table_name
                                )
                            })?;

                        let right_col_idx = right_schema
                            .columns
                            .iter()
                            .position(|c| c.name == join_info.right_column)
                            .ok_or_else(|| {
                                format!(
                                    "Column {} not found in table {}",
                                    join_info.right_column, join_info.right_table
                                )
                            })?;

                        // Perform join
                        for row_a in &rows {
                            for row_b in &right_rows {
                                if row_a.fields[left_col_idx] == row_b.fields[right_col_idx] {
                                    // Merge rows
                                    let mut merged_fields = row_a.fields.clone();
                                    merged_fields.extend(row_b.fields.clone());
                                    final_rows.push(Row {
                                        fields: merged_fields,
                                    });
                                }
                            }
                        }

                        merged_columns = schema.columns.clone();
                        merged_columns.extend(right_schema.columns.clone());
                    } else {
                        final_rows = rows;
                        merged_columns = schema.columns.clone();
                    }
                }

                if final_rows.is_empty() {
                    return Ok(QueryResult::Message("No rows found.".to_string()));
                }

                if used_index {
                    println!("(Optimization used: Primary Key Index Lookup)");
                }

                Ok(QueryResult::Data(QueryResponse {
                    columns: merged_columns.iter().map(|c| c.name.clone()).collect(),
                    rows: final_rows.into_iter().map(|r| r.fields).collect(),
                }))
            }
        }
    }

    fn validate_and_prepare_row(
        &mut self,
        table_name: &str,
        provided_fields: Vec<Field>,
    ) -> Result<Row, String> {
        let schema = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let mut final_fields = provided_fields;

        // 1. Handle Autoincrement Logic
        if let Some(auto_idx) = schema.columns.iter().position(|c| c.is_autoincrement) {
            let next_id = self.catalog.get_next_id(table_name);
            let schema = self.catalog.tables.get(table_name).unwrap();

            let auto_field = Field::Integer(next_id);

            if final_fields.len() == schema.columns.len() {
                // Scenario: User provided an ID, but we override it
                final_fields[auto_idx] = auto_field;
            } else if final_fields.len() == schema.columns.len() - 1 {
                // Scenario: User omitted the ID, we insert it at the correct position
                final_fields.insert(auto_idx, auto_field);
            } else {
                return Err(format!(
                    "Column count mismatch: expected {} or {} (with autoincrement), found {}",
                    schema.columns.len(),
                    schema.columns.len() - 1,
                    final_fields.len()
                ));
            }
        }

        let schema = self.catalog.tables.get(table_name).unwrap();
        // 2. Final Length Check (for tables without autoincrement)
        if final_fields.len() != schema.columns.len() {
            return Err(format!(
                "Table {} expects {} columns, but {} were provided",
                table_name,
                schema.columns.len(),
                final_fields.len()
            ));
        }

        // 3. Type Validation
        for (i, column) in schema.columns.iter().enumerate() {
            let provided = &final_fields[i];

            let is_valid = match (&column.data_type, provided) {
                (DataType::Integer, Field::Integer(_)) => true,
                (DataType::Boolean, Field::Boolean(_)) => true,
                (DataType::Text(_), Field::Text(_)) => true,
                _ => false,
            };

            if !is_valid {
                return Err(format!(
                    "Type mismatch for column '{}': expected {:?}, found {:?}",
                    column.name, column.data_type, provided
                ));
            }
        }

        Ok(Row {
            fields: final_fields,
        })
    }
}
