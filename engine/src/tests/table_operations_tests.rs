#[cfg(test)]
mod tests {
    use crate::catalog::schema::{Column, DataType, Schema};
    use crate::index::PrimaryIndex;
    use crate::storage::Table;
    use crate::storage::pager::Pager;
    use crate::storage::record::{Field, Row};
    use std::fs;

    #[test]
    fn test_table_operations() {
        let file_path = "/tmp/test_table.db";
        // Clean up before test
        let _ = fs::remove_file(file_path);

        let schema = Schema {
            table_name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    is_primary: true,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Text(32),
                    is_primary: false,
                },
            ],
        };

        let pager = Pager::open(file_path).expect("Failed to open pager");
        let index = PrimaryIndex::new();

        let mut table = Table {
            pager,
            schema: schema.clone(),
            index,
        };

        let row1 = Row {
            fields: vec![Field::Integer(1), Field::Text("Alice".to_string())],
        };
        let row2 = Row {
            fields: vec![Field::Integer(2), Field::Text("Bob".to_string())],
        };

        table
            .insert_row(row1.clone())
            .expect("Failed to insert row 1");
        table
            .insert_row(row2.clone())
            .expect("Failed to insert row 2");

        let rows = table.scan_rows().expect("Failed to scan rows");
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&row1));
        assert!(rows.contains(&row2));

        // Test Duplicate Key
        let err = table.insert_row(row1.clone());
        assert!(err.is_err());

        // Test persistence (close and reopen)
        drop(table); // Drop table (and pager) to close file handle

        let pager = Pager::open(file_path).expect("Failed to reopen pager");
        let index = PrimaryIndex::new();

        let mut table = Table {
            pager,
            schema: schema.clone(),
            index,
        };
        table.load_index().expect("Failed to load index");

        let rows = table.scan_rows().expect("Failed to scan rows after reopen");
        assert_eq!(rows.len(), 2);

        // Verify index was loaded correctly by trying to insert duplicate again
        let err = table.insert_row(row1.clone());
        assert!(err.is_err());

        let _ = fs::remove_file(file_path);
    }
}
