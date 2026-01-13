#[cfg(test)]
mod tests {
    use crate::{
        catalog::schema::{Column, DataType, Schema},
        storage::record::{Field, Row},
    };

    #[test]
    fn test_row_serialization() {
        let schema = Schema {
            table_name: "test".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    is_primary: true,
                    is_autoincrement: true,
                },
                Column {
                    name: "active".to_string(),
                    data_type: DataType::Boolean,
                    is_primary: false,
                    is_autoincrement: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Text(20),
                    is_primary: false,
                    is_autoincrement: false,
                },
            ],
        };

        let row = Row {
            fields: vec![
                Field::Integer(123),
                Field::Boolean(true),
                Field::Text("Hello".to_string()),
            ],
        };

        let bytes = row.serialize(&schema);
        let deserialized = Row::deserialize(&bytes, &schema);

        assert_eq!(row, deserialized);
    }
}
