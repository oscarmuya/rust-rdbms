use crate::catalog::schema::{DataType, Schema};

#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    Integer(i32),
    Boolean(bool),
    Text(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    pub fields: Vec<Field>,
}

impl Row {
    pub fn serialize(&self, schema: &Schema) -> Vec<u8> {
        let mut bytes = Vec::new();

        for (i, column) in schema.columns.iter().enumerate() {
            let field = &self.fields[i];
            match &column.data_type {
                DataType::Integer => {
                    if let Field::Integer(val) = field {
                        bytes.extend_from_slice(&val.to_le_bytes());
                    }
                }
                DataType::Boolean => {
                    if let Field::Boolean(val) = field {
                        bytes.push(if *val { 1 } else { 0 });
                    }
                }
                DataType::Text(max_len) => {
                    if let Field::Text(val) = field {
                        let mut buf = vec![0u8; *max_len];

                        let string_bytes = val.as_bytes();
                        let len_to_copy = std::cmp::min(*max_len, string_bytes.len());
                        buf[..len_to_copy].copy_from_slice(&string_bytes[..len_to_copy]);

                        bytes.extend(buf);
                    }
                }
            }
        }

        bytes
    }

    pub fn deserialize(bytes: &[u8], schema: &Schema) -> Self {
        let mut fields = Vec::new();
        let mut cursor = 0;

        for column in &schema.columns {
            match column.data_type {
                DataType::Integer => {
                    let val = i32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
                    fields.push(Field::Integer(val));
                    cursor += 4;
                }
                DataType::Boolean => {
                    fields.push(Field::Boolean(bytes[cursor] != 0));
                    cursor += 1;
                }
                DataType::Text(max_len) => {
                    let string_bytes = &bytes[cursor..cursor + max_len];
                    let trimmed = string_bytes
                        .iter()
                        .take_while(|&&b| b != 0)
                        .copied()
                        .collect::<Vec<u8>>();
                    let string_value = String::from_utf8_lossy(&trimmed).to_string();

                    fields.push(Field::Text(string_value));
                    cursor += max_len;
                }
            }
        }

        Row { fields }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::{Column, DataType, Schema};

    #[test]
    fn test_row_serialization() {
        let schema = Schema {
            table_name: "test".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    is_primary: true,
                },
                Column {
                    name: "active".to_string(),
                    data_type: DataType::Boolean,
                    is_primary: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Text(20),
                    is_primary: false,
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
