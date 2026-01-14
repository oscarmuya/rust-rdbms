use serde::Serialize;

use crate::{
    catalog::schema::{DataType, Schema},
    sql::{Filter, Operator},
};

#[derive(Debug, Clone, PartialEq, Serialize)]
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

    pub fn row_matches_filter(row: &Row, filter: &Filter, schema: &Schema) -> bool {
        // 1. Find the index of the column being filtered
        let col_idx = match schema
            .columns
            .iter()
            .position(|c| c.name == filter.column_name)
        {
            Some(idx) => idx,
            None => return false,
        };

        let actual_value = &row.fields[col_idx];

        // 2. Compare actual_value vs filter.value based on the operator
        match filter.operator {
            Operator::Eq => actual_value == &filter.value,
            Operator::NotEq => actual_value != &filter.value,
            // For GreaterThan/LessThan, we will handle only Integers
            Operator::GreaterThan => {
                if let (Field::Integer(a), Field::Integer(b)) = (actual_value, &filter.value) {
                    a > b
                } else {
                    false
                }
            }
            Operator::LessThan => {
                if let (Field::Integer(a), Field::Integer(b)) = (actual_value, &filter.value) {
                    a < b
                } else {
                    false
                }
            }
        }
    }
}
