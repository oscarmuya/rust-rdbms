use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Boolean,
    Text(usize), // The usize is the max_length
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub table_name: String,
    pub columns: Vec<Column>,
}

impl DataType {
    pub fn byte_size(&self) -> usize {
        match self {
            DataType::Integer => 4,
            DataType::Boolean => 1,
            DataType::Text(len) => *len,
        }
    }
}

impl Schema {
    pub fn row_size(&self) -> usize {
        let mut total_bytes = 0;

        for column in &self.columns {
            total_bytes += column.data_type.byte_size();
        }

        total_bytes
    }
}
