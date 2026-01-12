pub mod parser;

use crate::catalog::schema::{Column, DataType, Schema};
use crate::storage::record::{Field, Row};

#[derive(Debug)]
pub enum Command {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        row: Row,
    },
    Select {
        table_name: String,
        // Later we will add:
        // filters: Vec<Expression>,
        // join: Option<JoinClause>
    },
}
