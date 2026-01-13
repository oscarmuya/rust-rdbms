pub mod parser;

use crate::catalog::schema::Column;
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
        filter: Option<Filter>,
        join: Option<JoinClause>,
    },
    Update {
        table_name: String,
        // Column name and the new value
        assignments: Vec<(String, Field)>,
        filter: Option<Filter>,
    },
    Delete {
        table_name: String,
        filter: Option<Filter>,
    },
    DropTable {
        table_name: String,
    },
}

#[derive(Debug)]
pub enum Operator {
    Eq,
    NotEq,
    GreaterThan,
    LessThan,
}

#[derive(Debug)]
pub struct Filter {
    pub column_name: String,
    pub operator: Operator,
    pub value: Field,
}

#[derive(Debug)]
pub struct JoinClause {
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
}
