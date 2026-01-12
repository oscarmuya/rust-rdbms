use crate::catalog::schema::{Column, DataType};
use crate::sql::Command;
use crate::storage::record::{Field, Row};
use sqlparser::ast::{ColumnDef, DataType as SQLDataType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql(sql: &str) -> Result<Vec<Command>, String> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

    let mut commands = Vec::new();

    for statement in ast {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let table_name = name.to_string();
                let mut my_columns = Vec::new();

                for col in columns {
                    my_columns.push(convert_column(col)?);
                }

                commands.push(Command::CreateTable {
                    name: table_name,
                    columns: my_columns,
                });
            }
            Statement::Insert {
                table_name, source, ..
            } => {
                // TODO: Translate SQL Insert into our Command::Insert
                let fields = Vec::new();

                let row = Row { fields: fields };
                commands.push(Command::Insert {
                    table_name: table_name.to_string(),
                    row: row,
                });
            }

            Statement::Query(query) => {
                // TODO: Translate SQL Select into our Command::Select
                let table_name = query.name;

                commands.push(Command::Select {
                    table_name: table_name,
                });
            }
            _ => return Err("Unsupported SQL statement".to_string()),
        }
    }

    Ok(commands)
}

fn convert_row(row: RowDef) -> Result<Row, String> {}

fn convert_column(col: ColumnDef) -> Result<Column, String> {
    let data_type = match col.data_type {
        SQLDataType::Int(_) | SQLDataType::Integer(_) => DataType::Integer,
        SQLDataType::Boolean => DataType::Boolean,
        SQLDataType::Varchar(Some(len)) => DataType::Text(len.to_string().parse().unwrap()),
        _ => return Err(format!("Unsupported data type: {:?}", col.data_type)),
    };

    // Check if it's a primary key
    let is_primary = col.options.iter().any(|opt| {
        matches!(
            opt.option,
            sqlparser::ast::ColumnOption::Unique { is_primary: true }
        )
    });

    Ok(Column {
        name: col.name.to_string(),
        data_type,
        is_primary,
    })
}
