use crate::catalog::schema::{Column, DataType};
use crate::sql::Command;
use crate::storage::record::{Field, Row};
use sqlparser::ast::{
    ColumnDef, DataType as SQLDataType, Expr, SelectItem, SetExpr, Statement, TableFactor,
};
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
                let table = table_name.to_string();

                if let Some(source) = source {
                    if let SetExpr::Values(values) = source.body.as_ref() {
                        for row_values in &values.rows {
                            let mut fields = Vec::new();
                            for expr in row_values {
                                fields.push(convert_expr_to_field(expr)?);
                            }
                            commands.push(Command::Insert {
                                table_name: table.clone(),
                                row: Row { fields },
                            });
                        }
                    } else {
                        return Err("Unsupported INSERT format".to_string());
                    }
                } else {
                    return Err("INSERT statement missing values".to_string());
                }
            }
            Statement::Query(query) => {
                let (_, table) = match query.body.as_ref() {
                    SetExpr::Select(select) => {
                        let proj: Vec<String> = select
                            .projection
                            .iter()
                            .filter_map(|item| match item {
                                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                                    Some(ident.value.clone())
                                }
                                SelectItem::Wildcard(_) => Some("*".to_string()),
                                _ => None,
                            })
                            .collect();

                        let tbl = if let Some(table_with_joins) = select.from.first() {
                            if let TableFactor::Table { name, .. } = &table_with_joins.relation {
                                name.to_string()
                            } else {
                                return Err("Unsupported table reference".to_string());
                            }
                        } else {
                            return Err("Missing table in FROM clause".to_string());
                        };

                        (proj, tbl)
                    }
                    _ => return Err("Unsupported query format".to_string()),
                };

                commands.push(Command::Select { table_name: table });
            }
            _ => return Err("Unsupported SQL statement".to_string()),
        }
    }

    Ok(commands)
}

fn convert_column(col: ColumnDef) -> Result<Column, String> {
    let data_type = match col.data_type {
        SQLDataType::Int(_) | SQLDataType::Integer(_) => DataType::Integer,
        SQLDataType::Boolean => DataType::Boolean,
        SQLDataType::Varchar(Some(len)) => DataType::Text(len.to_string().parse().unwrap()),
        SQLDataType::Text => DataType::Text(255),
        _ => return Err(format!("Unsupported data type: {:?}", col.data_type)),
    };

    // Check if it's a primary key
    let is_primary = col.options.iter().any(|opt| {
        matches!(
            opt.option,
            sqlparser::ast::ColumnOption::Unique {
                is_primary: true,
                ..
            }
        )
    });

    Ok(Column {
        name: col.name.to_string(),
        data_type,
        is_primary,
    })
}

fn convert_expr_to_field(expr: &Expr) -> Result<Field, String> {
    match expr {
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
            if let Ok(i) = n.parse::<i32>() {
                Ok(Field::Integer(i))
            } else {
                Err(format!("Invalid integer: {}", n))
            }
        }
        Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => Ok(Field::Text(s.clone())),
        Expr::Value(sqlparser::ast::Value::Boolean(b)) => Ok(Field::Boolean(*b)),
        _ => Err(format!("Unsupported expression type: {:?}", expr)),
    }
}
