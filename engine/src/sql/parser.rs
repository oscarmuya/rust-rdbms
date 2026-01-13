use crate::catalog::schema::{Column, DataType};
use crate::sql::{Command, JoinClause};
use crate::storage::record::{Field, Row};
use sqlparser::ast::{
    BinaryOperator, ColumnDef, DataType as SQLDataType, Expr, JoinConstraint, JoinOperator,
    SetExpr, Statement, TableFactor,
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
                if let SetExpr::Select(select) = *query.body {
                    // 1. Get the Primary (Left) Table
                    let first_from = select.from.first().ok_or("Missing FROM clause")?;
                    let left_table = match &first_from.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err("Unsupported table reference".to_string()),
                    };

                    // 2. Check for JOINs
                    let mut join_info = None;
                    if let Some(join) = first_from.joins.first() {
                        let right_table = match &join.relation {
                            TableFactor::Table { name, .. } => name.to_string(),
                            _ => return Err("Unsupported JOIN table".to_string()),
                        };

                        // 3. Extract the ON condition (e.g., tableA.id = tableB.user_id)
                        if let JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                            left,
                            op,
                            right,
                        })) = &join.join_operator
                        {
                            if let BinaryOperator::Eq = op {
                                // Extract column names from expressions like 'users.id'
                                let left_column = extract_column_name(left)?;
                                let right_column = extract_column_name(right)?;

                                join_info = Some(JoinClause {
                                    right_table,
                                    left_column,
                                    right_column,
                                });
                            }
                        }
                    }

                    commands.push(Command::Select {
                        table_name: left_table,
                        join: join_info,
                    });
                }
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

fn extract_column_name(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // We just take the last part (the column name)
            Ok(parts.last().unwrap().value.clone())
        }
        _ => Err(format!("Expected column name, found {:?}", expr)),
    }
}
