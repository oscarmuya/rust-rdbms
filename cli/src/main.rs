use cli_table::{Cell, Style, Table, print_stdout};
use engine::engine::Database;
use engine::sql::QueryResult;
use engine::sql::parser::parse_sql;

fn main() {
    let mut db = Database::open("./data");
    let mut rl = rustyline::DefaultEditor::new().unwrap();

    println!("Welcome to ScarDB '26. Type SQL commands or 'exit'.");

    loop {
        let readline = rl.readline("scar-db> ");
        match readline {
            Ok(line) => {
                if line == "exit" {
                    break;
                }

                // 1. Parse
                match parse_sql(&line) {
                    Ok(commands) => {
                        for cmd in commands {
                            // 2. Execute
                            match db.execute(cmd) {
                                Ok(msg) => print_result(msg),
                                Err(e) => println!("Error: {}", e),
                            }
                        }
                    }
                    Err(e) => println!("SQL Error: {}", e),
                }
            }
            Err(_) => break,
        }
    }
}

fn print_result(result: QueryResult) {
    match result {
        QueryResult::Message(msg) => println!("{}", msg),
        QueryResult::Data(resp) => {
            // Build table rows
            let table = resp
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|field| format!("{:?}", field).cell())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .table()
                .title(
                    resp.columns
                        .iter()
                        .map(|col| col.cell().bold(true))
                        .collect::<Vec<_>>(),
                );

            if let Err(e) = print_stdout(table) {
                eprintln!("Failed to print table: {}", e);
            }
        }
    }
}
