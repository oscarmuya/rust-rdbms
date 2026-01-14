use cli_table::{Cell, Color, Style, Table, print_stdout};
use colored::Colorize;
use engine::engine::Database;
use engine::sql::QueryResult;
use engine::sql::parser::parse_sql;

fn main() {
    let mut db = Database::open("./data");
    let mut rl = rustyline::DefaultEditor::new().unwrap();

    println!(
        "{}",
        "Welcome to ScarDB '26. Type SQL commands or 'exit'."
            .yellow()
            .bold()
    );

    loop {
        let readline = rl.readline(&"scar-db> ".yellow().to_string());
        match readline {
            Ok(line) => {
                if line == "exit" {
                    println!("{}", "Goodbye!".yellow());
                    break;
                }
                match parse_sql(&line) {
                    Ok(commands) => {
                        for cmd in commands {
                            match db.execute(cmd) {
                                Ok(msg) => print_result(msg),
                                Err(e) => println!("{} {}", "Error:".red().bold(), e),
                            }
                        }
                    }
                    Err(e) => println!("{} {}", "SQL Error:".red().bold(), e),
                }
            }
            Err(_) => break,
        }
    }
}

fn print_result(result: QueryResult) {
    match result {
        QueryResult::Message(msg) => {
            // Color success messages green
            if msg.contains("success") || msg.contains("created") || msg.contains("inserted") {
                println!("{}", msg.green());
            } else {
                println!("{}", msg.cyan());
            }
        }
        QueryResult::Data(resp) => {
            let table = resp
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|field| {
                            format!("{:?}", field)
                                .cell()
                                .foreground_color(Some(Color::Cyan))
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .table()
                .title(
                    resp.columns
                        .iter()
                        .map(|col| col.cell().bold(true).foreground_color(Some(Color::Green)))
                        .collect::<Vec<_>>(),
                );
            if let Err(e) = print_stdout(table) {
                eprintln!("{}", format!("Failed to print table: {}", e).red());
            }
        }
    }
}
