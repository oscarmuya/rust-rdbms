use engine::engine::Database;
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
                                Ok(msg) => println!("{}", msg),
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
