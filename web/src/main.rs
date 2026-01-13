use axum::{routing::{get, post}, Json, Router};
use engine::engine::Database;
use engine::sql::parser::parse_sql;
use std::sync::{Arc, Mutex};

// Shared state so all web requests use the same DB instance
struct AppState {
    db: Mutex<Database>,
}

#[tokio::main]
async fn main() {
    let db = Database::open("./data");
    let shared_state = Arc::new(AppState { db: Mutex::new(db) });

    let app = Router::new()
        .route("/query", post(handle_query))
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    println!("Web Demo running on http://127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn handle_query(
    state: axum::extract::State<Arc<AppState>>,
    Json(payload): Json<String>, // User sends raw SQL string
) -> String {
    let mut db = state.db.lock().unwrap();

    match parse_sql(&payload) {
        Ok(commands) => {
            let mut results = String::new();
            for cmd in commands {
                match db.execute(cmd) {
                    Ok(res) => results.push_str(&res),
                    Err(e) => results.push_str(&format!("Error: {}", e)),
                }
            }
            results
        }
        Err(e) => format!("SQL Error: {}", e),
    }
}
