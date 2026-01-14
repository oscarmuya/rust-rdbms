use axum::{
    Json, Router,
    response::Html,
    routing::{get, post},
};
use engine::sql::parser::parse_sql;
use engine::{engine::Database, sql::QueryResult};
use std::sync::{Arc, Mutex};

struct AppState {
    db: Mutex<Database>,
}

#[tokio::main]
async fn main() {
    let db = Database::open("./data");
    let shared_state = Arc::new(AppState { db: Mutex::new(db) });
    let app = Router::new()
        .route("/", get(serve_html))
        .route("/query", post(handle_query))
        .with_state(shared_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Web Demo running on http://127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn serve_html() -> Html<&'static str> {
    Html(
        r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ScarDB SQL Query Interface</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: system-ui, -apple-system, sans-serif;
            padding: 2rem;
            background: #fafafa;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
        }
        h1 {
            margin-bottom: 2rem;
            color: #1a1a1a;
        }
        .input-group {
            margin-bottom: 1rem;
        }
        label {
            display: block;
            margin-bottom: 0.5rem;
            font-weight: 500;
            color: #4a4a4a;
        }
        textarea {
            width: 100%;
            padding: 0.75rem;
            border: 1px solid #d4d4d4;
            border-radius: 6px;
            font-family: 'Courier New', monospace;
            font-size: 14px;
            resize: vertical;
            min-height: 120px;
        }
        textarea:focus {
            outline: none;
            border-color: #8b8b8b;
        }
        button {
            padding: 0.75rem 1.5rem;
            background: #1a1a1a;
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
        }
        button:hover {
            background: #2a2a2a;
        }
        button:active {
            background: #0a0a0a;
        }
        .result {
            margin-top: 2rem;
            padding: 1rem;
            background: white;
            border: 1px solid #e4e4e4;
            border-radius: 6px;
            white-space: pre-wrap;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            display: none;
        }
        .result.show {
            display: block;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>ScarDB SQL Query Interface</h1>
        <div class="input-group">
            <label for="sql-input">SQL Command</label>
            <textarea id="sql-input" placeholder="Enter your SQL command here..."></textarea>
        </div>
        <button onclick="executeQuery()">Execute</button>
        <pre id="result" class="result"></pre>
    </div>
    <script>
        async function executeQuery() {
            const sql = document.getElementById('sql-input').value;
            const resultDiv = document.getElementById('result');

            if (!sql.trim()) {
                resultDiv.textContent = 'Please enter a SQL command';
                resultDiv.classList.add('show');
                return;
            }

            try {
                const response = await fetch('/query', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(sql)
                });

                const result = await response.text();
                resultDiv.textContent = result;
                resultDiv.classList.add('show');
            } catch (error) {
                resultDiv.textContent = 'Error: ' + error.message;
                resultDiv.classList.add('show');
            }
        }

        document.getElementById('sql-input').addEventListener('keydown', function(e) {
            if (e.ctrlKey && e.key === 'Enter') {
                executeQuery();
            }
        });
    </script>
</body>
</html>
    "#,
    )
}

async fn handle_query(
    state: axum::extract::State<Arc<AppState>>,
    Json(payload): Json<String>,
) -> Json<Vec<QueryResult>> {
    let mut db = state.db.lock().unwrap();
    match parse_sql(&payload) {
        Ok(commands) => {
            let mut results = Vec::new();
            for cmd in commands {
                match db.execute(cmd) {
                    Ok(res) => results.push(res),
                    Err(e) => results.push(QueryResult::Message(format!("Error: {}", e))),
                }
            }
            Json(results)
        }
        Err(e) => Json(vec![QueryResult::Message(format!("SQL Error: {}", e))]),
    }
}
