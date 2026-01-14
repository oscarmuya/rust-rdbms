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
            max-width: 900px;
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
        .result {
            margin-top: 2rem;
            padding: 1rem;
            background: white;
            border: 1px solid #e4e4e4;
            border-radius: 6px;
            white-space: pre-wrap;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            display: none;
            max-height: 200px;
            overflow: auto;
        }
        .result.show {
            display: block;
        }

        /* Table Styles */
        .table-container {
            margin-top: 1.5rem;
            overflow-x: auto;
            border-radius: 6px;
            border: 1px solid #e4e4e4;
            background: white;
            display: none;
        }
        .table-container.show {
            display: block;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
            text-align: left;
        }
        th {
            background: #f4f4f4;
            font-weight: 600;
            padding: 12px;
            border-bottom: 2px solid #e4e4e4;
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #eee;
        }
        tr:last-child td {
            border-bottom: none;
        }
        tr:hover {
            background: #f9f9f9;
        }
        .type-label {
            font-size: 10px;
            color: #888;
            margin-left: 5px;
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

        <div id="table-container" class="table-container">
            <table id="data-table">
                <thead id="table-head"></thead>
                <tbody id="table-body"></tbody>
            </table>
        </div>
    </div>

    <script>
        async function executeQuery() {
            const sql = document.getElementById('sql-input').value;
            const resultDiv = document.getElementById('result');
            const tableContainer = document.getElementById('table-container');
            const tableHead = document.getElementById('table-head');
            const tableBody = document.getElementById('table-body');

            if (!sql.trim()) {
                resultDiv.textContent = 'Please enter a SQL command';
                resultDiv.classList.add('show');
                tableContainer.classList.remove('show');
                return;
            }

            try {
                const response = await fetch('/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(sql)
                });

                const rawText = await response.text();
                resultDiv.textContent = rawText;
                resultDiv.classList.add('show');

                const data = JSON.parse(rawText);
                renderTable(data);

            } catch (error) {
                resultDiv.textContent = 'Error: ' + error.message;
                resultDiv.classList.add('show');
                tableContainer.classList.remove('show');
            }
        }

        function renderTable(data) {
            const tableContainer = document.getElementById('table-container');
            const tableHead = document.getElementById('table-head');
            const tableBody = document.getElementById('table-body');

            // Clear previous results
            tableHead.innerHTML = '';
            tableBody.innerHTML = '';
            tableContainer.classList.remove('show');

            if (!Array.isArray(data) || data.length === 0) return;

            const firstResult = data[0];

            if (firstResult.Data) {
                const cols = firstResult.Data.columns;
                const rows = firstResult.Data.rows;

                // Create Headers
                const headerRow = document.createElement('tr');
                cols.forEach(colName => {
                    const th = document.createElement('th');
                    th.textContent = colName;
                    headerRow.appendChild(th);
                });
                tableHead.appendChild(headerRow);

                // Create Rows
                rows.forEach(row => {
                    const tr = document.createElement('tr');
                    row.forEach(cell => {
                        const td = document.createElement('td');
                        // Extract value from format: {"Integer": 1} or {"Text": "oscar"}
                        const valueKey = Object.keys(cell)[0];
                        const value = cell[valueKey];

                        td.innerHTML = `${value}<span class="type-label">(${valueKey})</span>`;
                        tr.appendChild(td);
                    });
                    tableBody.appendChild(tr);
                });

                tableContainer.classList.add('show');
            } else if (firstResult.Message) {
                // If it's just a message, we only show it in the pre tag (already done)
                tableContainer.classList.remove('show');
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
