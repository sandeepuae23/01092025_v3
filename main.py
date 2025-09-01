from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict

from db import init_db, get_connection

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialise database on startup
init_db()

@app.get("/")
async def index() -> FileResponse:
    """Serve the React application."""
    return FileResponse("templates/index.html")

@app.get("/api/customers")
async def list_customers(page: int = 1, size: int = 10) -> Dict[str, object]:
    """Return paginated customers from SQLite."""
    offset = (page - 1) * size
    conn = get_connection()
    cursor = conn.execute(
        "SELECT customer_id, first_name, last_name, email FROM customers LIMIT ? OFFSET ?",
        (size, offset),
    )
    rows = cursor.fetchall()
    total = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    conn.close()
    data = [
        {
            "customer_id": r[0],
            "first_name": r[1],
            "last_name": r[2],
            "email": r[3],
        }
        for r in rows
    ]
    return {"data": data, "total": total, "page": page, "size": size}

