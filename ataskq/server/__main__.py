from ..db_handler import DBHandler
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import sqlite3

# Create a FastAPI app
app = FastAPI()

# Enable CORS (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Helper function to connect to the SQLite database


def get_db():
    conn = sqlite3.connect('test.db')
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        conn.close()

# API endpoint to create an item


@app.post("/items/")
def create_item(name: str, description: str, db: sqlite3.Connection = Depends(get_db)):
    cursor = db
    cursor.execute("INSERT INTO items (name, description) VALUES (?, ?)", (name, description))
    db.commit()
    return {"name": name, "description": description}

# API endpoint to get all items


@app.get("/items/")
def read_items(skip: int = 0, limit: int = 10, db: sqlite3.Connection = Depends(get_db)):
    cursor = db
    cursor.execute("SELECT id, name, description FROM items LIMIT ? OFFSET ?", (limit, skip))
    items = cursor.fetchall()
    return items
