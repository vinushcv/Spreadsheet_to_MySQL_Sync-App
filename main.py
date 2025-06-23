#!/usr/bin/env python3
"""
FastAPI Spreadsheet to MySQL Database Synchronization System
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse  # NEW ADDITION
import pandas as pd
import mysql.connector
from mysql.connector import Error
import hashlib
import os
import tempfile
from datetime import datetime
import logging
from typing import Dict
from pydantic import BaseModel

# ✅ Hardcoded MySQL credentials
class DatabaseConfig(BaseModel):
    host: str = "localhost"
    database: str = "fastapi_test"
    user: str = "root"
    password: str = "yaya2004"
    port: int = 3306

db_config = DatabaseConfig()

class SyncResponse(BaseModel):
    success: bool
    message: str
    stats: Dict[str, int]
    table_name: str
    timestamp: str

class SpreadsheetMySQLSync:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.connection = None
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sync_log.txt'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_database(self) -> bool:
        try:
            self.connection = mysql.connector.connect(
                host=self.db_config.host,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                port=self.db_config.port
            )
            if self.connection.is_connected():
                self.logger.info("Connected to MySQL")
                return True
        except Error as e:
            self.logger.error(f"MySQL Connection Error: {e}")
        return False
    
    def disconnect_database(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Disconnected from MySQL")
    
    def read_spreadsheet(self, file_path: str) -> pd.DataFrame:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format")
        
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
        return df

    def calculate_row_hash(self, row: pd.Series) -> str:
        row_str = ''.join(str(value) for value in row.values)
        return hashlib.md5(row_str.encode()).hexdigest()

    def create_table_from_df(self, df: pd.DataFrame, table_name: str, primary_key: str = "id"):
        cursor = self.connection.cursor()
        columns = []
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DECIMAL(10,2)"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "DATETIME"
            else:
                maxlen = df[col].astype(str).str.len().max()
                sql_type = f"VARCHAR({min(maxlen + 50, 500)})"
            if col == primary_key:
                columns.append(f"{col} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{col} {sql_type}")
        columns.append("data_hash VARCHAR(64)")
        columns.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        columns.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """
        cursor.execute(create_query)
        self.connection.commit()
        cursor.close()

    def get_existing_data(self, table_name: str, primary_key: str) -> Dict[str, str]:
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(f"SELECT {primary_key}, data_hash FROM {table_name}")
        data = {str(row[primary_key]): row["data_hash"] for row in cursor.fetchall()}
        cursor.close()
        return data

    def sync_data(self, df: pd.DataFrame, table_name: str, primary_key: str) -> Dict[str, int]:
        stats = {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0}
        cursor = self.connection.cursor()
        existing = self.get_existing_data(table_name, primary_key)
        new_ids = set()

        for _, row in df.iterrows():
            row_hash = self.calculate_row_hash(row)
            row_id = str(row[primary_key])
            new_ids.add(row_id)

            columns = list(row.index)
            values = [row[col] if pd.notna(row[col]) else None for col in columns]

            if row_id in existing:
                if existing[row_id] != row_hash:
                    set_clause = ', '.join([f"{col} = %s" for col in columns])
                    query = f"""
                        UPDATE {table_name} 
                        SET {set_clause}, data_hash = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE {primary_key} = %s
                    """
                    cursor.execute(query, values + [row_hash, row_id])
                    stats["updated"] += 1
                else:
                    stats["unchanged"] += 1
            else:
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)}, data_hash)
                    VALUES ({placeholders}, %s)
                """
                cursor.execute(query, values + [row_hash])
                stats["inserted"] += 1

        old_ids = set(existing.keys())
        for row_id in old_ids - new_ids:
            cursor.execute(f"DELETE FROM {table_name} WHERE {primary_key} = %s", (row_id,))
            stats["deleted"] += 1

        self.connection.commit()
        cursor.close()
        return stats

# FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files if you have them
app.mount("/static", StaticFiles(directory="static"), name="static")

# ✅ Serve the index.html on root URL
@app.get("/", response_class=HTMLResponse)  # NEW ADDITION
async def serve_index():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read(), status_code=200)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>index.html not found</h1>", status_code=404)

@app.post("/upload-excel", response_model=SyncResponse)
async def upload_excel(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    table_name: str = "excel_data",
    primary_key: str = "id",
    create_table: bool = True
):
    ext = os.path.splitext(file.filename)[1]
    if ext.lower() not in ['.csv', '.xlsx', '.xls']:
        raise HTTPException(status_code=400, detail="Unsupported file type")

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        sync = SpreadsheetMySQLSync(db_config)
        if not sync.connect_database():
            raise HTTPException(status_code=500, detail="Database connection failed")

        df = sync.read_spreadsheet(tmp_path)
        if df.empty:
            raise HTTPException(status_code=400, detail="Spreadsheet is empty")

        if create_table:
            sync.create_table_from_df(df, table_name, primary_key)

        stats = sync.sync_data(df, table_name, primary_key)
        sync.disconnect_database()

        return SyncResponse(
            success=True,
            message="Sync complete",
            stats=stats,
            table_name=table_name,
            timestamp=datetime.now().isoformat()
        )

    finally:
        os.unlink(tmp_path)

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# 🔻 ADD THESE TWO AT THE END 🔻

@app.post("/config/database")
async def configure_database(config: DatabaseConfig):
    global db_config
    db_config = config
    return {"message": "Database configuration updated successfully"}

@app.get("/config/database")
async def get_database_config():
    return db_config
@app.get("/tables")
def get_table_names():
    try:
        sync = SpreadsheetMySQLSync(db_config)
        if not sync.connect_database():
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = sync.connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        sync.disconnect_database()
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
