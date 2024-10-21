import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from pathlib import Path
from datetime import datetime

app = FastAPI()

LOG_DIR = Path("timestamp_logs")


class LogEntry(BaseModel):
    timestamp: str
    data: Dict[str, Any]


def ensure_log_dir():
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_log_file(timestamp: str):
    return LOG_DIR / f"{timestamp}.json"


def read_log(timestamp: str):
    file_path = get_log_file(timestamp)
    try:
        return json.loads(file_path.read_text())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Log not found")


def write_log(timestamp: str, log_data: dict):
    file_path = get_log_file(timestamp)
    file_path.write_text(json.dumps(log_data, indent=2))


def generate_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S")


@app.get("/logs/{timestamp}")
async def get_log(timestamp: str):
    """タイムスタンプを指定して、ログ全体を取得"""
    return read_log(timestamp)


@app.post("/logs")
async def create_log(data: Dict[str, Any]):
    """
    新たなJSONファイルを作成し、そのタイムスタンプを返す
    初期データは引数から取得する
    """
    ensure_log_dir()
    timestamp = generate_timestamp()
    # log_data = {
    #     "id": timestamp,
    #     "data": data
    # }
    write_log(timestamp, data)
    return {"timestamp": timestamp}


@app.patch("/logs/{timestamp}")
async def update_log(timestamp: str, update_data: Dict[str, Any]):
    """タイムスタンプと更新項目と更新値を指定して、ログの内容を更新する"""
    log_data = read_log(timestamp)
    log_data["data"].update(update_data)
    write_log(timestamp, log_data)
    return log_data


@app.delete("/logs/{timestamp}")
async def delete_log(timestamp: str):
    """タイムスタンプを指定して、ログファイルを削除する"""
    file_path = get_log_file(timestamp)
    try:
        file_path.unlink()
        return {"message": "Log deleted successfully"}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Log not found")