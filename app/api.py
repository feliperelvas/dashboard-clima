from __future__ import annotations
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from datetime import datetime, timezone
import os
import sqlite3
from contextlib import asynccontextmanager

from app.fetch_weather import fetch_by_city
from app.save_to_sqlite import parse_current, ensure_schema, insert_observation, DB_PATH
from app.read_from_sqlite import fetch_latest, fetch_range

app = FastAPI(
    title="Clima em Tempo Real",
    description="API local para coleta e leitura de condições meteorológicas",
    version="0.1.0",
)

def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    return conn

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    with get_conn() as conn:
        ensure_schema(conn)
    yield

@app.post("/collect")
def collect(city: str = Query(..., description="Nome da cidade"),
            country: str = Query("BR", description="Código do país (ISO-3166-1 alpha-2)")):
    """
    Busca na API (Weatherbit) e salva 1 observação no SQLite.
    Retorna se inseriu ou se era duplicata (idempotência).
    """
    payload = fetch_by_city(city=city, country=country, lang="pt", units="M")
    row = parse_current(payload)
    with get_conn() as conn:
        created = insert_observation(conn, row)
    ts = datetime.fromtimestamp(row["ts_utc"], tz=timezone.utc).isoformat()
    return {"city": f"{row['city_name']}-{row['country_code']}", "ts_utc": row["ts_utc"], "ts_iso_utc": ts, "inserted": created}

@app.get("/latest")
def latest(city: str, country: str = "BR"):
    """
    Retorna a última observação salva para a cidade/país.
    """
    rows = fetch_latest(city=city, country=country, limit=1)
    if not rows:
        raise HTTPException(404, detail="Sem dados para esta cidade.")
    (city_name, country_code, ts_utc, tz, temp_c, feels_like_c, humidity, weather_description) = rows[0]
    return {
        "city": city_name, "country": country_code, "ts_utc": ts_utc, "tz": tz,
        "temp_c": temp_c, "feels_like_c": feels_like_c,
        "humidity": humidity, "weather_description": weather_description
    }

@app.get("/weather")
def weather(city: str,
            country: str = "BR",
            start: Optional[int] = Query(None, description="Epoch UTC inicial (segundos)"),
            end: Optional[int] = Query(None, description="Epoch UTC final (segundos)")):
    """
    Retorna histórico de observações. Use epoch UTC (segundos) em start/end.
    Se não informar, retorna tudo da cidade.
    """
    rows = fetch_range(city=city, country=country, start_utc=start, end_utc=end)
    data = [
        {
            "city": r[0], "country": r[1], "ts_utc": r[2], "tz": r[3],
            "temp_c": r[4], "feels_like_c": r[5],
            "humidity": r[6], "weather_description": r[7],
        }
        for r in rows
    ]
    return {"count": len(data), "data": data}

@app.get("/health")
def health():
    return {"status": "ok"}
