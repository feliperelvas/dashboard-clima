from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path("./data/weather.db")

def get_conn() -> sqlite3.Connection:
    """Abre conexão com o SQLite (crash se o arquivo não existir)."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco não encontrado em {DB_PATH}. Rode primeiro o save_to_sqlite.")
    return sqlite3.connect(DB_PATH)

def fetch_latest(city: str, country: str = "BR", limit: int = 10) -> list[tuple]:
    """
    Retorna as últimas N observações para cidade/país.
    """
    sql = """
    SELECT city_name, country_code, ts_utc, tz, temp_c, feels_like_c, humidity, weather_description
    FROM weather_observations
    WHERE city_name = ? AND country_code = ?
    ORDER BY ts_utc DESC
    LIMIT ?
    """
    with get_conn() as conn:
        cur = conn.execute(sql, (city, country, limit))
        return cur.fetchall()

def fetch_range(city: str, country: str = "BR",
                start_utc: Optional[int] = None, end_utc: Optional[int] = None) -> list[tuple]:
    """
    Retorna observações dentro de um intervalo de epoch UTC (start_utc <= ts_utc <= end_utc).
    Se start_utc/end_utc forem None, o filtro correspondente é ignorado.
    """
    base = """
    SELECT city_name, country_code, ts_utc, tz, temp_c, feels_like_c, humidity, weather_description
    FROM weather_observations
    WHERE city_name = ? AND country_code = ?
    """
    params: list = [city, country]
    if start_utc is not None:
        base += " AND ts_utc >= ?"
        params.append(int(start_utc))
    if end_utc is not None:
        base += " AND ts_utc <= ?"
        params.append(int(end_utc))
    base += " ORDER BY ts_utc ASC"

    with get_conn() as conn:
        cur = conn.execute(base, tuple(params))
        return cur.fetchall()

def rows_to_df(rows):
    """Helper para DataFrame. Será utilizado para a plotagem dos gráficos."""
    cols = ["city_name","country_code","ts_utc","tz","temp_c","feels_like_c","humidity","weather_description"]
    return pd.DataFrame(rows, columns=cols)
