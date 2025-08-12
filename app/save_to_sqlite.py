from __future__ import annotations
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Importa a função que vai consultar a API e coletar os dados
from app.fetch_weather import fetch_by_city

DB_PATH = Path("./data/weather.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

"""
Cria a tabela se ainda não existir.

    - id é a chave primária (IDENTIFICADOR único da linha).

    - Campos meteorológicos: city, país, lat/lon, tempo, vento, etc.

    - ts_utc: inteiro com o Unix timestamp (em UTC) da observação.

    - tz: timezone IANA (ex.: America/Sao_Paulo) — útil para converter ao exibir.

    - created_at: preenchido automaticamente com a hora da inserção (no banco).

    - UNIQUE(city_name, country_code, ts_utc): evita duplicatas para a mesma cidade no mesmo instante → garante idempotência (se tentar inserir de novo a mesma observação, o banco recusa).

Idempotência aqui = rodar a coleta 2x para o mesmo instante não gera 2 linhas; o estado final (1 linha) é o mesmo.
"""

DDL = """
CREATE TABLE IF NOT EXISTS weather_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    lat REAL,
    lon REAL,
    ts_utc INTEGER NOT NULL,
    tz TEXT,
    temp_c REAL,
    feels_like_c REAL,
    humidity INTEGER,
    pressure REAL,
    wind_speed REAL,
    wind_dir INTEGER,
    clouds INTEGER,
    visibility_km REAL,
    weather_description TEXT,
    created_at TEXT DEFAULT (CURRENT_TIMESTAMP),
    UNIQUE(city_name, country_code, ts_utc)
);
"""

def parse_current(payload: dict) -> dict:
    """Extrai e normaliza campos relevantes do JSON da Weatherbit."""
    d = (payload.get("data") or [{}])[0]
    ts_utc = int(d["ts"])  # epoch UTC
    tz_name = d.get("timezone", "UTC")

    return {
        "city_name": d.get("city_name"),
        "country_code": d.get("country_code"),
        "lat": d.get("lat"),
        "lon": d.get("lon"),
        "ts_utc": ts_utc,
        "tz": tz_name,
        "temp_c": d.get("temp"),
        "feels_like_c": d.get("app_temp"),
        "humidity": d.get("rh"),
        "pressure": d.get("pres"),
        "wind_speed": d.get("wind_spd"),
        "wind_dir": d.get("wind_dir"),
        "clouds": d.get("clouds"),
        "visibility_km": d.get("vis"),
        "weather_description": d.get("weather", {}).get("description"),
    }

def ensure_schema(conn: sqlite3.Connection) -> None:
    """Executa a DDL e confirma (commit) — idempotente: se a tabela já existir, não quebra nada."""
    conn.execute(DDL)
    conn.commit()

# INSERT OR IGNORE: se a UNIQUE for violada (mesmo city_name/country_code/ts_utc), o SQLite ignora a inserção (não lança erro).
def insert_observation(conn: sqlite3.Connection, row: dict) -> bool:
    """Insere 1 linha. Retorna True se inseriu, False se ignorou (duplicata)."""
    sql = """
    INSERT OR IGNORE INTO weather_observations (
        city_name, country_code, lat, lon, ts_utc, tz,
        temp_c, feels_like_c, humidity, pressure, wind_speed, wind_dir,
        clouds, visibility_km, weather_description
    ) VALUES (
        :city_name, :country_code, :lat, :lon, :ts_utc, :tz,
        :temp_c, :feels_like_c, :humidity, :pressure, :wind_speed, :wind_dir,
        :clouds, :visibility_km, :weather_description
    );
    """
    cur = conn.execute(sql, row)
    conn.commit()
    return cur.rowcount > 0

def main() -> None:
    """
    - Cidade padrão vem do .env (se existir), senão usa Rio/BR (bom para testes).
    - Coleta os dados atuais pela Weatherbit.
    - Parseia para o formato do banco.
    - Abre conexão com with sqlite3.connect(...):
        - garante fechar a conexão automaticamente;
        - chama ensure_schema() (cria a tabela se faltar);
        - insert_observation() cuida da inserção/duplicata.
    - Log: converte ts_utc (UTC) para horário local (tz) apenas para imprimir, confirmando se foi inserido ou ignorado.
    """

    # Cidade padrão — pode vir do .env se preferir:
    city = os.getenv("DEFAULT_CITY", "Rio de Janeiro")
    country = os.getenv("DEFAULT_COUNTRY", "BR")

    # 1) Coleta
    payload = fetch_by_city(city=city, country=country, lang="pt", units="M")

    # 2) Parse
    row = parse_current(payload)

    # 3) Salva no SQLite
    with sqlite3.connect(DB_PATH) as conn:
        ensure_schema(conn)
        created = insert_observation(conn, row)

    # 4) Log simples
    # Mostra horário local calculado a partir do ts_utc para conferência
    dt_local = datetime.fromtimestamp(row["ts_utc"], tz=timezone.utc).astimezone(ZoneInfo(row["tz"]))
    if created:
        print(f"[OK] Inserido: {row['city_name']}-{row['country_code']} @ {dt_local:%Y-%m-%d %H:%M} ({row['tz']})")
    else:
        print(f"[IGNORADO] Duplicado para {row['city_name']}-{row['country_code']} @ {dt_local:%Y-%m-%d %H:%M} ({row['tz']})")

if __name__ == "__main__":
    main()