# =============== Funções auxiliares para teste/local demo ===============

from typing import Iterable
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from app.read_from_sqlite import fetch_latest, fetch_range


def _format_row(row: tuple) -> str:
    """
    Formata uma linha para impressão.
    row: (city_name, country_code, ts_utc, tz, temp_c, feels_like_c, humidity, weather_description)
    """
    (city, country, ts_utc, tz_name, temp, feels_like, rh, desc) = row
    dt_local = datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(ZoneInfo(tz_name or "UTC"))
    return (f"{city}-{country} | {dt_local:%Y-%m-%d %H:%M} ({tz_name}) | "
            f"{temp}°C (sens. {feels_like}°C) | {rh}% umid | {desc}")

def print_rows(rows: Iterable[tuple]) -> None:
    """Imprime linhas formatadas (uma por linha)."""
    count = 0
    for r in rows:
        print(_format_row(r))
        count += 1
    if count == 0:
        print("Nenhum registro encontrado.")

def _demo_latest() -> None:
    """Exemplo rápido: últimas 5 leituras da cidade padrão (.env opcional)."""
    city = os.getenv("DEFAULT_CITY", "Rio de Janeiro")
    country = os.getenv("DEFAULT_COUNTRY", "BR")
    rows = fetch_latest(city=city, country=country, limit=5)
    print_rows(rows)


def _demo_range() -> None:
    """Exemplo rápido: intervalo das últimas 24h."""
    city = os.getenv("DEFAULT_CITY", "Rio de Janeiro")
    country = os.getenv("DEFAULT_COUNTRY", "BR")
    now_utc = int(datetime.now(tz=timezone.utc).timestamp())
    day_ago = now_utc - 24 * 3600
    rows = fetch_range(city=city, country=country, start_utc=day_ago, end_utc=now_utc)
    print_rows(rows)

if __name__ == "__main__":
    # Rode um dos demos abaixo (comente/descomente conforme o que quiser testar)
    _demo_latest()
    # _demo_range()