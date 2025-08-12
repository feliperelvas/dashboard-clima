import requests
import os
import sys
from dotenv import load_dotenv
from typing import Any, Dict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

BASE_URL = "https://api.weatherbit.io/v2.0/current"
load_dotenv() 

def get_api_key() -> str:
    key = os.getenv("WEATHERBIT_API_KEY")
    if not key:
        print("ERRO: variável WEATHERBIT_API_KEY não encontrada no .env", file=sys.stderr)
        sys.exit(1)
    return key

def fetch_by_city(city: str, country: str | None = None, lang: str = "pt", units: str = "M") -> Dict[str, Any]:
    params = {
        "city": city,
        "key": get_api_key(),
        "lang": lang,
        "units": units,
    }
    if country:
        params["country"] = country
    r = requests.get(BASE_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_by_coords(lat: float, lon: float, lang: str = "pt", units: str = "M") -> Dict[str, Any]:
    params = {
        "lat": lat,
        "lon": lon,
        "key": get_api_key(),
        "lang": lang,
        "units": units,
    }
    r = requests.get(BASE_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def summarize(payload: dict) -> str:
    try:
        d = payload["data"][0]
    except (KeyError, IndexError, TypeError):
        return "Resposta inesperada da API:\n" + str(payload)

    # Converte o ts (sempre em UTC) para datetime
    dt_utc = datetime.fromtimestamp(d["ts"], tz=timezone.utc)

    # Converte para o fuso informado pela API
    tz_name = d.get("timezone", "UTC")
    try:
        dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
    except Exception:
        dt_local = dt_utc  # fallback caso não consiga interpretar

    # Monta o texto
    parts = [
        f"Cidade: {d.get('city_name')}, {d.get('country_code')}",
        f"Tempo: {d.get('weather', {}).get('description')}",
        f"Temp: {d.get('temp')} °C (sensação {d.get('app_temp')} °C)",
        f"Umidade: {d.get('rh')}%",
        f"Vento: {d.get('wind_spd')} m/s direção {d.get('wind_dir')}°",
        f"Nuvens: {d.get('clouds')}%",
        f"Visibilidade: {d.get('vis')} km",
        f"Hora local: {dt_local.strftime('%Y-%m-%d %H:%M')} ({tz_name})",
        f"Hora UTC: {dt_utc.strftime('%Y-%m-%d %H:%M')} (UTC)",
        f"Nascer do sol (UTC): {d.get('sunrise')}",
        f"Pôr do sol (UTC): {d.get('sunset')}",
    ]
    return "\n".join(parts)