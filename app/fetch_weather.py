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
    """Função que faz a leitura da API_KEY."""
    key = os.getenv("WEATHERBIT_API_KEY")
    if not key:
        print("ERRO: variável WEATHERBIT_API_KEY não encontrada no .env", file=sys.stderr)
        sys.exit(1)
    return key

def fetch_by_city(city: str, country: str | None = None, lang: str = "pt", units: str = "M") -> Dict[str, Any]:
    """
    Busca dados meteorológicos atuais da Weatherbit pelo nome da cidade.

    Args:
        city (str): Nome da cidade.
        country (str, opcional): Código do país no formato ISO (ex: "BR").
        lang (str, opcional): Idioma da resposta (padrão: "pt").
        units (str, opcional): Unidade de medida (padrão: "M").

    Returns:
        dict: Resposta JSON da API.
    """
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
    """
    Busca dados meteorológicos atuais da Weatherbit através da latitude e da longitude.

    Args:
        lat (float): Latitude.
        lon (float): Longitude.
        lang (str, opcional): Idioma (padrão: "pt").
        units (str, opcional): Unidade de medida (padrão: "M").

    Returns:
        dict: Resposta JSON da API.
    """
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
    """
    Gera um resumo em formato de texto dos dados meteorológicos recebidos.

    Observação:
        Esta função é utilizada apenas para testes e depuração.
        A API retorna horários em UTC no campo 'ob_time', por mais que na documentação não deixe isso explícito.
        Por isso, é usado o timestamp 'ts' para conversão.

    Args:
        payload (dict): Resposta JSON da API Weatherbit.

    Returns:
        str: Texto formatado com resumo das informações meteorológicas.
    """
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

if __name__ == "__main__":
    data = fetch_by_city(city="Rio de Janeiro")
    print(summarize(data))