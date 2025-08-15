# app/plot_weather.py
"""
Geração de gráficos estáticos a partir do SQLite (média diária).

- Carrega dados via read_from_sqlite (fetch_range)
- Converte ts_utc -> datetime local (coluna 'ts_local')
- Agrega SEMPRE por média diária (resample 'D')
- Eixo X com ticks apenas nos pontos existentes
- Salva PNGs em ./plots/

Uso:
    # janela de 7 dias (exemplo)
    HOURS_WINDOW=168 python -m app.plot_weather
"""
from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FixedLocator
import pandas as pd

from app.read_from_sqlite import fetch_range, rows_to_df

PLOTS_DIR = Path("./plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------- utilitários de tempo/df ---------------------- #
def add_ts_local(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona coluna ts_local (timezone-aware) a partir de ts_utc e tz."""
    if df.empty:
        return df
    tz = df["tz"].iloc[0] or "UTC"  # registros da mesma cidade devem ter o mesmo tz
    dt_local = pd.to_datetime(df["ts_utc"], unit="s", utc=True).dt.tz_convert(ZoneInfo(tz))
    out = df.copy()
    out["ts_local"] = dt_local
    return out


def daily_mean(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa por dia local e calcula médias das métricas numéricas."""
    if df.empty:
        return df
    df2 = df.set_index("ts_local").sort_index()

    # médias das colunas numéricas
    agg = df2.resample("D").mean(numeric_only=True)

    # preserva metadados (cidade/país/tz) pegando o primeiro do dia
    meta = df2[["city_name", "country_code", "tz"]].resample("D").first()
    out = pd.concat([agg, meta], axis=1)
    out["ts_local"] = out.index
    return out.reset_index(drop=True)


# ------------------- utilitário de formatação do eixo X ------------------- #
def set_ticks_at_points(ax, x_series: pd.Series, fmt: str = "%d/%m"):
    """
    Define ticks exatamente nos pontos presentes em x_series e formata os rótulos.
    - x_series: pandas Series de datetimes (timezone-aware ok)
    - fmt: formato do rótulo (ex.: "%d/%m" para dia/mês)
    """
    # garante ordem e unicidade
    xs = pd.to_datetime(pd.Series(x_series)).sort_values().drop_duplicates()
    if xs.empty:
        return

    # locator fixo nos pontos da série
    ax.xaxis.set_major_locator(FixedLocator(mdates.date2num(xs.to_list())))

    # formatter para exibir como dia/mês (sem hora)
    ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))

    # deixa os rótulos centralizados e sem rotação (pra diários)
    for label in ax.get_xticklabels():
        label.set_rotation(0)
        label.set_horizontalalignment("center")


# --------------------------- funções de plot -------------------------- #
def plot_temp(df: pd.DataFrame, city_tag: str) -> Path:
    """Linha: temperatura (média diária)."""
    if df.empty:
        raise ValueError("DataFrame vazio para plot_temp.")
    df = df.sort_values("ts_local")
    fig = plt.figure()
    ax = plt.gca()
    ax.plot(df["ts_local"], df["temp_c"], marker="o", label="Temperatura (°C)")
    ax.set_xlabel("Tempo")
    ax.set_ylabel("°C")
    ax.set_title(f"Temperatura — {city_tag} (média diária)")
    ax.legend()
    set_ticks_at_points(ax, df["ts_local"], fmt="%d/%m")
    out = PLOTS_DIR / f"temp_{city_tag.replace(' ', '_')}.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


def plot_temp_vs_feels(df: pd.DataFrame, city_tag: str) -> Path:
    """Linha: temperatura vs. sensação (média diária)."""
    if df.empty:
        raise ValueError("DataFrame vazio para plot_temp_vs_feels.")
    df = df.sort_values("ts_local")
    fig = plt.figure()
    ax = plt.gca()
    ax.plot(df["ts_local"], df["temp_c"], marker="o", label="Temp (°C)")
    ax.plot(df["ts_local"], df["feels_like_c"], marker="o", label="Sensação (°C)")
    ax.set_xlabel("Tempo")
    ax.set_ylabel("°C")
    ax.set_title(f"Temp vs Sensação — {city_tag} (média diária)")
    ax.legend()
    set_ticks_at_points(ax, df["ts_local"], fmt="%d/%m")
    out = PLOTS_DIR / f"temp_feels_{city_tag.replace(' ', '_')}.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


def plot_humidity(df: pd.DataFrame, city_tag: str) -> Path:
    """Linha: umidade (média diária)."""
    if df.empty:
        raise ValueError("DataFrame vazio para plot_humidity.")
    df = df.sort_values("ts_local")
    fig = plt.figure()
    ax = plt.gca()
    ax.plot(df["ts_local"], df["humidity"], marker="o", label="Umidade (%)")
    ax.set_xlabel("Tempo")
    ax.set_ylabel("%")
    ax.set_title(f"Umidade — {city_tag} (média diária)")
    ax.legend()
    set_ticks_at_points(ax, df["ts_local"], fmt="%d/%m")
    out = PLOTS_DIR / f"humidity_{city_tag.replace(' ', '_')}.png"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


# ------------------------------- main -------------------------------- #
def main():
    """Gera 3 gráficos (média diária) para a cidade do .env ou padrão."""
    city = os.getenv("DEFAULT_CITY", "Rio de Janeiro")
    country = os.getenv("DEFAULT_COUNTRY", "BR")
    hours_window = int(os.getenv("HOURS_WINDOW", "168"))  # padrão 7 dias

    now_utc = int(datetime.now(tz=timezone.utc).timestamp())
    start_utc = now_utc - hours_window * 3600

    rows = fetch_range(city=city, country=country, start_utc=start_utc, end_utc=now_utc)
    df = rows_to_df(rows)
    df = add_ts_local(df)

    if df.empty:
        print("Sem dados para plotar. Rode uma coleta antes (save_to_sqlite).")
        return

    # AGREGAÇÃO SEMPRE por média diária
    df = daily_mean(df)

    tag = f"{city}-{country}"
    p1 = plot_temp(df, tag)
    p2 = plot_temp_vs_feels(df, tag)
    p3 = plot_humidity(df, tag)
    print(f"Gráficos salvos em:\n - {p1}\n - {p2}\n - {p3}")


if __name__ == "__main__":
    main()
