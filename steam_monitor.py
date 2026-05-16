"""Consulta à API de preços da Steam Community Market e parsing de valores em R$."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import requests
from urllib.parse import urlencode

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


@dataclass
class PriceOverview:
    success: bool
    lowest_price_raw: str | None
    median_price_raw: str | None
    volume: str | None
    lowest_brl: float | None
    median_brl: float | None
    raw: dict[str, Any]


def parse_brl_string(value: str | None) -> float | None:
    """Converte strings como 'R$ 820,80' ou 'R$1.234,56' para float."""
    if not value or not isinstance(value, str):
        return None
    cleaned = value.replace("R$", "").strip()
    cleaned = cleaned.replace(".", "").replace(",", ".")
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def fetch_price_overview(url: str, timeout: float = 30.0) -> PriceOverview:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    lowest_raw = data.get("lowest_price")
    median_raw = data.get("median_price")
    vol = data.get("volume")
    ok = bool(data.get("success"))

    return PriceOverview(
        success=ok,
        lowest_price_raw=lowest_raw if isinstance(lowest_raw, str) else None,
        median_price_raw=median_raw if isinstance(median_raw, str) else None,
        volume=str(vol) if vol is not None else None,
        lowest_brl=parse_brl_string(lowest_raw) if isinstance(lowest_raw, str) else None,
        median_brl=parse_brl_string(median_raw) if isinstance(median_raw, str) else None,
        raw=data if isinstance(data, dict) else {},
    )


def build_priceoverview_url(appid: int, currency: int, market_hash_name: str) -> str:
    params = {
        "appid": str(appid),
        "currency": str(currency),
        "market_hash_name": market_hash_name,
    }
    return "https://steamcommunity.com/market/priceoverview/?" + urlencode(params)
