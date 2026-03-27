# -*- coding: utf-8 -*-
"""Shared utilities for MYA Intelligence."""

import yaml
import uuid
from pathlib import Path
from datetime import datetime, timezone

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"


def load_yaml(filename: str) -> dict:
    """Load a YAML config file from the config/ directory."""
    path = CONFIG_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_theses() -> dict:
    return load_yaml("theses.yaml")


def load_watchlists() -> dict:
    return load_yaml("watchlists.yaml")


def load_alerts_config() -> dict:
    return load_yaml("alerts.yaml")


def new_run_id() -> str:
    """Generate a unique run ID."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"{ts}_{short}"


def all_tickers_from_config() -> list[str]:
    """Extract every unique ticker from theses.yaml and watchlists.yaml."""
    tickers = set()

    # From theses
    theses = load_theses()
    for t in theses.values():
        indicators = t.get("indicators", {})
        for item in indicators.get("market", []):
            tickers.add(item["ticker"])

    # From watchlists
    wl = load_watchlists()
    for group in wl.get("equities", {}).values():
        for sym in group:
            tickers.add(sym)

    return sorted(tickers)


def all_fred_series_from_config() -> list[dict]:
    """Extract every unique FRED series from theses.yaml."""
    seen = set()
    series = []
    theses = load_theses()
    for t in theses.values():
        indicators = t.get("indicators", {})
        for item in indicators.get("fred", []):
            sid = item["series"]
            if sid not in seen:
                seen.add(sid)
                series.append(item)
    return series


def all_news_keywords_from_config() -> dict[str, list[str]]:
    """Extract news keywords mapped to thesis IDs."""
    result = {}
    theses = load_theses()
    for thesis_id, t in theses.items():
        indicators = t.get("indicators", {})
        keywords = indicators.get("news_keywords", [])
        if keywords:
            result[thesis_id] = keywords
    return result


def fmt_pct(value: float | None, decimals: int = 2) -> str:
    """Format a percentage value for display."""
    if value is None:
        return "N/A"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{decimals}f}%"


def fmt_number(value: float | None, decimals: int = 2) -> str:
    """Format a number with commas."""
    if value is None:
        return "N/A"
    return f"{value:,.{decimals}f}"


def severity_color(severity: str) -> str:
    """Return a hex color for severity level."""
    colors = {
        "critical": "#FF4444",
        "high": "#FF8C00",
        "medium": "#FFD700",
        "low": "#888888",
    }
    return colors.get(severity, "#888888")


def staleness_label(ts_str: str | None) -> tuple[str, str]:
    """Return a staleness description and color given an ISO timestamp."""
    if not ts_str:
        return "No data", "#FF4444"
    try:
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - ts
        minutes = age.total_seconds() / 60
        if minutes < 35:
            return f"{int(minutes)}m ago", "#00CC66"
        elif minutes < 120:
            return f"{int(minutes)}m ago", "#FFD700"
        elif minutes < 1440:
            return f"{int(minutes // 60)}h ago", "#FF8C00"
        else:
            return f"{int(minutes // 1440)}d ago", "#FF4444"
    except Exception:
        return "Unknown", "#FF4444"
