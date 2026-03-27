# -*- coding: utf-8 -*-
from __future__ import annotations
"""FRED API client for MYA Intelligence.

Fetches economic indicator series from the St. Louis Fed FRED API.
Handles rate limits, caching, and graceful degradation.
"""

import os
import logging
import requests
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


class FredClient:
    """Wrapper around the FRED API."""

    def __init__(self):
        self.api_key = os.getenv("FRED_API_KEY", "")
        if not self.api_key:
            log.warning("FRED_API_KEY not set -- FRED data will be unavailable")
        self._cache: dict[str, dict] = {}

    def fetch_series(self, series_id: str, lookback_days: int = 365) -> dict:
        """Fetch a single FRED series and return processed snapshot.

        Returns:
            dict with keys: series_id, value, previous_value,
            change_abs, change_pct, observation_date, history (list of dicts)
        """
        if not self.api_key:
            return self._empty(series_id, "No API key")

        start = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        try:
            resp = requests.get(FRED_BASE, params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": start,
                "sort_order": "desc",
                "limit": 500,
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.warning("FRED fetch failed for %s: %s", series_id, e)
            return self._empty(series_id, str(e))

        observations = data.get("observations", [])
        # Filter out missing values
        valid = [
            o for o in observations
            if o.get("value") not in (None, ".", "")
        ]

        if not valid:
            return self._empty(series_id, "No valid observations")

        latest = valid[0]
        previous = valid[1] if len(valid) > 1 else None

        current_val = float(latest["value"])
        prev_val = float(previous["value"]) if previous else None

        change_abs = (current_val - prev_val) if prev_val is not None else None
        change_pct = (change_abs / abs(prev_val) * 100) if prev_val and prev_val != 0 else None

        # Build history (ascending order for charts)
        history = [
            {"date": o["date"], "value": float(o["value"])}
            for o in reversed(valid)
        ]

        result = {
            "series_id": series_id,
            "value": current_val,
            "previous_value": prev_val,
            "change_abs": change_abs,
            "change_pct": change_pct,
            "observation_date": latest["date"],
            "history": history,
        }

        self._cache[series_id] = result
        return result

    def fetch_multiple(self, series_list: list[dict]) -> dict[str, dict]:
        """Fetch multiple FRED series.

        Args:
            series_list: list of dicts with 'series' key (and optional 'label')

        Returns:
            dict mapping series_id -> snapshot dict
        """
        results = {}
        for item in series_list:
            sid = item["series"]
            result = self.fetch_series(sid)
            result["label"] = item.get("label", sid)
            results[sid] = result
        return results

    def get_cached(self, series_id: str) -> dict | None:
        """Return cached data if available."""
        return self._cache.get(series_id)

    @staticmethod
    def _empty(series_id: str, reason: str) -> dict:
        return {
            "series_id": series_id,
            "value": None,
            "previous_value": None,
            "change_abs": None,
            "change_pct": None,
            "observation_date": None,
            "history": [],
            "error": reason,
        }
