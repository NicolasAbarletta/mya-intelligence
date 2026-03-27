# -*- coding: utf-8 -*-
from __future__ import annotations
"""Volatility & Options Analytics for MYA Intelligence.

Computes IV vs RV analysis, vol regime, term structure signals,
and options-specific metrics for position monitoring.
"""

import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field

from src.data.market_client import hist_cache

log = logging.getLogger(__name__)


@dataclass
class VolSnapshot:
    """Volatility snapshot for a single ticker."""
    ticker: str
    implied_vol: float | None = None
    realized_vol_20d: float | None = None
    realized_vol_60d: float | None = None
    iv_rv_gap: float | None = None         # IV - RV20d
    iv_rv_ratio: float | None = None       # IV / RV20d
    vol_regime: str = "normal"             # "compressed", "normal", "elevated", "extreme"
    iv_percentile_1y: float | None = None  # Where current IV sits in 1y range
    rv_trend: str = "stable"               # "declining", "stable", "rising"
    skewness: float | None = None


@dataclass
class VolSurface:
    """Aggregate vol analytics across the watchlist."""
    snapshots: dict[str, VolSnapshot] = field(default_factory=dict)
    avg_iv_rv_gap: float | None = None
    vol_regime_summary: str = ""
    underpriced_vol: list[str] = field(default_factory=list)
    overpriced_vol: list[str] = field(default_factory=list)


def compute_vol_snapshot(ticker: str, market_snap: dict) -> VolSnapshot:
    """Compute vol analytics for a single ticker.

    Args:
        ticker: symbol
        market_snap: snapshot dict from MarketClient
    """
    vs = VolSnapshot(ticker=ticker)

    iv = market_snap.get("implied_vol")
    rv20 = market_snap.get("realized_vol_20d")
    vs.implied_vol = iv
    vs.realized_vol_20d = rv20

    # IV-RV gap
    if iv is not None and rv20 is not None:
        vs.iv_rv_gap = iv - rv20
        vs.iv_rv_ratio = iv / rv20 if rv20 > 0 else None

    # Compute RV60d from cached history
    hist = hist_cache.get(ticker)
    if hist is not None and len(hist) >= 60:
        close = hist["Close"]
        log_ret = np.log(close / close.shift(1)).dropna()
        if len(log_ret) >= 60:
            vs.realized_vol_60d = float(
                log_ret.tail(60).std() * np.sqrt(252) * 100
            )

        # RV trend (20d vs 60d)
        if vs.realized_vol_20d and vs.realized_vol_60d:
            ratio = vs.realized_vol_20d / vs.realized_vol_60d
            if ratio > 1.2:
                vs.rv_trend = "rising"
            elif ratio < 0.8:
                vs.rv_trend = "declining"

        # IV percentile over trailing 1y (using RV as proxy if no IV history)
        if rv20 and len(log_ret) >= 200:
            # Compute rolling 20d vol for percentile
            rolling_vol = log_ret.rolling(20).std() * np.sqrt(252) * 100
            rolling_vol = rolling_vol.dropna()
            if len(rolling_vol) > 0:
                pct = (rolling_vol < rv20).mean()
                vs.iv_percentile_1y = float(pct * 100)

    # Skewness
    vs.skewness = market_snap.get("skewness_60d")

    # Vol regime classification
    if rv20 is not None:
        if rv20 > 50:
            vs.vol_regime = "extreme"
        elif rv20 > 30:
            vs.vol_regime = "elevated"
        elif rv20 < 10:
            vs.vol_regime = "compressed"
        else:
            vs.vol_regime = "normal"

    return vs


def compute_vol_surface(market_data: dict) -> VolSurface:
    """Compute aggregate vol analytics across all tickers.

    Args:
        market_data: dict[ticker -> snapshot] from DataManager
    """
    surface = VolSurface()
    gaps = []

    for ticker, snap in market_data.items():
        if snap.get("price") is None:
            continue

        vs = compute_vol_snapshot(ticker, snap)
        surface.snapshots[ticker] = vs

        if vs.iv_rv_gap is not None:
            gaps.append(vs.iv_rv_gap)

            if vs.iv_rv_gap < -5:
                surface.underpriced_vol.append(ticker)
            elif vs.iv_rv_gap > 10:
                surface.overpriced_vol.append(ticker)

    if gaps:
        surface.avg_iv_rv_gap = sum(gaps) / len(gaps)

    # Summary
    n_elevated = sum(
        1 for v in surface.snapshots.values()
        if v.vol_regime in ("elevated", "extreme")
    )
    n_compressed = sum(
        1 for v in surface.snapshots.values()
        if v.vol_regime == "compressed"
    )
    total = len(surface.snapshots)

    if n_elevated > total * 0.4:
        surface.vol_regime_summary = "Broad vol elevation -- risk-off environment"
    elif n_compressed > total * 0.4:
        surface.vol_regime_summary = "Broad vol compression -- complacency, cheap hedges available"
    else:
        surface.vol_regime_summary = "Mixed vol regime -- selective opportunities"

    return surface
