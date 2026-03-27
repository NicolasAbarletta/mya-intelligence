# -*- coding: utf-8 -*-
from __future__ import annotations
"""Macro Regime Monitor for MYA Intelligence.

Classifies the current macro environment into regimes based on
composite indicators: VIX, credit spreads, yield curve, Fed policy,
and dollar strength. Produces a regime label and color for the dashboard.
"""

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class MacroRegime:
    """Current macro regime classification."""
    overall: str          # "risk-on", "neutral", "stress", "crisis"
    overall_color: str    # hex color for dashboard
    vix_regime: str       # "complacent", "calm", "elevated", "fear", "panic"
    credit_regime: str    # "tight", "normal", "loose", "stressed"
    curve_regime: str     # "steep", "normal", "flat", "inverted"
    policy_regime: str    # "easing", "neutral", "tightening"
    dollar_regime: str    # "weak", "neutral", "strong"
    score: float          # 0-100 composite stress score
    components: dict      # raw component values
    summary: str          # one-line description


def classify_regime(market_data: dict, fred_data: dict) -> MacroRegime:
    """Classify the current macro regime from raw data.

    Args:
        market_data: dict[ticker -> snapshot dict]
        fred_data: dict[series_id -> snapshot dict]

    Returns:
        MacroRegime with full classification
    """
    components = {}
    stress_score = 50  # Start neutral

    # -- VIX regime --
    vix_data = market_data.get("^VIX", {})
    vix = vix_data.get("price")
    components["vix"] = vix

    if vix is None:
        vix_regime = "unknown"
    elif vix > 35:
        vix_regime = "panic"
        stress_score += 25
    elif vix > 25:
        vix_regime = "fear"
        stress_score += 15
    elif vix > 18:
        vix_regime = "elevated"
        stress_score += 5
    elif vix > 13:
        vix_regime = "calm"
        stress_score -= 5
    else:
        vix_regime = "complacent"
        stress_score -= 10

    # -- Credit regime (HY OAS) --
    hy_data = fred_data.get("BAMLH0A0HYM2", {})
    hy_spread = hy_data.get("value")
    components["hy_spread"] = hy_spread

    if hy_spread is None:
        credit_regime = "unknown"
    elif hy_spread > 600:
        credit_regime = "stressed"
        stress_score += 20
    elif hy_spread > 450:
        credit_regime = "tight"
        stress_score += 10
    elif hy_spread > 300:
        credit_regime = "normal"
    else:
        credit_regime = "loose"
        stress_score -= 10

    # -- Yield curve regime --
    curve_data = fred_data.get("T10Y2Y", {})
    curve = curve_data.get("value")
    components["yield_curve"] = curve

    if curve is None:
        curve_regime = "unknown"
    elif curve < -0.5:
        curve_regime = "inverted"
        stress_score += 15
    elif curve < 0:
        curve_regime = "flat"
        stress_score += 5
    elif curve < 1.0:
        curve_regime = "normal"
    else:
        curve_regime = "steep"
        stress_score -= 5

    # -- Policy regime (Fed Funds Rate trajectory) --
    dff_data = fred_data.get("DFF", {})
    fed_funds = dff_data.get("value")
    fed_change = dff_data.get("change_abs")
    components["fed_funds"] = fed_funds
    components["fed_change"] = fed_change

    if fed_change is None:
        policy_regime = "neutral"
    elif fed_change > 0:
        policy_regime = "tightening"
        stress_score += 5
    elif fed_change < 0:
        policy_regime = "easing"
        stress_score -= 5
    else:
        policy_regime = "neutral"

    # -- Dollar regime --
    dxy_data = market_data.get("UUP", {})
    dxy_5d = dxy_data.get("change_5d_pct")
    components["dollar_5d"] = dxy_5d

    if dxy_5d is None:
        dollar_regime = "neutral"
    elif dxy_5d > 1.5:
        dollar_regime = "strong"
        stress_score += 5
    elif dxy_5d < -1.5:
        dollar_regime = "weak"
        stress_score -= 3
    else:
        dollar_regime = "neutral"

    # -- Fed balance sheet --
    bs_data = fred_data.get("WALCL", {})
    bs_change = bs_data.get("change_pct")
    components["fed_bs_change"] = bs_change
    if bs_change is not None:
        if bs_change > 0.5:
            stress_score -= 5  # Expansion = liquidity
        elif bs_change < -0.5:
            stress_score += 5  # Tightening = less liquidity

    # -- Consumer sentiment --
    sent_data = fred_data.get("UMCSENT", {})
    sentiment = sent_data.get("value")
    components["consumer_sentiment"] = sentiment
    if sentiment is not None:
        if sentiment < 60:
            stress_score += 5
        elif sentiment > 80:
            stress_score -= 5

    # -- CPI --
    cpi_data = fred_data.get("CPIAUCSL", {})
    cpi_change = cpi_data.get("change_pct")
    components["cpi_change"] = cpi_change
    if cpi_change is not None and cpi_change > 0.4:
        stress_score += 5

    # -- Clamp score --
    stress_score = max(0, min(100, stress_score))

    # -- Overall classification --
    if stress_score >= 75:
        overall = "crisis"
        overall_color = "#FF4444"
    elif stress_score >= 60:
        overall = "stress"
        overall_color = "#FF8C00"
    elif stress_score >= 40:
        overall = "neutral"
        overall_color = "#FFD700"
    else:
        overall = "risk-on"
        overall_color = "#00CC66"

    # -- Build summary --
    parts = []
    if vix is not None:
        parts.append(f"VIX {vix:.0f} ({vix_regime})")
    if hy_spread is not None:
        parts.append(f"HY {hy_spread:.0f}bps ({credit_regime})")
    if curve is not None:
        parts.append(f"Curve {curve:+.2f} ({curve_regime})")
    parts.append(f"Policy: {policy_regime}")
    parts.append(f"Dollar: {dollar_regime}")
    summary = f"{overall.upper()} [score: {stress_score}] -- " + ", ".join(parts)

    return MacroRegime(
        overall=overall,
        overall_color=overall_color,
        vix_regime=vix_regime,
        credit_regime=credit_regime,
        curve_regime=curve_regime,
        policy_regime=policy_regime,
        dollar_regime=dollar_regime,
        score=stress_score,
        components=components,
        summary=summary,
    )
