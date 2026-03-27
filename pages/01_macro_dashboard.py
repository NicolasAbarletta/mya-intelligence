# -*- coding: utf-8 -*-
from __future__ import annotations
"""Page 1: Macro Dashboard.

Real-time macro regime indicator, key FRED charts, DXY/rates/commodities strip.
"""

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import plotly.graph_objects as go
from src.data.data_manager import DataManager
from src.analysis.macro_monitor import classify_regime
from src.utils.helpers import load_theses

st.set_page_config(page_title="Macro Dashboard | MYA", page_icon="M", layout="wide")

st.markdown("## Macro Dashboard")
st.caption("Regime classification, key indicators, and cross-asset context")

dm: DataManager = st.session_state.get("data_manager")
if dm is None or not dm.fred_data:
    st.info("No data loaded. Return to the home page and run the pipeline.")
    st.stop()

# -- Regime classification --
regime = st.session_state.get("regime")
if regime is None and dm.market_data and dm.fred_data:
    regime = classify_regime(dm.market_data, dm.fred_data)
    st.session_state.regime = regime

if regime:
    st.markdown(
        f'<div style="background:{regime.overall_color}15;border:2px solid {regime.overall_color};'
        f'border-radius:8px;padding:16px;margin-bottom:20px">'
        f'<span style="font-size:1.4rem;font-weight:700;color:{regime.overall_color}">'
        f'{regime.overall.upper()}</span>'
        f' <span style="color:#888;font-size:0.9rem">Stress Score: {regime.score:.0f}/100</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Regime components
    r1, r2, r3, r4, r5 = st.columns(5)
    r1.metric("VIX Regime", regime.vix_regime.title(),
              f"{regime.components.get('vix', 0):.1f}" if regime.components.get('vix') else None)
    r2.metric("Credit", regime.credit_regime.title(),
              f"{regime.components.get('hy_spread', 0):.0f}bps" if regime.components.get('hy_spread') else None)
    r3.metric("Yield Curve", regime.curve_regime.title(),
              f"{regime.components.get('yield_curve', 0):+.2f}" if regime.components.get('yield_curve') is not None else None)
    r4.metric("Fed Policy", regime.policy_regime.title())
    r5.metric("Dollar", regime.dollar_regime.title())

    st.divider()

# -- Vol Surface Summary --
vol_surface = st.session_state.get("vol_surface")
if vol_surface:
    st.markdown("### Volatility Regime")
    st.markdown(f"*{vol_surface.vol_regime_summary}*")

    vc1, vc2, vc3 = st.columns(3)
    with vc1:
        avg_gap = vol_surface.avg_iv_rv_gap
        st.metric("Avg IV-RV Gap",
                  f"{avg_gap:+.1f}%" if avg_gap is not None else "N/A")
    with vc2:
        st.metric("Underpriced Vol", f"{len(vol_surface.underpriced_vol)} tickers")
        if vol_surface.underpriced_vol:
            st.caption(", ".join(vol_surface.underpriced_vol[:8]))
    with vc3:
        st.metric("Overpriced Vol", f"{len(vol_surface.overpriced_vol)} tickers")
        if vol_surface.overpriced_vol:
            st.caption(", ".join(vol_surface.overpriced_vol[:8]))

    st.divider()

# -- FRED charts --
st.markdown("### Key FRED Series")

theses = load_theses()
macro = theses.get("macro_regime", {})
fred_indicators = macro.get("indicators", {}).get("fred", [])

for ind in fred_indicators:
    sid = ind["series"]
    data = dm.get_fred_snapshot(sid)
    if not data or not data.get("history"):
        continue

    col1, col2 = st.columns([1, 3])
    with col1:
        st.metric(
            data.get("label", sid),
            f"{data['value']:,.2f}",
            f"{data.get('change_pct', 0):+.2f}%" if data.get("change_pct") else None,
        )
        st.caption(f"As of {data.get('observation_date', 'N/A')}")

    with col2:
        history = data["history"]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[h["date"] for h in history],
            y=[h["value"] for h in history],
            mode="lines",
            line=dict(color="#4A9EFF", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(74,158,255,0.05)",
        ))
        fig.update_layout(
            height=120,
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, showticklabels=True, color="#666"),
            yaxis=dict(showgrid=True, gridcolor="#1A1D23", color="#666"),
        )
        st.plotly_chart(fig, use_container_width=True)

# -- Market macro strip --
st.divider()
st.markdown("### Market Macro Strip")

macro_tickers = macro.get("indicators", {}).get("market", [])
if macro_tickers:
    cols = st.columns(min(len(macro_tickers), 8))
    for i, ind in enumerate(macro_tickers[:8]):
        ticker = ind["ticker"]
        snap = dm.get_market_snapshot(ticker)
        with cols[i]:
            if snap and snap.get("price"):
                change = snap.get("change_1d_pct")
                st.metric(
                    ind.get("label", ticker),
                    f"${snap['price']:.2f}" if snap['price'] < 10000 else f"{snap['price']:,.0f}",
                    f"{change:+.2f}%" if change else None,
                )
            else:
                st.metric(ind.get("label", ticker), "N/A")
