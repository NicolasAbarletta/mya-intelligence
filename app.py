# -*- coding: utf-8 -*-
"""MYA Capital -- Market Intelligence & Signal Aggregation Agent.

Main Streamlit application entry point.
"""

import sys
import os
import logging
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from src.storage.db import init_db, get_all_freshness, get_recent_signals, get_recent_alerts
from src.data.data_manager import DataManager
from src.analysis.thesis_engine import ThesisEngine
from src.analysis.signal_detector import SignalDetector
from src.analysis.macro_monitor import classify_regime
from src.analysis.vol_surface import compute_vol_surface
from src.utils.helpers import load_theses, staleness_label, severity_color

# -- Logging --
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mya")

# -- Page config --
st.set_page_config(
    page_title="MYA Intelligence",
    page_icon="M",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -- Custom CSS --
st.markdown("""
<style>
    /* Dark professional theme overrides */
    .stApp {
        background-color: #0E1117;
    }
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #E0E0E0;
        margin-bottom: 0;
        letter-spacing: -0.5px;
    }
    .sub-header {
        font-size: 0.9rem;
        color: #888;
        margin-top: -8px;
        margin-bottom: 24px;
    }
    .freshness-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .severity-critical { color: #FF4444; font-weight: 700; }
    .severity-high { color: #FF8C00; font-weight: 600; }
    .severity-medium { color: #FFD700; }
    .severity-low { color: #888; }
    .metric-card {
        background: #1A1D23;
        border: 1px solid #2A2D35;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .thesis-card {
        background: #1A1D23;
        border-left: 3px solid #4A9EFF;
        border-radius: 0 8px 8px 0;
        padding: 16px;
        margin-bottom: 16px;
    }
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #0A0C10;
    }
</style>
""", unsafe_allow_html=True)


# -- Initialize DB --
init_db()

# -- Session state --
if "data_manager" not in st.session_state:
    st.session_state.data_manager = DataManager()
if "pipeline_run" not in st.session_state:
    st.session_state.pipeline_run = False
if "regime" not in st.session_state:
    st.session_state.regime = None
if "signals" not in st.session_state:
    st.session_state.signals = []
if "thesis_snapshots" not in st.session_state:
    st.session_state.thesis_snapshots = {}
if "vol_surface" not in st.session_state:
    st.session_state.vol_surface = None

dm: DataManager = st.session_state.data_manager


# -- Sidebar --
with st.sidebar:
    st.markdown('<p class="main-header">MYA</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Market Intelligence</p>',
                unsafe_allow_html=True)

    st.divider()

    # Data refresh controls
    st.markdown("**Data Pipeline**")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Full Refresh", use_container_width=True,
                      type="primary"):
            st.session_state.pipeline_run = True
    with col2:
        if st.button("Market Only", use_container_width=True):
            st.session_state.pipeline_run = "market_only"

    # Freshness indicators
    st.divider()
    st.markdown("**Data Freshness**")
    freshness = get_all_freshness()
    if freshness:
        for f in freshness:
            label, color = staleness_label(f["last_success_ts"])
            status_icon = "+" if f["status"] == "ok" else "~"
            st.markdown(
                f'<span class="freshness-badge" style="background:{color}20;color:{color}">'
                f'{status_icon} {f["source"]}: {label} ({f["record_count"]} records)'
                f'</span>',
                unsafe_allow_html=True,
            )
    else:
        st.caption("No data fetched yet. Click 'Full Refresh' to start.")

    # Active theses summary
    st.divider()
    st.markdown("**Active Theses**")
    theses = load_theses()
    for tid, t in theses.items():
        st.markdown(f"- {t['name']}")

    st.divider()
    st.caption("MYA Capital | Confidential")


# -- Pipeline execution --
if st.session_state.pipeline_run:
    mode = st.session_state.pipeline_run
    st.session_state.pipeline_run = False

    fetch_news = mode != "market_only"
    label = "Running full pipeline..." if fetch_news else "Fetching market data..."

    with st.spinner(label):
        run_id = dm.run_pipeline(fetch_news=fetch_news)

    # Run analysis layer
    with st.spinner("Running signal detection..."):
        # Macro regime
        regime = classify_regime(dm.market_data, dm.fred_data)
        st.session_state.regime = regime

        # Thesis snapshots
        engine = ThesisEngine(
            dm.market_data, dm.fred_data, dm.news_data, dm.news
        )
        snapshots = engine.build_all_snapshots()
        st.session_state.thesis_snapshots = snapshots

        # Signal detection
        detector = SignalDetector()
        signals = detector.evaluate_all(snapshots, run_id)
        st.session_state.signals = signals

        # Vol surface
        vol_surface = compute_vol_surface(dm.market_data)
        st.session_state.vol_surface = vol_surface

    signal_count = len(signals)
    critical = sum(1 for s in signals if s.get("severity") == "critical")
    st.toast(
        f"Pipeline complete: {signal_count} signals detected"
        + (f" ({critical} CRITICAL)" if critical else ""),
        icon="M",
    )


# -- Main content (home page) --
st.markdown('<p class="main-header">Market Intelligence Dashboard</p>',
            unsafe_allow_html=True)
st.markdown('<p class="sub-header">Thesis-driven signal detection for MYA Capital</p>',
            unsafe_allow_html=True)

if not dm.market_data:
    st.info(
        "No data loaded. Click **Full Refresh** in the sidebar to run "
        "the data pipeline. This fetches market prices, FRED macro indicators, "
        "and news for all active theses."
    )
    st.stop()

# -- Macro regime banner --
regime = st.session_state.regime
if regime:
    st.markdown(
        f'<div style="background:{regime.overall_color}15;border:1px solid {regime.overall_color};'
        f'border-radius:8px;padding:12px 16px;margin-bottom:16px">'
        f'<span style="font-size:1.1rem;font-weight:700;color:{regime.overall_color}">'
        f'REGIME: {regime.overall.upper()}</span>'
        f' <span style="color:#888;font-size:0.85rem">| Stress Score: {regime.score:.0f}/100</span><br>'
        f'<span style="color:#aaa;font-size:0.85rem">{regime.summary}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

# -- Critical/High alerts banner --
recent_alerts = get_recent_alerts(days=1)
critical_alerts = [a for a in recent_alerts if a.get("severity") in ("critical", "high")]
if critical_alerts:
    for a in critical_alerts[:3]:
        sev = a["severity"]
        color = severity_color(sev)
        st.markdown(
            f'<div style="background:{color}15;border-left:3px solid {color};'
            f'padding:8px 12px;margin-bottom:8px;border-radius:0 4px 4px 0">'
            f'<strong style="color:{color}">[{sev.upper()}]</strong> '
            f'{a.get("body", "")[:200]}'
            f'</div>',
            unsafe_allow_html=True,
        )

# -- Quick stats row --
tickers_with_data = dm.get_all_tickers_with_data()
fred_count = len([v for v in dm.fred_data.values() if v.get("value") is not None])
news_count = sum(len(v) for v in dm.news_data.values())
signal_count = len(st.session_state.signals)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Tickers", len(tickers_with_data))
c2.metric("FRED Series", fred_count)
c3.metric("News Articles", news_count)
c4.metric("Signals", signal_count)
c5.metric("Theses", len(theses))

st.divider()

# -- Thesis overview cards --
st.markdown("### Active Thesis Overview")

for tid, t in theses.items():
    with st.container():
        st.markdown(
            f'<div class="thesis-card">'
            f'<strong>{t["name"]}</strong><br>'
            f'<span style="color:#888;font-size:0.85rem">{t["core_view"][:200]}...</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Key market indicators for this thesis
        indicators = t.get("indicators", {})
        market_indicators = indicators.get("market", [])

        if market_indicators:
            cols = st.columns(min(len(market_indicators), 6))
            for i, ind in enumerate(market_indicators[:6]):
                ticker = ind["ticker"]
                snap = dm.get_market_snapshot(ticker)
                with cols[i]:
                    if snap and snap.get("price"):
                        change = snap.get("change_1d_pct")
                        st.metric(
                            ind.get("label", ticker),
                            f"${snap['price']:.2f}" if snap["price"] < 10000 else f"{snap['price']:,.0f}",
                            f"{change:+.2f}%" if change else "N/A",
                        )
                    else:
                        st.metric(ind.get("label", ticker), "N/A")

        # News count for this thesis
        thesis_news = dm.get_thesis_news(tid)
        if thesis_news:
            st.caption(f"{len(thesis_news)} news articles in last fetch")

    st.markdown("")  # spacer

# -- FRED summary strip --
st.divider()
st.markdown("### Macro Indicators")

fred_series = [v for v in dm.fred_data.values() if v.get("value") is not None]
if fred_series:
    cols = st.columns(min(len(fred_series), 5))
    for i, data in enumerate(fred_series[:5]):
        with cols[i]:
            change_str = ""
            if data.get("change_pct") is not None:
                change_str = f"{data['change_pct']:+.2f}%"
            st.metric(
                data.get("label", data["series_id"]),
                f"{data['value']:,.2f}",
                change_str or None,
            )
    if len(fred_series) > 5:
        with st.expander(f"See all {len(fred_series)} FRED series"):
            for data in fred_series[5:]:
                st.write(f"**{data.get('label', data['series_id'])}**: "
                         f"{data['value']:,.2f}")
