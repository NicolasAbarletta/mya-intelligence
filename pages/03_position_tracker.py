# -*- coding: utf-8 -*-
"""Page 3: Position Tracker.

Live P&L proxy, options greeks, private position cards.
"""

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.data.data_manager import DataManager
from src.utils.helpers import load_watchlists, fmt_number, fmt_pct

st.set_page_config(page_title="Position Tracker | MYA", page_icon="M", layout="wide")

st.markdown("## Position Tracker")
st.caption("Live market positions, options, and private allocations")

dm: DataManager = st.session_state.get("data_manager")
if dm is None or not dm.market_data:
    st.info("No data loaded. Return to the home page and run the pipeline.")
    st.stop()

wl = load_watchlists()

# -- Options positions --
st.markdown("### Options Positions")

options_positions = wl.get("options_positions", [])
if options_positions:
    for pos in options_positions:
        underlying = pos["underlying"]
        snap = dm.get_market_snapshot(underlying)

        with st.container():
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.markdown(f"**{pos['name']}** ({underlying})")
                st.caption(f"Type: {pos.get('type', 'N/A')}")
            with col2:
                if snap and snap.get("price"):
                    st.metric("Underlying", f"${snap['price']:.2f}",
                              fmt_pct(snap.get("change_1d_pct")))
                else:
                    st.metric("Underlying", "N/A")
            with col3:
                if snap:
                    iv = snap.get("implied_vol")
                    rv = snap.get("realized_vol_20d")
                    st.metric("IV", f"{iv:.1f}%" if iv else "N/A")
                    st.caption(f"RV(20d): {rv:.1f}%" if rv else "")

            # Options chain data (if available)
            opts = dm.market.fetch_options_snapshot(underlying) if pos.get("monitor") else None
            if opts:
                st.markdown("**Nearest Expiry Options (ATM)**")
                oc1, oc2 = st.columns(2)
                with oc1:
                    call = opts.get("atm_call", {})
                    st.markdown(f"**Call** @ ${call.get('strike', 0):.2f}")
                    st.caption(
                        f"Last: ${call.get('last_price', 0):.2f} | "
                        f"IV: {call.get('implied_vol', 0)*100:.1f}% | "
                        f"Vol: {call.get('volume', 0):,} | "
                        f"OI: {call.get('open_interest', 0):,}"
                    )
                with oc2:
                    put = opts.get("atm_put", {})
                    if put:
                        st.markdown(f"**Put** @ ${put.get('strike', 0):.2f}")
                        st.caption(
                            f"Last: ${put.get('last_price', 0):.2f} | "
                            f"IV: {put.get('implied_vol', 0)*100:.1f}% | "
                            f"Vol: {put.get('volume', 0):,} | "
                            f"OI: {put.get('open_interest', 0):,}"
                        )
            st.divider()
else:
    st.caption("No options positions configured.")

# -- Public equities by group --
st.markdown("### Equity Watchlist")

equities = wl.get("equities", {})
for group_name, tickers in equities.items():
    with st.expander(f"**{group_name.replace('_', ' ').title()}** ({len(tickers)} tickers)"):
        cols = st.columns(min(len(tickers), 5))
        for i, ticker in enumerate(tickers[:10]):
            snap = dm.get_market_snapshot(ticker)
            with cols[i % 5]:
                if snap and snap.get("price"):
                    st.metric(
                        ticker,
                        f"${snap['price']:.2f}" if snap["price"] < 10000 else f"{snap['price']:,.0f}",
                        fmt_pct(snap.get("change_1d_pct")),
                    )
                else:
                    st.metric(ticker, "N/A")

# -- Private positions --
st.divider()
st.markdown("### Private Positions")

private = wl.get("private_positions", [])
if private:
    for pos in private:
        with st.container():
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{pos['name']}**")
                st.caption(f"Thesis: {pos.get('thesis', 'N/A')}")
                notes = pos.get("notes", "")
                if notes:
                    st.markdown(f"_{notes}_")
            with col2:
                val = pos.get("last_known_valuation")
                if val:
                    st.metric("Valuation", f"${val/1e9:.0f}B")
                comm = pos.get("commitment")
                if comm:
                    st.metric("Commitment", f"${comm/1e6:.1f}M")
            st.divider()
else:
    st.caption("No private positions configured.")
