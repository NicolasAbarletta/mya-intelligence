# -*- coding: utf-8 -*-
from __future__ import annotations
"""Signal Detector for MYA Intelligence.

Layer 1: Rule-based trigger evaluation against thesis-defined thresholds.
Layer 2: LLM synthesis (Phase 4) -- accumulates signals and synthesizes via Claude.

Operates on ThesisSnapshot objects produced by ThesisEngine.
"""

import logging
from datetime import datetime, timezone

from src.analysis.thesis_engine import ThesisSnapshot
from src.storage.db import insert_signal, insert_alert
from src.utils.helpers import load_theses, load_alerts_config

log = logging.getLogger(__name__)


class SignalDetector:
    """Rule-based signal detection engine.

    Evaluates each thesis snapshot against its configured triggers
    and logs detected signals to SQLite.
    """

    def __init__(self):
        self.theses = load_theses()
        self.alerts_config = load_alerts_config()

    def evaluate_all(self, snapshots: dict[str, ThesisSnapshot],
                     run_id: str) -> list[dict]:
        """Evaluate all thesis snapshots for signals.

        Returns list of detected signal dicts.
        """
        all_signals = []
        for tid, snap in snapshots.items():
            signals = self._evaluate_thesis(tid, snap, run_id)
            all_signals.extend(signals)

        log.info("Signal detection: %d signals across %d theses",
                 len(all_signals), len(snapshots))
        return all_signals

    def _evaluate_thesis(self, thesis_id: str, snap: ThesisSnapshot,
                         run_id: str) -> list[dict]:
        """Evaluate a single thesis for signals."""
        signals = []
        ci = snap.conviction_inputs

        # Run thesis-specific rule checks
        if thesis_id == "energy_disruption":
            signals.extend(self._check_energy(ci, run_id))
        elif thesis_id == "defense_tech":
            signals.extend(self._check_defense(ci, run_id))
        elif thesis_id == "ai_infrastructure":
            signals.extend(self._check_ai_infra(ci, run_id))
        elif thesis_id == "macro_regime":
            signals.extend(self._check_macro(ci, run_id))

        # Universal checks (apply to all theses)
        signals.extend(self._check_news_cluster(thesis_id, snap, run_id))
        signals.extend(self._check_vol_signals(thesis_id, snap, run_id))
        signals.extend(self._check_big_movers(thesis_id, snap, run_id))

        # Store signals and route alerts
        for sig in signals:
            sig["thesis"] = thesis_id
            signal_id = insert_signal(run_id, sig)
            self._route_alert(signal_id, sig)

        return signals

    # ------------------------------------------------------------------ #
    #  Thesis-specific rule checks                                        #
    # ------------------------------------------------------------------ #

    def _check_energy(self, ci: dict, run_id: str) -> list[dict]:
        """Energy / Geopolitical Disruption triggers."""
        signals = []

        # WTI daily move > 3%
        wti_1d = ci.get("wti_1d_pct")
        if wti_1d is not None and abs(wti_1d) > 3:
            signals.append({
                "signal_type": "rule",
                "indicator": "CL=F",
                "trigger_condition": "wti_daily_move_pct > 3",
                "raw_value": wti_1d,
                "threshold": 3.0,
                "severity": "high",
                "description": (
                    f"WTI crude moved {wti_1d:+.1f}% today. "
                    f"Current price: ${ci.get('wti_price', 0):.2f}. "
                    f"This exceeds the 3% threshold for energy disruption signals."
                ),
            })

        # WTI 5-day momentum > 5% (strong directional move)
        wti_5d = ci.get("wti_5d_pct")
        if wti_5d is not None and abs(wti_5d) > 5:
            signals.append({
                "signal_type": "rule",
                "indicator": "CL=F",
                "trigger_condition": "wti_5d_momentum > 5",
                "raw_value": wti_5d,
                "threshold": 5.0,
                "severity": "medium",
                "description": (
                    f"WTI 5-day momentum: {wti_5d:+.1f}%. "
                    f"Sustained directional move in crude oil."
                ),
            })

        # HY spreads widening
        hy_change = ci.get("hy_spread_change")
        hy_level = ci.get("hy_spread")
        if hy_change is not None and hy_change > 25:
            signals.append({
                "signal_type": "rule",
                "indicator": "BAMLH0A0HYM2",
                "trigger_condition": "hy_spread_weekly_change > 25",
                "raw_value": hy_change,
                "threshold": 25.0,
                "severity": "high",
                "description": (
                    f"HY OAS spread widened {hy_change:+.0f}bps. "
                    f"Current level: {hy_level:.0f}bps. "
                    f"Credit stress signal consistent with risk-off environment."
                ),
            })

        # Brent-WTI spread dislocation
        bwt_spread = ci.get("brent_wti_spread")
        if bwt_spread is not None and abs(bwt_spread) > 5:
            signals.append({
                "signal_type": "rule",
                "indicator": "BZ=F vs CL=F",
                "trigger_condition": "brent_wti_spread > 5",
                "raw_value": bwt_spread,
                "threshold": 5.0,
                "severity": "medium",
                "description": (
                    f"Brent-WTI spread: ${bwt_spread:.2f}. "
                    f"Wide spread suggests geopolitical supply premium."
                ),
            })

        return signals

    def _check_defense(self, ci: dict, run_id: str) -> list[dict]:
        """Defense Tech Displacement triggers."""
        signals = []

        # Legacy underperformance vs sector
        legacy_gap = ci.get("legacy_vs_sector_5d")
        if legacy_gap is not None and legacy_gap < -2:
            signals.append({
                "signal_type": "rule",
                "indicator": "Legacy primes vs ITA",
                "trigger_condition": "legacy_underperformance_5d > 2",
                "raw_value": legacy_gap,
                "threshold": -2.0,
                "severity": "medium",
                "description": (
                    f"Legacy defense primes underperforming sector by "
                    f"{abs(legacy_gap):.1f}% over 5 days. "
                    f"Potential displacement signal."
                ),
            })

        # PLTR breakout (proxy for defense tech momentum)
        pltr_5d = ci.get("pltr_5d_pct")
        if pltr_5d is not None and pltr_5d > 5:
            signals.append({
                "signal_type": "rule",
                "indicator": "PLTR",
                "trigger_condition": "pltr_breakout_5d > 5",
                "raw_value": pltr_5d,
                "threshold": 5.0,
                "severity": "medium",
                "description": (
                    f"Palantir up {pltr_5d:+.1f}% over 5 days. "
                    f"Defense-tech sentiment momentum."
                ),
            })

        return signals

    def _check_ai_infra(self, ci: dict, run_id: str) -> list[dict]:
        """AI Infrastructure Bottleneck triggers."""
        signals = []

        # Power utility composite breakout
        power_5d = ci.get("power_composite_5d")
        if power_5d is not None and power_5d > 5:
            signals.append({
                "signal_type": "rule",
                "indicator": "VST/CEG/NRG composite",
                "trigger_condition": "power_utility_weekly_move > 5",
                "raw_value": power_5d,
                "threshold": 5.0,
                "severity": "high",
                "description": (
                    f"Power utility composite up {power_5d:+.1f}% over 5 days. "
                    f"Data center power demand thesis accelerating."
                ),
            })

        # Semi composite divergence from power
        semi_5d = ci.get("semi_composite_5d")
        if power_5d and semi_5d and abs(power_5d - semi_5d) > 8:
            leader = "power" if power_5d > semi_5d else "semis"
            signals.append({
                "signal_type": "rule",
                "indicator": "Power vs Semi divergence",
                "trigger_condition": "power_semi_divergence > 8",
                "raw_value": power_5d - semi_5d,
                "threshold": 8.0,
                "severity": "medium",
                "description": (
                    f"Power-semi divergence: power {power_5d:+.1f}% vs "
                    f"semis {semi_5d:+.1f}% (5d). {leader} leading -- "
                    f"watch for convergence trade."
                ),
            })

        # Natural gas spike (power input cost)
        ng_change = ci.get("natgas_change")
        if ng_change is not None and abs(ng_change) > 10:
            signals.append({
                "signal_type": "rule",
                "indicator": "DHHNGSP",
                "trigger_condition": "natgas_change > 10pct",
                "raw_value": ng_change,
                "threshold": 10.0,
                "severity": "medium",
                "description": (
                    f"Natural gas price change: {ng_change:+.1f}%. "
                    f"Significant move in data center power input costs."
                ),
            })

        return signals

    def _check_macro(self, ci: dict, run_id: str) -> list[dict]:
        """Macro Regime Monitor triggers."""
        signals = []

        # VIX regime transitions
        vix = ci.get("vix_level")
        if vix is not None:
            if vix > 30:
                signals.append({
                    "signal_type": "rule",
                    "indicator": "^VIX",
                    "trigger_condition": "vix_above_30",
                    "raw_value": vix,
                    "threshold": 30.0,
                    "severity": "critical",
                    "description": (
                        f"VIX at {vix:.1f} -- fear regime. "
                        f"Elevated risk across all portfolios."
                    ),
                })
            elif vix < 13:
                signals.append({
                    "signal_type": "rule",
                    "indicator": "^VIX",
                    "trigger_condition": "vix_below_13",
                    "raw_value": vix,
                    "threshold": 13.0,
                    "severity": "high",
                    "description": (
                        f"VIX at {vix:.1f} -- complacency zone. "
                        f"Historically precedes vol spikes. "
                        f"Cheap to hedge here."
                    ),
                })

        # VIX 5-day spike
        vix_5d = ci.get("vix_5d_change")
        if vix_5d is not None and vix_5d > 20:
            signals.append({
                "signal_type": "rule",
                "indicator": "^VIX",
                "trigger_condition": "vix_5d_spike > 20pct",
                "raw_value": vix_5d,
                "threshold": 20.0,
                "severity": "high",
                "description": (
                    f"VIX spiked {vix_5d:+.1f}% over 5 days. "
                    f"Rapid vol expansion -- check all positions."
                ),
            })

        # Yield curve inversion / disinversion
        yc = ci.get("yield_curve")
        yc_change = ci.get("yield_curve_change")
        if yc is not None:
            if yc < 0:
                signals.append({
                    "signal_type": "rule",
                    "indicator": "T10Y2Y",
                    "trigger_condition": "yield_curve_inverted",
                    "raw_value": yc,
                    "threshold": 0.0,
                    "severity": "critical",
                    "description": (
                        f"Yield curve inverted at {yc:+.2f}%. "
                        f"Historical recession indicator. "
                        f"Change: {yc_change:+.2f}%" if yc_change else
                        f"Yield curve inverted at {yc:+.2f}%."
                    ),
                })

        # HY spreads stress levels
        hy = ci.get("hy_spread")
        if hy is not None:
            if hy > 500:
                signals.append({
                    "signal_type": "rule",
                    "indicator": "BAMLH0A0HYM2",
                    "trigger_condition": "hy_spread_gt_500",
                    "raw_value": hy,
                    "threshold": 500.0,
                    "severity": "critical",
                    "description": (
                        f"HY OAS at {hy:.0f}bps -- credit stress zone. "
                        f"Affects all private credit and levered positions."
                    ),
                })
            elif hy < 300:
                signals.append({
                    "signal_type": "rule",
                    "indicator": "BAMLH0A0HYM2",
                    "trigger_condition": "hy_spread_lt_300",
                    "raw_value": hy,
                    "threshold": 300.0,
                    "severity": "high",
                    "description": (
                        f"HY OAS at {hy:.0f}bps -- complacency zone. "
                        f"Credit markets pricing minimal risk. "
                        f"Historically tight -- watch for snapback."
                    ),
                })

        # Consumer sentiment drop
        sent_change = ci.get("sentiment_change")
        if sent_change is not None and sent_change < -5:
            signals.append({
                "signal_type": "rule",
                "indicator": "UMCSENT",
                "trigger_condition": "consumer_sentiment_drop_gt_5",
                "raw_value": sent_change,
                "threshold": -5.0,
                "severity": "high",
                "description": (
                    f"Consumer sentiment dropped {sent_change:+.1f} pts. "
                    f"Current level: {ci.get('consumer_sentiment', 'N/A')}. "
                    f"Demand deterioration risk."
                ),
            })

        # Dollar strength/weakness
        dollar_5d = ci.get("dollar_5d_pct")
        if dollar_5d is not None and abs(dollar_5d) > 2:
            direction = "strengthening" if dollar_5d > 0 else "weakening"
            signals.append({
                "signal_type": "rule",
                "indicator": "UUP",
                "trigger_condition": "dollar_5d_move > 2pct",
                "raw_value": dollar_5d,
                "threshold": 2.0,
                "severity": "medium",
                "description": (
                    f"Dollar {direction}: {dollar_5d:+.1f}% over 5 days. "
                    f"Impacts EM, commodities, and international positions."
                ),
            })

        return signals

    # ------------------------------------------------------------------ #
    #  Universal checks (all theses)                                      #
    # ------------------------------------------------------------------ #

    def _check_news_cluster(self, thesis_id: str, snap: ThesisSnapshot,
                            run_id: str) -> list[dict]:
        """Check for news clustering (3+ articles in 24h)."""
        signals = []
        if snap.news_cluster_24h >= 3:
            severity = "high" if snap.news_cluster_24h >= 5 else "medium"
            headlines = "; ".join(snap.news_headlines[:3])
            signals.append({
                "signal_type": "rule",
                "indicator": "news_cluster",
                "trigger_condition": "news_cluster_24h >= 3",
                "raw_value": snap.news_cluster_24h,
                "threshold": 3.0,
                "severity": severity,
                "description": (
                    f"{snap.news_cluster_24h} news articles in last 24h for "
                    f"{snap.name}. Headlines: {headlines}"
                ),
            })
        return signals

    def _check_vol_signals(self, thesis_id: str, snap: ThesisSnapshot,
                           run_id: str) -> list[dict]:
        """Check for significant IV-RV divergences."""
        signals = []
        vol_sigs = snap.conviction_inputs.get("vol_signals", [])
        for vs in vol_sigs:
            direction = "overpriced" if vs["gap"] > 0 else "underpriced"
            signals.append({
                "signal_type": "rule",
                "indicator": f"{vs['ticker']} vol",
                "trigger_condition": "iv_rv_gap > 10pct",
                "raw_value": vs["gap"],
                "threshold": 10.0,
                "severity": "medium",
                "description": (
                    f"{vs['ticker']} vol is {direction}: "
                    f"IV={vs['iv']:.1f}% vs RV={vs['rv']:.1f}% "
                    f"(gap: {vs['gap']:+.1f}%). "
                    f"{'Options cheap relative to realized moves.' if vs['gap'] < 0 else 'Options expensive relative to realized moves.'}"
                ),
            })
        return signals

    def _check_big_movers(self, thesis_id: str, snap: ThesisSnapshot,
                          run_id: str) -> list[dict]:
        """Check for large daily moves in thesis-relevant tickers."""
        signals = []
        movers = snap.conviction_inputs.get("biggest_movers", [])
        for m in movers:
            if abs(m["change"]) > 5:  # Only flag really big moves
                signals.append({
                    "signal_type": "rule",
                    "indicator": m["ticker"],
                    "trigger_condition": "daily_move > 5pct",
                    "raw_value": m["change"],
                    "threshold": 5.0,
                    "severity": "high",
                    "description": (
                        f"{m['label']} ({m['ticker']}) moved {m['change']:+.1f}% today. "
                        f"Significant for {snap.name} thesis."
                    ),
                })
        return signals

    # ------------------------------------------------------------------ #
    #  Alert routing                                                      #
    # ------------------------------------------------------------------ #

    def _route_alert(self, signal_id: int, signal: dict):
        """Route a signal to the appropriate alert channels."""
        severity = signal.get("severity", "low")
        config = self.alerts_config.get("severity_levels", {})
        level_config = config.get(severity, {})
        channels = level_config.get("channels", ["log"])

        for channel in channels:
            insert_alert(
                signal_id=signal_id,
                thesis=signal.get("thesis", "unknown"),
                severity=severity,
                channel=channel,
                title=signal.get("trigger_condition", "Signal"),
                body=signal.get("description", ""),
            )
