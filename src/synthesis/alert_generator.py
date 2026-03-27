# -*- coding: utf-8 -*-
from __future__ import annotations
"""Claude-powered alert synthesis for MYA Intelligence.

Layer 2 of signal detection: takes accumulated rule-based signals
per thesis, recent news, and market context, and produces structured
Claude-synthesized alerts with conviction deltas, cross-thesis flags,
and suggested actions.
"""

import os
import json
import logging
from datetime import datetime, timezone

from anthropic import Anthropic

from src.analysis.thesis_engine import ThesisEngine, ThesisSnapshot
from src.analysis.macro_monitor import MacroRegime
from src.storage.db import insert_signal, get_conn

log = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"


def _get_client() -> Anthropic | None:
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        log.warning("ANTHROPIC_API_KEY not set -- synthesis unavailable")
        return None
    return Anthropic(api_key=key)


def synthesize_thesis(
    thesis_id: str,
    snapshot: ThesisSnapshot,
    signals: list[dict],
    regime: MacroRegime | None,
    engine: ThesisEngine,
    run_id: str,
) -> dict | None:
    """Run Claude synthesis on a single thesis.

    Takes the thesis snapshot, recent signals, and macro regime context,
    and produces a structured synthesis with conviction delta, summary,
    suggested actions, and cross-thesis flags.

    Returns:
        Structured dict matching the signal detection output schema,
        or None if synthesis fails.
    """
    client = _get_client()
    if not client:
        return None

    # Build context
    thesis_context = engine.format_for_synthesis(snapshot)

    # Format recent signals
    signal_lines = []
    for s in signals:
        signal_lines.append(
            f"  [{s.get('severity', '?').upper()}] {s.get('indicator', '?')}: "
            f"{s.get('description', 'No description')}"
        )
    signals_text = "\n".join(signal_lines) if signal_lines else "  No rule-based signals detected."

    # Regime context
    regime_text = regime.summary if regime else "Regime data unavailable."

    prompt = f"""You are the chief investment analyst for MYA Capital, a family office.
Analyze the following thesis data and produce a structured intelligence synthesis.

MACRO REGIME: {regime_text}

{thesis_context}

RULE-BASED SIGNALS DETECTED:
{signals_text}

Your task:
1. Assess whether these signals collectively strengthen or weaken the thesis
2. Identify what the MARKET IS MISSING -- what non-obvious connection or risk is being underpriced
3. Flag any cross-thesis interactions (e.g., energy moves affecting AI infra power costs)
4. Suggest 2-3 specific, actionable next steps (NOT trade execution -- "investigate", "monitor", "review")

Be direct, analytical, and specific. No fluff. Reference actual numbers from the data.

Respond ONLY with valid JSON in this exact format:
{{
  "signal_strength": <float 0-1>,
  "conviction_delta": "<string like +0.1 or -0.05>",
  "summary": "<2-3 sentence synthesis -- what matters and why>",
  "what_market_is_missing": "<1-2 sentences on the non-obvious angle>",
  "suggested_actions": ["<action 1>", "<action 2>", "<action 3>"],
  "cross_thesis_flags": ["<flag 1>", "<flag 2>"],
  "risk_flag": "<1 sentence on the biggest risk to this thesis right now>"
}}"""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Parse JSON (handle markdown code blocks)
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            text = text.rsplit("```", 1)[0]
        result = json.loads(text)

        # Store as a synthesis signal
        signal_data = {
            "thesis": thesis_id,
            "signal_type": "synthesis",
            "indicator": "claude_synthesis",
            "trigger_condition": f"conviction_delta: {result.get('conviction_delta', '0')}",
            "raw_value": result.get("signal_strength"),
            "threshold": 0.5,
            "severity": _severity_from_delta(result.get("conviction_delta", "0")),
            "description": result.get("summary", ""),
            "synthesis": json.dumps(result),
            "signal_strength": result.get("signal_strength"),
        }
        insert_signal(run_id, signal_data)

        log.info("Synthesis for %s: strength=%.2f, delta=%s",
                 thesis_id, result.get("signal_strength", 0),
                 result.get("conviction_delta", "?"))
        return result

    except json.JSONDecodeError as e:
        log.warning("Failed to parse synthesis JSON for %s: %s", thesis_id, e)
        return None
    except Exception as e:
        log.warning("Synthesis failed for %s: %s", thesis_id, e)
        return None


def synthesize_all(
    snapshots: dict[str, ThesisSnapshot],
    signals_by_thesis: dict[str, list[dict]],
    regime: MacroRegime | None,
    engine: ThesisEngine,
    run_id: str,
) -> dict[str, dict]:
    """Run Claude synthesis on all theses.

    Args:
        snapshots: thesis_id -> ThesisSnapshot
        signals_by_thesis: thesis_id -> list of signal dicts
        regime: current MacroRegime
        engine: ThesisEngine instance (for formatting)
        run_id: current pipeline run ID

    Returns:
        dict mapping thesis_id -> synthesis result dict
    """
    results = {}
    for tid, snap in snapshots.items():
        thesis_signals = signals_by_thesis.get(tid, [])
        result = synthesize_thesis(
            tid, snap, thesis_signals, regime, engine, run_id
        )
        if result:
            results[tid] = result
    return results


def _severity_from_delta(delta_str: str) -> str:
    """Convert a conviction delta string to a severity level."""
    try:
        delta = abs(float(delta_str))
    except (ValueError, TypeError):
        return "medium"

    if delta >= 0.2:
        return "high"
    elif delta >= 0.1:
        return "medium"
    else:
        return "low"
