# -*- coding: utf-8 -*-
"""SQLite storage layer for MYA Intelligence.

Tables:
  - market_snapshots: price/vol data per ticker per fetch
  - fred_snapshots: FRED series values per fetch
  - news_articles: raw news articles with thesis tags
  - signals: detected signals (rule-based + LLM)
  - alerts: routed alerts with severity
  - daily_briefings: Claude-generated daily summaries
  - data_freshness: tracks last successful fetch per source
"""

import sqlite3
import threading
import json
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "mya_intelligence.db"

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Return a thread-local SQLite connection, validated."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = None

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    _local.conn = conn
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_conn()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS market_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        ticker TEXT NOT NULL,
        price REAL,
        change_1d_pct REAL,
        change_5d_pct REAL,
        change_20d_pct REAL,
        high_52w REAL,
        low_52w REAL,
        pct_from_52w_high REAL,
        implied_vol REAL,
        realized_vol_20d REAL,
        volume REAL,
        volume_avg_20d REAL,
        volume_trend REAL,
        extra_json TEXT
    );

    CREATE TABLE IF NOT EXISTS fred_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        series_id TEXT NOT NULL,
        value REAL,
        previous_value REAL,
        change_abs REAL,
        change_pct REAL,
        observation_date TEXT
    );

    CREATE TABLE IF NOT EXISTS news_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        source TEXT,
        title TEXT,
        description TEXT,
        url TEXT,
        published_at TEXT,
        keyword TEXT,
        thesis_tags TEXT,
        sentiment REAL
    );

    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        run_id TEXT,
        thesis TEXT NOT NULL,
        signal_type TEXT NOT NULL,
        indicator TEXT,
        trigger_condition TEXT,
        raw_value REAL,
        threshold REAL,
        severity TEXT NOT NULL DEFAULT 'low',
        description TEXT,
        synthesis TEXT,
        signal_strength REAL,
        is_active INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        signal_id INTEGER REFERENCES signals(id),
        thesis TEXT NOT NULL,
        severity TEXT NOT NULL,
        channel TEXT NOT NULL,
        title TEXT,
        body TEXT,
        acknowledged INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS daily_briefings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        run_id TEXT,
        date TEXT NOT NULL,
        content TEXT NOT NULL,
        thesis_summaries TEXT,
        cross_thesis_flags TEXT,
        suggested_actions TEXT
    );

    CREATE TABLE IF NOT EXISTS data_freshness (
        source TEXT PRIMARY KEY,
        last_success_ts TEXT NOT NULL,
        last_run_id TEXT,
        record_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'ok',
        error_message TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_market_run ON market_snapshots(run_id);
    CREATE INDEX IF NOT EXISTS idx_market_ticker ON market_snapshots(ticker);
    CREATE INDEX IF NOT EXISTS idx_fred_run ON fred_snapshots(run_id);
    CREATE INDEX IF NOT EXISTS idx_fred_series ON fred_snapshots(series_id);
    CREATE INDEX IF NOT EXISTS idx_news_run ON news_articles(run_id);
    CREATE INDEX IF NOT EXISTS idx_signals_thesis ON signals(thesis);
    CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts);
    CREATE INDEX IF NOT EXISTS idx_alerts_thesis ON alerts(thesis);
    CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);
    """)
    conn.commit()


def update_freshness(source: str, run_id: str, count: int,
                     status: str = "ok", error: str = None):
    """Update data freshness tracker."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO data_freshness (source, last_success_ts, last_run_id,
                                    record_count, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source) DO UPDATE SET
            last_success_ts = excluded.last_success_ts,
            last_run_id = excluded.last_run_id,
            record_count = excluded.record_count,
            status = excluded.status,
            error_message = excluded.error_message
    """, (source, now, run_id, count, status, error))
    conn.commit()


def get_freshness(source: str) -> dict | None:
    """Get last freshness record for a data source."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM data_freshness WHERE source = ?", (source,)
    ).fetchone()
    return dict(row) if row else None


def get_all_freshness() -> list[dict]:
    """Get freshness status for all data sources."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM data_freshness ORDER BY last_success_ts DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def insert_market_snapshot(run_id: str, ticker: str, data: dict):
    """Insert a single market snapshot row."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    extra = data.get("extra", {})
    conn.execute("""
        INSERT INTO market_snapshots
        (run_id, ts, ticker, price, change_1d_pct, change_5d_pct,
         change_20d_pct, high_52w, low_52w, pct_from_52w_high,
         implied_vol, realized_vol_20d, volume, volume_avg_20d,
         volume_trend, extra_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        run_id, now, ticker,
        data.get("price"), data.get("change_1d_pct"),
        data.get("change_5d_pct"), data.get("change_20d_pct"),
        data.get("high_52w"), data.get("low_52w"),
        data.get("pct_from_52w_high"),
        data.get("implied_vol"), data.get("realized_vol_20d"),
        data.get("volume"), data.get("volume_avg_20d"),
        data.get("volume_trend"),
        json.dumps(extra) if extra else None
    ))
    conn.commit()


def insert_fred_snapshot(run_id: str, series_id: str, data: dict):
    """Insert a single FRED snapshot row."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO fred_snapshots
        (run_id, ts, series_id, value, previous_value,
         change_abs, change_pct, observation_date)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        run_id, now, series_id,
        data.get("value"), data.get("previous_value"),
        data.get("change_abs"), data.get("change_pct"),
        data.get("observation_date")
    ))
    conn.commit()


def insert_news_article(run_id: str, article: dict):
    """Insert a single news article row."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    tags = article.get("thesis_tags", [])
    conn.execute("""
        INSERT INTO news_articles
        (run_id, ts, source, title, description, url,
         published_at, keyword, thesis_tags, sentiment)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        run_id, now,
        article.get("source"), article.get("title"),
        article.get("description"), article.get("url"),
        article.get("published_at"), article.get("keyword"),
        json.dumps(tags) if tags else None,
        article.get("sentiment")
    ))
    conn.commit()


def insert_signal(run_id: str, signal: dict) -> int:
    """Insert a signal and return its ID."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute("""
        INSERT INTO signals
        (ts, run_id, thesis, signal_type, indicator, trigger_condition,
         raw_value, threshold, severity, description, synthesis,
         signal_strength, is_active)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now, run_id,
        signal.get("thesis"), signal.get("signal_type", "rule"),
        signal.get("indicator"), signal.get("trigger_condition"),
        signal.get("raw_value"), signal.get("threshold"),
        signal.get("severity", "low"), signal.get("description"),
        signal.get("synthesis"), signal.get("signal_strength"),
        1
    ))
    conn.commit()
    return cursor.lastrowid


def insert_alert(signal_id: int, thesis: str, severity: str,
                 channel: str, title: str, body: str):
    """Insert an alert."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO alerts (ts, signal_id, thesis, severity, channel, title, body)
        VALUES (?,?,?,?,?,?,?)
    """, (now, signal_id, thesis, severity, channel, title, body))
    conn.commit()


def get_recent_signals(thesis: str = None, days: int = 7,
                       limit: int = 100) -> list[dict]:
    """Fetch recent signals, optionally filtered by thesis."""
    conn = get_conn()
    query = "SELECT * FROM signals WHERE ts >= datetime('now', ?)"
    params: list = [f"-{days} days"]
    if thesis:
        query += " AND thesis = ?"
        params.append(thesis)
    query += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_recent_alerts(severity: str = None, days: int = 7,
                      limit: int = 50) -> list[dict]:
    """Fetch recent alerts."""
    conn = get_conn()
    query = "SELECT * FROM alerts WHERE ts >= datetime('now', ?)"
    params: list = [f"-{days} days"]
    if severity:
        query += " AND severity = ?"
        params.append(severity)
    query += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_latest_market_data(run_id: str = None) -> list[dict]:
    """Get latest market snapshots for a run (or most recent run)."""
    conn = get_conn()
    if run_id is None:
        row = conn.execute(
            "SELECT run_id FROM market_snapshots ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if not row:
            return []
        run_id = row["run_id"]
    rows = conn.execute(
        "SELECT * FROM market_snapshots WHERE run_id = ? ORDER BY ticker",
        (run_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_fred_data(run_id: str = None) -> list[dict]:
    """Get latest FRED snapshots for a run (or most recent run)."""
    conn = get_conn()
    if run_id is None:
        row = conn.execute(
            "SELECT run_id FROM fred_snapshots ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if not row:
            return []
        run_id = row["run_id"]
    rows = conn.execute(
        "SELECT * FROM fred_snapshots WHERE run_id = ? ORDER BY series_id",
        (run_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_news(run_id: str = None, limit: int = 50) -> list[dict]:
    """Get latest news articles."""
    conn = get_conn()
    if run_id:
        rows = conn.execute(
            "SELECT * FROM news_articles WHERE run_id = ? ORDER BY published_at DESC LIMIT ?",
            (run_id, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM news_articles ORDER BY published_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return [dict(r) for r in rows]
