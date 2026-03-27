# -*- coding: utf-8 -*-
"""Market data client using yfinance for MYA Intelligence.

Fetches prices, options chains, derived metrics, and vol data.
"""

import logging
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# Module-level cache for history DataFrames (used by analysis modules)
hist_cache: dict[str, pd.DataFrame] = {}


class MarketClient:
    """yfinance-based market data client."""

    def __init__(self):
        self._spy_returns: pd.Series | None = None

    def fetch_ticker(self, symbol: str, period: str = "1y") -> dict:
        """Fetch snapshot data for a single ticker.

        Returns dict with price, changes, vol, 52w range, volume metrics, etc.
        Also caches the raw DataFrame in hist_cache for downstream analysis.
        """
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period=period, auto_adjust=True)

            if hist.empty:
                log.warning("%s: no price data", symbol)
                return self._empty(symbol, "No data")

            # Cache for analysis modules
            hist_cache[symbol] = hist

            close = hist["Close"]
            latest_price = float(close.iloc[-1])

            # Returns
            change_1d = self._pct_change(close, 1)
            change_5d = self._pct_change(close, 5)
            change_20d = self._pct_change(close, 20)

            # 52-week range
            high_52w = float(close.max())
            low_52w = float(close.min())
            pct_from_high = ((latest_price - high_52w) / high_52w * 100) if high_52w else None

            # Volume
            vol = hist["Volume"]
            volume = float(vol.iloc[-1]) if not vol.empty else None
            vol_avg_20d = float(vol.tail(20).mean()) if len(vol) >= 20 else None
            volume_trend = self._volume_trend(vol) if len(vol) >= 20 else None

            # Realized vol (20-day annualized)
            log_ret = np.log(close / close.shift(1)).dropna()
            rv_20d = float(log_ret.tail(20).std() * np.sqrt(252) * 100) if len(log_ret) >= 20 else None

            # Implied vol (from options if available)
            iv = self._get_implied_vol(tk, symbol)

            # SPY correlation
            spy_corr = self._spy_correlation(log_ret, 60)

            # Return skewness
            skewness = float(log_ret.tail(60).skew()) if len(log_ret) >= 60 else None

            return {
                "ticker": symbol,
                "price": latest_price,
                "change_1d_pct": change_1d,
                "change_5d_pct": change_5d,
                "change_20d_pct": change_20d,
                "high_52w": high_52w,
                "low_52w": low_52w,
                "pct_from_52w_high": pct_from_high,
                "implied_vol": iv,
                "realized_vol_20d": rv_20d,
                "volume": volume,
                "volume_avg_20d": vol_avg_20d,
                "volume_trend": volume_trend,
                "skewness_60d": skewness,
                "spy_correlation_60d": spy_corr,
                "extra": {
                    "return_20d": change_20d,
                    "drawdown_from_peak": pct_from_high,
                },
            }

        except Exception as e:
            log.warning("%s: fetch failed -- %s", symbol, e)
            return self._empty(symbol, str(e))

    def fetch_multiple(self, symbols: list[str]) -> dict[str, dict]:
        """Fetch snapshots for multiple tickers."""
        # Pre-fetch SPY for correlation calculation
        if "SPY" not in hist_cache:
            try:
                spy = yf.Ticker("SPY").history(period="1y", auto_adjust=True)
                if not spy.empty:
                    hist_cache["SPY"] = spy
                    self._spy_returns = np.log(
                        spy["Close"] / spy["Close"].shift(1)
                    ).dropna()
            except Exception:
                pass

        results = {}
        for sym in symbols:
            results[sym] = self.fetch_ticker(sym)
        return results

    def fetch_options_snapshot(self, symbol: str) -> dict | None:
        """Fetch options chain summary for a ticker.

        Returns nearest expiry ATM call/put greeks if available.
        """
        try:
            tk = yf.Ticker(symbol)
            expirations = tk.options
            if not expirations:
                return None

            # Use nearest expiry
            exp = expirations[0]
            chain = tk.option_chain(exp)

            if chain.calls.empty:
                return None

            # Find ATM call
            price = float(tk.history(period="1d")["Close"].iloc[-1])
            calls = chain.calls
            calls = calls.assign(
                dist=abs(calls["strike"] - price)
            )
            atm_call = calls.loc[calls["dist"].idxmin()]

            # Find ATM put
            puts = chain.puts
            atm_put = None
            if not puts.empty:
                puts = puts.assign(dist=abs(puts["strike"] - price))
                atm_put = puts.loc[puts["dist"].idxmin()]

            result = {
                "ticker": symbol,
                "expiration": exp,
                "underlying_price": price,
                "atm_call": {
                    "strike": float(atm_call["strike"]),
                    "last_price": float(atm_call.get("lastPrice", 0)),
                    "implied_vol": float(atm_call.get("impliedVolatility", 0)),
                    "volume": int(atm_call.get("volume", 0) or 0),
                    "open_interest": int(atm_call.get("openInterest", 0) or 0),
                },
            }

            if atm_put is not None:
                result["atm_put"] = {
                    "strike": float(atm_put["strike"]),
                    "last_price": float(atm_put.get("lastPrice", 0)),
                    "implied_vol": float(atm_put.get("impliedVolatility", 0)),
                    "volume": int(atm_put.get("volume", 0) or 0),
                    "open_interest": int(atm_put.get("openInterest", 0) or 0),
                }

            return result

        except Exception as e:
            log.warning("%s: options fetch failed -- %s", symbol, e)
            return None

    def _spy_correlation(self, log_ret: pd.Series, window: int) -> float | None:
        """Compute trailing correlation with SPY."""
        if self._spy_returns is None:
            return None
        try:
            aligned = pd.concat([log_ret, self._spy_returns], axis=1).dropna()
            if len(aligned) < window:
                return None
            tail = aligned.tail(window)
            corr = float(tail.iloc[:, 0].corr(tail.iloc[:, 1]))
            return corr
        except Exception:
            return None

    @staticmethod
    def _pct_change(series: pd.Series, periods: int) -> float | None:
        if len(series) <= periods:
            return None
        old = float(series.iloc[-(periods + 1)])
        new = float(series.iloc[-1])
        if old == 0:
            return None
        return (new - old) / abs(old) * 100

    @staticmethod
    def _volume_trend(vol: pd.Series) -> float | None:
        """Compute volume trend as normalized slope of last 20 days."""
        try:
            recent = vol.tail(20).values.astype(float)
            x = np.arange(len(recent))
            if np.std(recent) == 0:
                return 0.0
            slope = np.polyfit(x, recent, 1)[0]
            return float(slope / np.mean(recent)) if np.mean(recent) != 0 else 0.0
        except Exception:
            return None

    @staticmethod
    def _get_implied_vol(tk, symbol: str) -> float | None:
        """Try to extract ATM implied vol from the nearest options chain."""
        try:
            exps = tk.options
            if not exps:
                return None
            chain = tk.option_chain(exps[0])
            if chain.calls.empty:
                return None
            price = float(tk.history(period="1d")["Close"].iloc[-1])
            calls = chain.calls
            calls = calls.assign(dist=abs(calls["strike"] - price))
            atm = calls.loc[calls["dist"].idxmin()]
            iv = atm.get("impliedVolatility")
            return float(iv * 100) if iv else None
        except Exception:
            return None

    @staticmethod
    def _empty(symbol: str, reason: str) -> dict:
        return {
            "ticker": symbol,
            "price": None,
            "change_1d_pct": None,
            "change_5d_pct": None,
            "change_20d_pct": None,
            "high_52w": None,
            "low_52w": None,
            "pct_from_52w_high": None,
            "implied_vol": None,
            "realized_vol_20d": None,
            "volume": None,
            "volume_avg_20d": None,
            "volume_trend": None,
            "error": reason,
        }
