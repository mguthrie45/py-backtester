"""
accumulators.py
---------------
Interval-agnostic streaming accumulators.
Works for any time frequency: intraday, daily, weekly, irregular.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import Any, Optional

import pandas as pd


class BaseAccumulator(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique key for report.json."""

    @property
    @abstractmethod
    def is_internal(self) -> bool:
        """Whether the accumulator is an intermediate series (not for report)"""

    @abstractmethod
    def update(self, batch: pd.DataFrame) -> None:
        """Consume one batch of data."""

    @abstractmethod
    def result(self) -> Any:
        """Return the final computed metric value."""


# ---------------------------------------------------------------------------
# Helper: Infer annualization factor from time series
# ---------------------------------------------------------------------------


def infer_annualization_factor(
    start: pd.Timestamp, end: pd.Timestamp, num_periods: int
) -> float:
    """
    Infer the annualization multiplier based on the time series.

    Returns sqrt(periods_per_year) for Sharpe-style metrics.

    Examples:
        - Daily data (252 periods/year): sqrt(252) ≈ 15.87
        - Hourly data (252*6.5 periods/year): sqrt(1638) ≈ 40.47
        - Weekly data (52 periods/year): sqrt(52) ≈ 7.21
    """
    if num_periods < 2:
        return 1.0

    total_days = (end - start).days
    if total_days == 0:
        return 1.0

    # Estimate periods per year
    periods_per_day = num_periods / total_days
    periods_per_year = periods_per_day * 365.25

    # For Sharpe/Sortino, we need sqrt(periods_per_year)
    return math.sqrt(max(1.0, periods_per_year))


# ---------------------------------------------------------------------------
# Streaming statistics (O(1) memory)
# ---------------------------------------------------------------------------


class WelfordAccumulator:
    """
    Welford's online algorithm for mean and variance.
    Tracks running stats without storing individual values.
    """

    def __init__(self) -> None:
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def variance(self) -> float:
        return self.m2 / self.count if self.count > 1 else 0.0

    def std(self) -> float:
        return math.sqrt(self.variance())


# ---------------------------------------------------------------------------
# State-observation accumulators
# ---------------------------------------------------------------------------


class PortfolioValueTimeSeriesAccumulator(BaseAccumulator):
    """
    Stores portfolio value at each unique timestamp.

    Memory: O(num_unique_timestamps) - could be seconds, minutes, days, etc.
    For intraday: stores every observation timestamp.
    For daily: stores ~252 values/year.
    """

    name = "_portfolio_value_series"
    is_internal = True

    def __init__(self) -> None:
        # timestamp -> portfolio_value
        # Keep most recent value if multiple rows per timestamp
        self._pv_by_time: dict[pd.Timestamp, float] = {}

    def update(self, batch: pd.DataFrame) -> None:
        b = batch.copy()
        h_shares = pd.to_numeric(b.get("h_num_shares", 0), errors="coerce").fillna(0)
        s_close = pd.to_numeric(b["s_close"], errors="coerce").fillna(0)
        cash = pd.to_numeric(b["cap_cash"], errors="coerce").fillna(0)
        b["_pv"] = cash + h_shares * s_close

        for _, row in b.iterrows():
            ts = row["dt"]
            pv = row["_pv"]
            # Keep the maximum PV if multiple tickers at same timestamp
            current = self._pv_by_time.get(ts, 0.0)
            self._pv_by_time[ts] = max(current, pv)

    def get_series(self) -> pd.Series:
        """Returns portfolio values as a sorted time series."""
        if not self._pv_by_time:
            return pd.Series(dtype=float)
        return pd.Series(self._pv_by_time).sort_index()

    def result(self) -> dict:
        """Returns metadata about the time series."""
        series = self.get_series()
        if series.empty:
            return {"num_observations": 0}
        return {
            "num_observations": len(series),
            "start": str(series.index[0]),
            "end": str(series.index[-1]),
        }


class TotalReturnAccumulator(BaseAccumulator):
    """O(1) memory — only tracks first and last portfolio value."""

    name = "total_return_pct"
    is_internal = False

    def __init__(self) -> None:
        self._first_pv: Optional[float] = None
        self._last_pv: Optional[float] = None
        self._first_ts: Optional[pd.Timestamp] = None
        self._last_ts: Optional[pd.Timestamp] = None

    def update(self, batch: pd.DataFrame) -> None:
        b = batch.copy()
        h_shares = pd.to_numeric(b.get("h_num_shares", 0), errors="coerce").fillna(0)
        s_close = pd.to_numeric(b["s_close"], errors="coerce").fillna(0)
        cash = pd.to_numeric(b["cap_cash"], errors="coerce").fillna(0)
        b["_pv"] = cash + h_shares * s_close
        b = b.sort_values("dt")

        for _, row in b.iterrows():
            ts = row["dt"]
            pv = row["_pv"]
            if self._first_ts is None or ts < self._first_ts:
                self._first_ts = ts
                self._first_pv = pv
            if self._last_ts is None or ts > self._last_ts:
                self._last_ts = ts
                self._last_pv = pv

    def result(self) -> Optional[float]:
        if self._first_pv is None or self._last_pv is None or self._first_pv == 0:
            return None
        return round((self._last_pv - self._first_pv) / self._first_pv * 100, 4)


class StreamingReturnsAccumulator(BaseAccumulator):
    """
    Computes Sharpe/Sortino/Volatility using Welford's algorithm.

    Returns are computed between sequential observations (not assuming daily).
    Annualization factor is inferred from the actual time series.
    """

    name = "_streaming_returns_stats"
    is_internal = True

    def __init__(self, risk_free_rate_annual: float = 0.0) -> None:
        self._rf_annual = risk_free_rate_annual

        # Track portfolio value by timestamp for sequential returns
        self._pv_series: list[tuple[pd.Timestamp, float]] = []

        # Welford accumulators
        self._all_returns = WelfordAccumulator()
        self._downside_returns = WelfordAccumulator()

        # For annualization
        self._first_ts: Optional[pd.Timestamp] = None
        self._last_ts: Optional[pd.Timestamp] = None
        self._num_returns = 0

    def update(self, batch: pd.DataFrame) -> None:
        b = batch.copy()
        h_shares = pd.to_numeric(b.get("h_num_shares", 0), errors="coerce").fillna(0)
        s_close = pd.to_numeric(b["s_close"], errors="coerce").fillna(0)
        cash = pd.to_numeric(b["cap_cash"], errors="coerce").fillna(0)
        b["_pv"] = cash + h_shares * s_close
        b = b.sort_values("dt")

        # Aggregate by timestamp (max PV if multiple tickers)
        for ts, group in b.groupby("dt"):
            pv = group["_pv"].max()
            self._pv_series.append((ts, pv))

            if self._first_ts is None:
                self._first_ts = ts
            self._last_ts = ts

    def _compute_returns(self) -> None:
        """Compute sequential returns (called once at result())."""
        if len(self._pv_series) < 2:
            return

        # Sort by timestamp
        sorted_series = sorted(self._pv_series, key=lambda x: x[0])

        for i in range(1, len(sorted_series)):
            prev_pv = sorted_series[i - 1][1]
            curr_pv = sorted_series[i][1]

            if prev_pv > 0:
                ret = (curr_pv - prev_pv) / prev_pv
                self._all_returns.update(ret)
                self._num_returns += 1

                # Downside return (for Sortino)
                if ret < 0:
                    self._downside_returns.update(ret)

    def _get_annualization_factor(self) -> float:
        """Infer annualization from the time series."""
        if self._first_ts is None or self._last_ts is None or self._num_returns < 2:
            return 1.0
        return infer_annualization_factor(
            self._first_ts, self._last_ts, self._num_returns
        )

    def _convert_risk_free_rate(self) -> float:
        """Convert annual risk-free rate to per-period rate."""
        if self._first_ts is None or self._last_ts is None or self._num_returns < 2:
            return 0.0

        total_days = (self._last_ts - self._first_ts).days
        if total_days == 0:
            return 0.0

        periods_per_day = self._num_returns / total_days
        periods_per_year = periods_per_day * 365.25

        # Convert annual rate to per-period rate
        return self._rf_annual / periods_per_year

    def sharpe(self) -> Optional[float]:
        self._compute_returns()
        if self._all_returns.count == 0 or self._all_returns.std() == 0:
            return None

        rf_per_period = self._convert_risk_free_rate()
        excess = self._all_returns.mean - rf_per_period
        ann_factor = self._get_annualization_factor()

        return round(excess / self._all_returns.std() * ann_factor, 4)

    def sortino(self) -> Optional[float]:
        self._compute_returns()
        if self._downside_returns.count == 0 or self._downside_returns.std() == 0:
            return None

        rf_per_period = self._convert_risk_free_rate()
        excess = self._all_returns.mean - rf_per_period
        ann_factor = self._get_annualization_factor()

        return round(excess / self._downside_returns.std() * ann_factor, 4)

    def volatility(self) -> Optional[float]:
        self._compute_returns()
        if self._all_returns.count == 0:
            return None

        ann_factor = self._get_annualization_factor()
        return round(self._all_returns.std() * ann_factor * 100, 4)

    def result(self) -> dict:
        return {
            "sharpe": self.sharpe(),
            "sortino": self.sortino(),
            "volatility": self.volatility(),
            "annualization_factor": round(self._get_annualization_factor(), 2),
            "num_return_periods": self._num_returns,
        }


class SharpeRatioAccumulator(BaseAccumulator):
    name = "sharpe_ratio"
    is_internal = False

    def __init__(self, streaming_acc: StreamingReturnsAccumulator) -> None:
        self._acc = streaming_acc

    def update(self, batch: pd.DataFrame) -> None:
        pass

    def result(self) -> Optional[float]:
        return self._acc.sharpe()


class SortinoRatioAccumulator(BaseAccumulator):
    name = "sortino_ratio"
    is_internal = False

    def __init__(self, streaming_acc: StreamingReturnsAccumulator) -> None:
        self._acc = streaming_acc

    def update(self, batch: pd.DataFrame) -> None:
        pass

    def result(self) -> Optional[float]:
        return self._acc.sortino()


class VolatilityAccumulator(BaseAccumulator):
    name = "annualised_volatility_pct"
    is_internal = False

    def __init__(self, streaming_acc: StreamingReturnsAccumulator) -> None:
        self._acc = streaming_acc

    def update(self, batch: pd.DataFrame) -> None:
        pass

    def result(self) -> Optional[float]:
        return self._acc.volatility()


class MaxDrawdownAccumulator(BaseAccumulator):
    """
    Requires O(timestamps) memory to store portfolio value series.
    Works for any time interval.
    """

    name = "max_drawdown_pct"
    is_internal = False

    def __init__(self, pv_acc: PortfolioValueTimeSeriesAccumulator) -> None:
        self._pv_acc = pv_acc

    def update(self, batch: pd.DataFrame) -> None:
        pass

    def result(self) -> Optional[float]:
        series = self._pv_acc.get_series()
        if series.empty:
            return None
        peak = series.cummax()
        drawdown = (series - peak) / peak
        return round(drawdown.min() * 100, 4)


class CalmarRatioAccumulator(BaseAccumulator):
    name = "calmar_ratio"
    is_internal = False

    def __init__(
        self,
        total_return_acc: TotalReturnAccumulator,
        max_dd_acc: MaxDrawdownAccumulator,
    ) -> None:
        self._tr = total_return_acc
        self._mdd = max_dd_acc

    def update(self, batch: pd.DataFrame) -> None:
        pass

    def result(self) -> Optional[float]:
        tr = self._tr.result()
        mdd = self._mdd.result()
        if tr is None or mdd is None or mdd == 0:
            return None
        return round(tr / abs(mdd), 4)


class ExposureTimeAccumulator(BaseAccumulator):
    """
    Measures what fraction of observations have h_num_shares > 0.
    Interval-agnostic (works for any frequency).
    """

    name = "exposure_time_pct"
    is_internal = False

    def __init__(self) -> None:
        self._total_obs = 0
        self._exposed_obs = 0

    def update(self, batch: pd.DataFrame) -> None:
        b = batch.copy()
        h_shares = pd.to_numeric(b.get("h_num_shares", 0), errors="coerce").fillna(0)

        self._total_obs += len(b)
        self._exposed_obs += (h_shares > 0).sum()

    def result(self) -> Optional[float]:
        if self._total_obs == 0:
            return None
        return round(self._exposed_obs / self._total_obs * 100, 4)


# ---------------------------------------------------------------------------
# Trade-observation accumulators
# ---------------------------------------------------------------------------


class TradeCountAccumulator(BaseAccumulator):
    """O(1) memory — dict of counters."""

    name = "trade_counts"
    is_internal = False

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}

    def update(self, batch: pd.DataFrame) -> None:
        if "a_type" not in batch.columns:
            return
        for a_type, count in batch["a_type"].value_counts().items():
            self._counts[str(a_type)] = self._counts.get(str(a_type), 0) + int(count)

    def result(self) -> dict[str, int]:
        return self._counts


class AvgHoldingPeriodAccumulator(BaseAccumulator):
    """
    Measures holding period in actual time units (not assuming trading days).
    Returns results in hours, days, and weeks.
    """

    name = "avg_holding_period"
    is_internal = False

    def __init__(self) -> None:
        self._open_buys: dict[str, deque] = defaultdict(deque)
        self._holding_durations: list[pd.Timedelta] = []

    def update(self, batch: pd.DataFrame) -> None:
        if "a_type" not in batch.columns or "dt" not in batch.columns:
            return
        b = batch.sort_values("dt")

        for _, row in b.iterrows():
            ticker = str(row["ticker"])
            a_type = str(row.get("a_type", "")).lower()
            ts = row["dt"]

            if a_type == "buy":
                self._open_buys[ticker].append(ts)
            elif a_type == "sell" and self._open_buys[ticker]:
                buy_ts = self._open_buys[ticker].popleft()
                duration = ts - buy_ts
                self._holding_durations.append(duration)

    def result(self) -> Optional[dict]:
        if not self._holding_durations:
            return None

        avg_duration = sum(self._holding_durations, pd.Timedelta(0)) / len(
            self._holding_durations
        )

        return {
            "hours": round(avg_duration.total_seconds() / 3600, 2),
            "days": round(avg_duration.total_seconds() / 86400, 2),
            "weeks": round(avg_duration.total_seconds() / 604800, 2),
        }


class TradingPeriodAccumulator(BaseAccumulator):
    """O(1) memory — tracks min/max timestamps."""

    name = "trading_period"
    is_internal = False

    def __init__(self) -> None:
        self._start: Optional[pd.Timestamp] = None
        self._end: Optional[pd.Timestamp] = None

    def update(self, batch: pd.DataFrame) -> None:
        if "dt" not in batch.columns or batch.empty:
            return
        batch_min = batch["dt"].min()
        batch_max = batch["dt"].max()
        if self._start is None or batch_min < self._start:
            self._start = batch_min
        if self._end is None or batch_max > self._end:
            self._end = batch_max

    def result(self) -> dict:
        if self._start is None or self._end is None:
            return {}

        duration = self._end - self._start

        return {
            "start": str(self._start),
            "end": str(self._end),
            "duration_hours": round(duration.total_seconds() / 3600, 2),
            "duration_days": round(duration.total_seconds() / 86400, 2),
            "duration_weeks": round(duration.total_seconds() / 604800, 2),
        }


class FinalPortfolioAccumulator(BaseAccumulator):
    """O(1) memory — overwrites with latest batch."""

    name = "final_portfolio"
    is_internal = False

    def __init__(self) -> None:
        self._last_batch: Optional[pd.DataFrame] = None

    def update(self, batch: pd.DataFrame) -> None:
        if not batch.empty:
            self._last_batch = batch.copy()

    def result(self) -> Optional[dict]:
        if self._last_batch is None:
            return None
        b = self._last_batch.sort_values("dt")
        last = b.iloc[-1]
        cash = float(pd.to_numeric(last.get("cap_cash", 0), errors="coerce") or 0)
        shares = float(pd.to_numeric(last.get("h_num_shares", 0), errors="coerce") or 0)
        close = float(pd.to_numeric(last.get("s_close", 0), errors="coerce") or 0)
        equity = shares * close
        return {
            "cash": round(cash, 4),
            "equity_value": round(equity, 4),
            "total_value": round(cash + equity, 4),
        }
