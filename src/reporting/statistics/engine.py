"""
engine.py
---------
Orchestrates paginated loading and metric accumulation,
then serialises results to report.json.
"""

from __future__ import annotations

import json

from model.reporting.types import ObservationType
from reporting.context import ObservationContext
from reporting.statistics.loader import ObservationLoader

from reporting.statistics.accumulators import (
    AvgHoldingPeriodAccumulator,
    CalmarRatioAccumulator,
    ExposureTimeAccumulator,
    FinalPortfolioAccumulator,
    MaxDrawdownAccumulator,
    PortfolioValueTimeSeriesAccumulator,
    SharpeRatioAccumulator,
    SortinoRatioAccumulator,
    StreamingReturnsAccumulator,
    TotalReturnAccumulator,
    TradeCountAccumulator,
    TradingPeriodAccumulator,
    VolatilityAccumulator,
)

UNAVAILABLE_METRICS = [
    {
        "metric": "alpha",
        "reason": "No benchmark price series available.",
        "fix": "Add 'benchmark_close' column to state_obs.csv or provide benchmark_obs.csv.",
    },
    {
        "metric": "beta",
        "reason": "No benchmark price series available.",
        "fix": "Same as alpha.",
    },
    {
        "metric": "information_ratio",
        "reason": "No benchmark returns to compare against.",
        "fix": "Same as alpha.",
    },
    {
        "metric": "realized_pnl_per_trade",
        "reason": "trade_obs.csv contains no execution price (a_price). "
        "s_close is end-of-day and does not reflect actual fill price.",
        "fix": "Add 'a_price' (execution/fill price) to trade_obs.csv.",
    },
    {
        "metric": "slippage_and_commission_costs",
        "reason": "No transaction cost data in either observation file.",
        "fix": "Add 'a_commission' and 'a_slippage' columns to trade_obs.csv.",
    },
]


class BacktestReportEngine:
    def __init__(
        self,
        context: ObservationContext,
        batch_size: int = 1000,
        risk_free_daily: float = 0.0,
    ) -> None:
        self.batch_size = batch_size
        self.risk_free_daily = risk_free_daily
        self.ctx = context
        risk_free_annual = risk_free_daily * 365.25 if risk_free_daily else 0.0

        self._state_obs_processor = ObservationLoader(
            observation_type=ObservationType.STATE, context=context
        )
        self._trade_obs_processor = ObservationLoader(
            observation_type=ObservationType.TRADE, context=context
        )

        self.metrics = MetricRepository(
            tickers=context.tickers,
            risk_free_rate_annual=risk_free_annual,
        )

    def run(self) -> dict:
        """Execute the full pipeline and write report.json. Returns the report dict."""
        print(f"[engine] Processing state observations...")
        self._process_state()

        print(f"[engine] Processing trade observations...")
        self._process_trades()

        print(f"[engine] Assembling report...")
        report = self._assemble_report()

        self.ctx.file_path_raw_report.write_text(
            json.dumps(report, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"[engine] Report written to: {self.ctx.file_path_report}")
        return report

    def _process_state(self) -> None:
        state_acc_list = list(self.metrics.state_accs.values())
        total = self._state_obs_processor.total_batches

        for i, batch in enumerate(self._state_obs_processor.batches(), start=1):
            print(f"  state batch {i}/{total} ({len(batch)} rows)")
            for acc in state_acc_list:
                acc.update(batch)
            if "ticker" in batch.columns:
                for ticker in self.metrics.tickers:
                    ticker_batch = batch[batch["ticker"] == ticker]
                    if ticker_batch.empty:
                        continue
                    for acc in self.metrics.state_accs_by_ticker[ticker].values():
                        acc.update(ticker_batch)

    def _process_trades(self) -> None:
        trade_acc_list = list(self.metrics.trade_accs.values())
        total = self._trade_obs_processor.total_batches

        for i, batch in enumerate(self._trade_obs_processor.batches(), start=1):
            print(f"  trade batch {i}/{total} ({len(batch)} rows)")
            for acc in trade_acc_list:
                acc.update(batch)
            if "ticker" in batch.columns:
                for ticker in self.metrics.tickers:
                    ticker_batch = batch[batch["ticker"] == ticker]
                    if ticker_batch.empty:
                        continue
                    for acc in self.metrics.trade_accs_by_ticker[ticker].values():
                        acc.update(ticker_batch)

    def _assemble_report(self) -> dict:
        cumulative_metrics: dict = {}
        for name, acc in self.metrics.state_accs.items():
            if not acc.is_internal:
                cumulative_metrics[name] = acc.result()
        for name, acc in self.metrics.trade_accs.items():
            if not acc.is_internal:
                cumulative_metrics[name] = acc.result()

        by_ticker: dict[str, dict] = {}
        for ticker in self.metrics.tickers:
            by_ticker[ticker] = {}
            for name, acc in self.metrics.state_accs_by_ticker[ticker].items():
                if not acc.is_internal:
                    by_ticker[ticker][name] = acc.result()
            for name, acc in self.metrics.trade_accs_by_ticker[ticker].items():
                if not acc.is_internal:
                    by_ticker[ticker][name] = acc.result()

        return {
            "metrics": {
                "cumulative": cumulative_metrics,
                "by_ticker": by_ticker,
            },
            "unavailable_metrics": UNAVAILABLE_METRICS,
            "config": {
                "batch_size": self.batch_size,
                "risk_free_daily": self.risk_free_daily,
            },
        }


def _build_state_accumulators(risk_free_rate_annual: float = 0.0) -> dict:
    pv_series = PortfolioValueTimeSeriesAccumulator()
    streaming = StreamingReturnsAccumulator(risk_free_rate_annual)
    tr = TotalReturnAccumulator()
    mdd = MaxDrawdownAccumulator(pv_series)

    accumulators = [
        pv_series,
        tr,
        streaming,
        SharpeRatioAccumulator(streaming),
        SortinoRatioAccumulator(streaming),
        VolatilityAccumulator(streaming),
        mdd,
        CalmarRatioAccumulator(tr, mdd),
        ExposureTimeAccumulator(),
        FinalPortfolioAccumulator(),
        TradingPeriodAccumulator(),
    ]
    return {acc.name: acc for acc in accumulators}


def _build_trade_accumulators() -> dict:
    accumulators = [
        TradeCountAccumulator(),
        AvgHoldingPeriodAccumulator(),
    ]
    return {acc.name: acc for acc in accumulators}


class MetricRepository:
    def __init__(
        self,
        tickers: list[str],
        risk_free_rate_annual: float = 0.0,
    ) -> None:
        self.tickers = tickers
        self.state_accs = _build_state_accumulators(risk_free_rate_annual)
        self.trade_accs = _build_trade_accumulators()
        self.state_accs_by_ticker = {
            t: _build_state_accumulators(risk_free_rate_annual) for t in tickers
        }
        self.trade_accs_by_ticker = {t: _build_trade_accumulators() for t in tickers}
