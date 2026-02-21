"""
Downsample state observations for plotting using a modified LTTB algorithm.
Preserves all datetimes at which any trade occurred.
Supports paginated/streaming processing for larger-than-memory datasets.
"""

from pathlib import Path

import pandas as pd

from constants import (
    DEFAULT_PLOT_DOWNSAMPLE_TARGET_SIZE,
    FILE_PATH_PLOT_DATA,
    OBS_FIELD_TIMESTAMP,
)
from model.reporting.types import ObservationType, ObservationTypeMetadata
from reporting.context import ObservationContext

# Columns written to plot_data.csv (dt for x-axis, ticker and s_close for plotting)
PLOT_OUTPUT_COLUMNS = [OBS_FIELD_TIMESTAMP, "ticker", "s_close"]


def _lttb_indices(x: list[float], y: list[float], target: int) -> list[int]:
    """
    Largest Triangle Three Buckets: return indices of `target` points that
    approximate the series (x, y) for visual fidelity.
    """
    n = len(x)
    if n <= target or target <= 2:
        return list(range(n))

    selected: list[int] = [0]
    bucket_size = (n - 2) / (target - 2)

    for b in range(target - 2):
        lo = int(1 + b * bucket_size)
        hi = int(1 + (b + 1) * bucket_size)
        hi = min(hi, n - 1)
        if lo >= hi:
            selected.append(hi)
            continue

        next_lo = int(1 + (b + 1) * bucket_size)
        next_hi = int(1 + (b + 2) * bucket_size)
        next_hi = min(next_hi, n)
        if next_lo >= next_hi:
            next_hi = n
        next_cx = sum(x[i] for i in range(next_lo, next_hi)) / max(1, next_hi - next_lo)
        next_cy = sum(y[i] for i in range(next_lo, next_hi)) / max(1, next_hi - next_lo)

        prev_i = selected[-1]
        px, py = x[prev_i], y[prev_i]

        best_i = lo
        best_area = -1.0
        for i in range(lo, hi):
            area = abs((x[i] - px) * (next_cy - py) - (next_cx - px) * (y[i] - py))
            if area > best_area:
                best_area = area
                best_i = i
        selected.append(best_i)

    selected.append(n - 1)
    return selected


def _stream_trade_dts(path: Path, chunk_size: int = 10_000) -> set[pd.Timestamp]:
    """Build set of trade datetimes by streaming trade CSV in chunks."""
    out: set[pd.Timestamp] = set()
    for chunk in pd.read_csv(path, chunksize=chunk_size):
        chunk[OBS_FIELD_TIMESTAMP] = pd.to_datetime(chunk[OBS_FIELD_TIMESTAMP])
        out.update(chunk[OBS_FIELD_TIMESTAMP].dropna().unique().tolist())
    return out


class Downsampler:
    """
    Downsamples state observations for plotting using modified LTTB.
    Never drops a datetime at which any trade observation occurred.
    Processes state in chunks to support larger-than-memory datasets.
    """

    def __init__(
        self,
        context: ObservationContext,
        target_size: int = DEFAULT_PLOT_DOWNSAMPLE_TARGET_SIZE,
        value_column: str | None = None,
        chunk_size: int = 50000,
    ) -> None:
        self.ctx = context
        self.target_size = max(2, target_size)
        self.value_column = value_column
        self.chunk_size = max(1, chunk_size)

    def _ensure_value_column(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.value_column and self.value_column in df.columns:
            return df
        cash = pd.to_numeric(df.get("cap_cash", 0), errors="coerce").fillna(0)
        shares = pd.to_numeric(df.get("h_num_shares", 0), errors="coerce").fillna(0)
        close = pd.to_numeric(df.get("s_close", 0), errors="coerce").fillna(0)
        df = df.copy()
        df["_value"] = cash + shares * close
        return df

    def _state_meta_num_records(self) -> int:
        path = self.ctx.file_paths_obs_meta.get(ObservationType.STATE.name)
        if not path or not path.exists():
            return 0
        meta = ObservationTypeMetadata.from_json(path)
        return meta.num_records

    def run(self) -> Path:
        """
        Downsample state observations (streaming) and write to interim/plot_data.csv.
        Returns the path of the written file.
        """
        state_path = self.ctx.file_paths_obs.get(ObservationType.STATE.name)
        out_path = self.ctx.file_path_interim / FILE_PATH_PLOT_DATA
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if not state_path or not state_path.exists():
            pd.DataFrame().to_csv(out_path, index=False)
            return out_path

        # Stream trade CSV to get datetimes where trades occurred (small memory)
        trade_path = self.ctx.file_paths_obs.get(ObservationType.TRADE.name)
        trade_dts: set[pd.Timestamp] = set()
        if trade_path and trade_path.exists():
            trade_dts = _stream_trade_dts(trade_path, chunk_size=self.chunk_size)

        total_n = self._state_meta_num_records()
        if total_n <= 0:
            # Metadata missing: count rows by streaming once
            total_n = sum(
                len(c) for c in pd.read_csv(state_path, chunksize=self.chunk_size)
            )
            if total_n == 0:
                pd.read_csv(state_path, nrows=0).to_csv(out_path, index=False)
                return out_path

        total_chunks = max(1, (total_n + self.chunk_size - 1) // self.chunk_size)
        lttb_budget_total = max(0, self.target_size - 2 - len(trade_dts))
        ts_col = OBS_FIELD_TIMESTAMP
        val_col = self.value_column if self.value_column else "_value"
        header_written = False

        for chunk_idx, chunk in enumerate(
            pd.read_csv(state_path, chunksize=self.chunk_size)
        ):
            chunk[ts_col] = pd.to_datetime(chunk[ts_col])
            chunk = self._ensure_value_column(chunk)
            n_chunk = len(chunk)
            if n_chunk == 0:
                continue

            is_first = chunk_idx == 0
            is_last = chunk_idx == total_chunks - 1

            # Must-keep: first row of series, last row of series, any row with dt in trade_dts
            must_keep = pd.Series(False, index=chunk.index)
            if is_first:
                must_keep.iloc[0] = True
            if is_last:
                must_keep.iloc[-1] = True
            must_keep = must_keep | chunk[ts_col].isin(trade_dts)

            other_mask = ~must_keep
            n_other = other_mask.sum()

            # LTTB budget for this chunk (proportional)
            lttb_budget = max(
                0,
                int((n_chunk / total_n) * lttb_budget_total),
            )
            lttb_budget = min(lttb_budget, n_other)

            if n_other == 0 or lttb_budget == 0:
                selected_mask = must_keep
            else:
                other_df = chunk.loc[other_mask]
                x = list(range(len(other_df)))
                y = other_df[val_col].astype(float).tolist()
                if lttb_budget <= 2:
                    lttb_sel = (
                        [0, len(other_df) - 1]
                        if len(other_df) > 1
                        else list(range(len(other_df)))
                    )
                else:
                    lttb_sel = _lttb_indices(x, y, lttb_budget)
                other_indices = other_df.index[lttb_sel].tolist()
                selected_mask = must_keep | chunk.index.isin(other_indices)

            out_chunk = chunk.loc[selected_mask]
            if "_value" in out_chunk.columns and val_col == "_value":
                out_chunk = out_chunk.drop(columns=["_value"], errors="ignore")
            # Only output columns needed for plotting
            out_cols = [c for c in PLOT_OUTPUT_COLUMNS if c in out_chunk.columns]
            out_chunk = out_chunk[out_cols] if out_cols else out_chunk
            out_chunk.to_csv(
                out_path,
                index=False,
                mode="w" if not header_written else "a",
                header=not header_written,
            )
            header_written = True

        return out_path
