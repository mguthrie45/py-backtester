from pathlib import Path

from constants import (
    FILE_PATH_RAW_REPORT,
    FILE_PATH_REL_INTERIM,
    FILE_PATH_REL_TEST_REPORT,
    FILE_PATH_STATE_OBS,
    FILE_PATH_STATE_OBS_META,
    FILE_PATH_TRADE_OBS,
    FILE_PATH_TRADE_OBS_META,
    OBS_FIELDS_NO_PREFIX,
)
from log.logger import Logger
from model.reporting.observable import Observable
from model.reporting.types import ObservationType


class ObservationContext:
    """
    Reporting context built from test/strategy names and tickers.
    Holds computed paths and field names; not a validated config payload.
    """

    def __init__(
        self,
        test_name: str,
        strategy_name: str,
        tickers: list[str],
        dedup: str = "DWYH",
    ) -> None:
        self.test_name = test_name
        self.strategy_name = strategy_name
        self.tickers = tickers
        self.dedup = dedup

        self.obs_field_names: dict[str, list[str]] = {}
        self.file_paths_obs: dict[str, Path] = {}
        self.file_paths_obs_meta: dict[str, Path] = {}

        Logger.info("Observer context set: %s", self.report_name)
        self._register_obs_field_names()

        cwd = Path(__file__).resolve().parent
        self.file_path_report = (
            cwd / FILE_PATH_REL_TEST_REPORT / self.report_name
        ).resolve()
        self.file_path_interim = (
            self.file_path_report / FILE_PATH_REL_INTERIM
        ).resolve()

        self.file_paths_obs[ObservationType.STATE.name] = (
            self.file_path_interim / FILE_PATH_STATE_OBS
        ).resolve()
        self.file_paths_obs_meta[ObservationType.STATE.name] = (
            self.file_path_interim / FILE_PATH_STATE_OBS_META
        ).resolve()
        self.file_paths_obs[ObservationType.TRADE.name] = (
            self.file_path_interim / FILE_PATH_TRADE_OBS
        ).resolve()
        self.file_paths_obs_meta[ObservationType.TRADE.name] = (
            self.file_path_interim / FILE_PATH_TRADE_OBS_META
        ).resolve()
        self.file_path_raw_report = (
            self.file_path_report / FILE_PATH_RAW_REPORT
        ).resolve()

    @property
    def report_name(self) -> str:
        return f"{self.test_name}_{self.strategy_name}_{self.dedup}"

    def _get_fields(self, observable_type: type[Observable]) -> list[str]:
        base_fields = observable_type.obs_fields()
        return [
            f
            if f in OBS_FIELDS_NO_PREFIX
            else f"{observable_type.obs_field_prefix()}_{f}"
            for f in base_fields
        ]

    def _register_obs_field_names(self) -> None:
        for observation_type in ObservationType:
            obs_field_names = []
            for observable_type in observation_type.value.__subclasses__():
                obs_field_names.extend(self._get_fields(observable_type))
            self.obs_field_names[observation_type.name] = obs_field_names
