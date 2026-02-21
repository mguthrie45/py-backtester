import csv
import json
from typing import Optional

from constants import DEFAULT_OBSERVATION_BATCH_SIZE, OBS_FIELDS_NO_PREFIX
from log.logger import Logger
from model.reporting import Observable, ObservationType, ObservationTypeState
from reporting.context import ObservationContext


class Observer:
    instance: Optional["Observer"] = None
    ctx: Optional[ObservationContext]

    def __init__(self, context: ObservationContext):
        self.observation_batch_size = DEFAULT_OBSERVATION_BATCH_SIZE
        self.observation_states = {
            k.name: ObservationTypeState() for k in ObservationType
        }

        self.ctx = context
        for t in ObservationType:
            self.observation_states[t.name].metadata.tickers = context.tickers
            self.observation_states[t.name].metadata.num_tickers = len(context.tickers)

        self.__create_report_dir()

        Observer.instance = self

    def __get_field_name(self, obs_json_attr: str, obs_key_prefix: str) -> str:
        return (
            obs_json_attr
            if obs_json_attr in OBS_FIELDS_NO_PREFIX
            else f"{obs_key_prefix}_{obs_json_attr}"
        )

    def __create_report_dir(self) -> None:
        if not self.ctx:
            Logger.error(
                "Report directory could not be created, observation context was not set."
            )
            return

        self.ctx.file_path_report.mkdir(parents=True, exist_ok=True)
        self.ctx.file_path_interim.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_instance() -> Optional["Observer"]:
        return Observer.instance

    def observe(self, obs: Observable) -> None:
        for observation_type in ObservationType:
            if isinstance(obs, observation_type.value):
                return (
                    self.observation_states[observation_type.name]
                    .obs_pool[obs.obs_field_prefix()]
                    .append(obs)
                )

        Logger.error(
            "Attempted to observe observable object but could not identify observation type."
        )

    def pack_observation_pool(self) -> None:
        for observation_type in ObservationType:
            merged_by_ticker = {
                k: {k: None for k in self.ctx.obs_field_names[observation_type.name]}
                for k in self.ctx.tickers
            }

            pool = self.observation_states[observation_type.name].obs_pool

            for pfx, obs_list in pool.items():
                for obs in obs_list:
                    for field, val in obs.obs_json.items():
                        if obs.ticker:
                            merged_by_ticker[obs.ticker][
                                self.__get_field_name(field, pfx)
                            ] = val
                        else:
                            for ticker in self.ctx.tickers:
                                merged_by_ticker[ticker][
                                    self.__get_field_name(field, pfx)
                                ] = val

            self.observation_states[observation_type.name].obs_slices.extend(
                list(merged_by_ticker.values())
            )

        for t in ObservationType:
            self.observation_states[t.name].obs_pool.clear()

            if (
                len(self.observation_states[t.name].obs_slices)
                >= self.observation_batch_size
            ):
                self.__flush_observation_slices(t)

    def shutdown(self) -> None:
        for observation_type in ObservationType:
            self.__flush_observation_slices(observation_type)

            file_path = self.ctx.file_paths_obs_meta.get(observation_type.name)
            if not file_path:
                Logger.error(
                    "Failed to save observation metadata on shutdown. %s metadata file path was not made.",
                    observation_type.name,
                )
                return

            file_path.write_text(
                json.dumps(
                    self.observation_states[observation_type.name].metadata.__dict__
                ),
                encoding="utf-8",
            )

    def __flush_observation_slices(self, obs_type: ObservationType) -> None:
        file_path = self.ctx.file_paths_obs.get(obs_type.name)
        if not file_path:
            Logger.error(
                "Cannot flush observations. %s observation file path was not made.",
                obs_type.name,
            )
            return

        slices = self.observation_states[obs_type.name].obs_slices
        if not slices:
            return

        self.observation_states[obs_type.name].metadata.num_records += len(slices)

        field_names = self.ctx.obs_field_names.get(obs_type.name, [])

        if not field_names:
            Logger.error(
                f"Cannot flush observations. No field names found for observation type: {obs_type.name}"
            )
            return

        file_exists = file_path.exists()
        try:
            with file_path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=field_names)

                if not file_exists:
                    writer.writeheader()

                writer.writerows(slices)

            self.observation_states[obs_type.name].obs_slices = []

        except Exception as e:
            Logger.error("Failed to flush observations to %s: %s", file_path, e)
