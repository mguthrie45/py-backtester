from typing import Generator, Optional

import pandas as pd

from constants import OBS_FIELD_TIMESTAMP
from model.reporting import ObservationType, ObservationTypeMetadata
from reporting.context import ObservationContext


class ObservationLoader:
    def __init__(
        self,
        observation_type: ObservationType,
        context: ObservationContext,
        dtype: Optional[dict] = None,
    ) -> None:
        self.obs_type = observation_type
        self.ctx = context

        self.meta = ObservationTypeMetadata.from_json(
            context.file_paths_obs_meta.get(observation_type.name)
        )

        self.batch_size = 1000
        self.dtype = dtype or {}

    @property
    def num_records(self) -> int:
        return self.meta.num_records

    @property
    def total_batches(self) -> int:
        return max(1, -(-self.num_records // self.batch_size))

    def batches(self) -> Generator[pd.DataFrame, None, None]:
        """Yields successive DataFrame batches."""
        reader = pd.read_csv(
            self.ctx.file_paths_obs.get(self.obs_type.name),
            chunksize=self.batch_size,
            dtype=self.dtype,
        )
        for chunk in reader:
            chunk[OBS_FIELD_TIMESTAMP] = pd.to_datetime(chunk[OBS_FIELD_TIMESTAMP])
            yield chunk
