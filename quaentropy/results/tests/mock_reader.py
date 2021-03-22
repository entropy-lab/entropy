from typing import List, Optional

from pandas import DataFrame

from quaentropy.api.data_reader import (
    DataReader,
    DebugRecord,
    MetadataRecord,
    ExperimentRecord,
    ResultRecord,
    PlotRecord,
)


class MockDataReader(DataReader):  # todo guy
    def get_last_experiments(self, count: int) -> DataFrame:
        pass

    def get_experiments_range(self, starting_from_index: int, count: int) -> DataFrame:
        pass

    def get_experiment_record(self, experiment_id: int) -> Optional[ExperimentRecord]:
        pass

    def get_result(self, experiment_id: int, label: str) -> Optional[ResultRecord]:
        pass

    def get_metadata_record(
        self, experiment_id: int, label: str
    ) -> Optional[MetadataRecord]:
        pass

    def get_debug_record(self, experiment_id: int) -> Optional[DebugRecord]:
        pass

    def get_raw_results_from_all_experiments(self, name) -> List[ResultRecord]:
        pass

    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        pass

    def get_last_result(self, experiment_id: int) -> Optional[ResultRecord]:
        pass
