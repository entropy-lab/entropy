from time import time_ns
from typing import List, Optional

from pandas import DataFrame

from quaentropy.api.data_reader import (
    DataReader,
    ResultRecord,
    DebugRecord,
    MetadataRecord,
    ExperimentRecord,
    ScriptViewer,
    PlotRecord,
)
from quaentropy.api.data_writer import DataWriter, Plot
from quaentropy.api.data_writer import (
    ExperimentInitialData,
    ExperimentEndData,
    RawResultData,
    Metadata,
    Debug,
)


class MemoryOnlyDataReaderWriter(DataWriter, DataReader):
    def __init__(self):
        super(DataWriter, self).__init__()
        super(DataReader, self).__init__()
        self._initial_data: Optional[ExperimentInitialData] = None
        self._end_data: Optional[ExperimentEndData] = None
        self._results: List[RawResultData] = []
        self._metadata: List[Metadata] = []
        self._debug: Optional[Debug] = None
        self._plot: List[Plot] = []

    def save_experiment_initial_data(self, initial_data: ExperimentInitialData) -> int:
        id = time_ns()
        self._initial_data = initial_data
        return id

    def save_experiment_end_data(self, experiment_id: int, end_data: ExperimentEndData):
        self._end_data = end_data

    def save_result(self, experiment_id: int, result: RawResultData):
        self._results.append(result)

    def save_metadata(self, experiment_id: int, metadata: Metadata):
        self._metadata.append(metadata)

    def save_debug(self, experiment_id: int, debug: Debug):
        self._debug = debug

    def save_plot(self, experiment_id: int, plot: Plot):
        self._plot.append(plot)

    def get_experiments_range(self, starting_from_index: int, count: int) -> DataFrame:
        raise NotImplementedError()

    def get_experiment_record(self, experiment_id: int) -> Optional[ExperimentRecord]:
        if self._initial_data:
            if self._end_data:
                end_time = self._end_data.end_time
            else:
                end_time = None
            return ExperimentRecord(
                experiment_id,
                self._initial_data.label,
                ScriptViewer([self._initial_data.script.serialize()]),
                self._initial_data.start_time,
                end_time,
                self._initial_data.story,
            )
        else:
            return None

    def get_result(self, experiment_id: int, label: str) -> Optional[ResultRecord]:
        raw_result = next((x for x in self._results if x.label == label), None)
        if raw_result:
            index = self._results.index(raw_result)
            return ResultRecord(
                experiment_id,
                index,
                raw_result.label,
                raw_result.story,
                raw_result.stage,
                raw_result.data,
            )
        else:
            return None

    def get_metadata_record(
        self, experiment_id: int, label: str
    ) -> Optional[MetadataRecord]:
        metadata = next((x for x in self._metadata if x.label == label), None)
        if metadata:
            index = self._metadata.index(metadata)
            return MetadataRecord(
                experiment_id, index, metadata.label, metadata.stage, metadata.data
            )
        else:
            return None

    def get_debug_record(self, experiment_id: int) -> Optional[DebugRecord]:
        if self._debug:
            return DebugRecord(
                experiment_id,
                0,
                self._debug.python_env,
                self._debug.python_history,
                self._debug.station_specs,
                self._debug.extra,
            )
        else:
            return None

    def get_raw_results_from_all_experiments(self, name) -> List[ResultRecord]:
        raise NotImplementedError()

    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        return [
            PlotRecord(
                experiment_id,
                self._plot.index(plot),
                plot.label,
                plot.story,
                plot.data,
                plot.data_type,
                plot.bokeh_generator,
            )
            for plot in self._plot
        ]

    def get_last_result(self, experiment_id: int) -> Optional[ResultRecord]:
        if len(self._results) > 0:
            raw_result = self._results[-1]
            if raw_result:
                index = self._results.index(raw_result)
                return ResultRecord(
                    experiment_id,
                    index,
                    raw_result.label,
                    raw_result.story,
                    raw_result.stage,
                    raw_result.data,
                )
            else:
                return None
        else:
            return None
