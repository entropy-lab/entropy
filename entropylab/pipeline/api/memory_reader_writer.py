import random
from datetime import datetime
from time import time_ns
from typing import List, Optional, Iterable, Any, Dict, Tuple

from pandas import DataFrame
from plotly import graph_objects as go

from entropylab.pipeline.api.data_reader import (
    DataReader,
    ResultRecord,
    DebugRecord,
    MetadataRecord,
    ExperimentRecord,
    ScriptViewer,
    PlotRecord,
    FigureRecord,
)
from entropylab.pipeline.api.data_writer import DataWriter, PlotSpec, NodeData
from entropylab.pipeline.api.data_writer import (
    ExperimentInitialData,
    ExperimentEndData,
    RawResultData,
    Metadata,
    Debug,
)


class MemoryOnlyDataReaderWriter(DataWriter, DataReader):
    """
    Implementation of DataWriter and DataReader that saves all the data
    to objects in memory.
    Used if no other implementation of the db is used in entropy.
    """

    def __init__(self):
        super(DataWriter, self).__init__()
        super(DataReader, self).__init__()
        self._initial_data: Optional[ExperimentInitialData] = None
        self._end_data: Optional[ExperimentEndData] = None
        self._results: List[Tuple[RawResultData, datetime]] = []
        self._metadata: List[Tuple[Metadata, datetime]] = []
        self._debug: Optional[Debug] = None
        self._plot: Dict[PlotSpec, Any] = {}
        self._figure: Dict[int, List[FigureRecord]] = {}
        self._nodes: List[NodeData] = []

    def save_experiment_initial_data(self, initial_data: ExperimentInitialData) -> int:
        self._initial_data = initial_data
        return time_ns()

    def save_experiment_end_data(self, experiment_id: int, end_data: ExperimentEndData):
        self._end_data = end_data

    def save_result(self, experiment_id: int, result: RawResultData):
        self._results.append((result, datetime.now()))

    def save_metadata(self, experiment_id: int, metadata: Metadata):
        self._metadata.append((metadata, datetime.now()))

    def save_debug(self, experiment_id: int, debug: Debug):
        self._debug = debug

    def save_plot(self, experiment_id: int, plot: PlotSpec, data: Any):
        self._plot[plot] = data

    def save_figure(self, experiment_id: int, figure: go.Figure) -> None:
        figure_record = FigureRecord(
            experiment_id=experiment_id,
            id=random.randint(0, 2**31 - 1),
            figure=figure,
            time=datetime.now(),
        )
        if experiment_id in self._figure:
            self._figure[experiment_id].append(figure_record)
        else:
            self._figure[experiment_id] = [figure_record]

    def save_node(self, experiment_id: int, node_data: NodeData):
        self._nodes.append(node_data)

    def get_experiments_range(self, starting_from_index: int, count: int) -> DataFrame:
        raise NotImplementedError()

    def get_experiments(
        self,
        label: Optional[str] = None,
        start_after: Optional[datetime] = None,
        end_after: Optional[datetime] = None,
        success: Optional[bool] = None,
    ) -> Iterable[ExperimentRecord]:
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
                ScriptViewer([self._initial_data.script]),
                self._initial_data.start_time,
                end_time,
                self._initial_data.story,
                self._end_data.success,
            )
        else:
            return None

    def get_results(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[ResultRecord]:
        return list(
            ResultRecord(
                experiment_id,
                str(self._results.index(x)),
                x[0].label,
                x[0].story,
                x[0].stage,
                x[0].data,
                x[1],
            )
            for x in self._results
            if (not label or x[0].label == label)
            and (stage is None or x[0].stage == stage)
        )

    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[MetadataRecord]:
        return list(
            MetadataRecord(
                experiment_id,
                str(self._metadata.index(x)),
                x[0].label,
                x[0].stage,
                x[0].data,
                x[1],
            )
            for x in self._metadata
            if (not label or x[0].label == label)
            and (stage is None or x[0].stage == stage)
        )

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

    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        return [
            PlotRecord(
                experiment_id,
                id(plot),
                self._plot[plot],
                plot.generator(),
                plot.label,
                plot.story,
            )
            for plot in self._plot
        ]

    def get_figures(self, experiment_id: int) -> List[FigureRecord]:
        return self._figure[experiment_id]

    def get_node_stage_ids_by_label(
        self, label: str, experiment_id: Optional[int] = None
    ) -> List[int]:
        return list(x.stage_id for x in self._nodes if (not label or x.label == label))

    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        if len(self._results) > 0:
            raw_result = self._results[-1]
            if raw_result:
                index = self._results.index(raw_result)
                return ResultRecord(
                    experiment_id,
                    str(index),
                    raw_result[0].label,
                    raw_result[0].story,
                    raw_result[0].stage,
                    raw_result[0].data,
                    raw_result[1],
                )
            else:
                return None
        else:
            return None

    def update_experiment_favorite(self, experiment_id: int, favorite: bool) -> None:
        raise NotImplementedError()
