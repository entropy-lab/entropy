import enum
import importlib
import pickle
from datetime import datetime
from io import BytesIO
from typing import Any

import numpy as np
from plotly import graph_objects as go
from plotly.io import from_json, to_json
from sqlalchemy import (
    Column,
    Integer,
    String,
    DATETIME,
    ForeignKey,
    BLOB,
    Enum,
    Boolean,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from entropylab.api.data_reader import (
    ExperimentRecord,
    ScriptViewer,
    ResultRecord,
    MetadataRecord,
    DebugRecord,
    PlotRecord,
    FigureRecord,
)
from entropylab.api.data_writer import (
    ExperimentInitialData,
    RawResultData,
    Metadata,
    Debug,
    PlotSpec,
    NodeData,
)
from entropylab.api.errors import EntropyError
from entropylab.logger import logger


def _get_class(module_name, class_name):
    module = importlib.import_module(module_name)
    if not hasattr(module, class_name):
        raise EntropyError("class {} is not in {}".format(class_name, module_name))
    logger.debug("reading class {} from module {}".format(class_name, module_name))
    cls = getattr(module, class_name)
    return cls


def _encode_serialized_data(data):
    if isinstance(data, (np.ndarray, np.generic)):
        bio = BytesIO()
        # noinspection PyTypeChecker
        np.save(bio, data)
        bio.seek(0)
        serialized_data = bio.read()
        data_type = ResultDataType.Npy
    else:
        try:
            serialized_data = pickle.dumps(data)
            data_type = ResultDataType.Pickled
        except RuntimeError:
            serialized_data = data.__repr__().encode(encoding="UTF-8")
            data_type = ResultDataType.String
    return data_type, serialized_data


def _decode_serialized_data(serialized_data, data_type):
    if data_type == ResultDataType.Pickled:
        data = pickle.loads(serialized_data)
    elif data_type == ResultDataType.Npy:
        bio = BytesIO(serialized_data)
        # noinspection PyTypeChecker
        data = np.load(bio)
    else:
        data = serialized_data.decode()
    return data


Base = declarative_base()


class ExperimentTable(Base):
    __tablename__ = "Experiments"

    id = Column(Integer, primary_key=True)
    label = Column(String)
    script = Column(String)
    start_time = Column(DATETIME, nullable=False)
    end_time = Column(DATETIME)
    user = Column(String)
    story = Column(String)
    success = Column(Boolean, default=False)
    results = relationship("ResultTable", cascade="all, delete-orphan")
    experiment_metadata = relationship("MetadataTable", cascade="all, delete-orphan")
    debug = relationship("DebugTable", cascade="all, delete-orphan")
    plots = relationship("PlotTable", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<_Experiment(id='{self.id}')>"

    def to_record(self):
        return ExperimentRecord(
            id=self.id,
            label=self.label,
            script=ScriptViewer([self.script]),
            start_time=self.start_time,
            end_time=self.end_time,
            story=self.story,
            success=self.success,
        )

    @staticmethod
    def from_initial_data(initial_data: ExperimentInitialData):
        return ExperimentTable(
            label=initial_data.label,
            script=initial_data.script,
            start_time=initial_data.start_time,
            user=initial_data.user,
            story=initial_data.story,
        )


class ResultDataType(enum.Enum):
    """Numeric values"""

    Pickled = 1
    String = 2
    Npy = 3


class ResultTable(Base):
    __tablename__: str = "Results"

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    stage = Column(Integer)
    story = Column(String)
    label = Column(String)
    time = Column(DATETIME, nullable=False)
    data = Column(BLOB)
    data_type = Column(Enum(ResultDataType))
    saved_in_hdf5 = Column(Boolean, nullable=False, default=False)

    def __repr__(self):
        return f"<Result(id='{self.id}')>"

    def to_record(self):
        data = _decode_serialized_data(self.data, self.data_type)
        return ResultRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            label=self.label,
            story=self.story,
            stage=self.stage,
            data=data,
            time=self.time,
        )

    @staticmethod
    def from_model(experiment_id: int, result: RawResultData):
        data_type, serialized_data = _encode_serialized_data(result.data)
        return ResultTable(
            experiment_id=experiment_id,
            stage=result.stage,
            story=result.story,
            label=result.label,
            time=datetime.now(),
            data=serialized_data,
            data_type=data_type,
        )


class MetadataTable(Base):
    __tablename__ = "ExperimentMetadata"

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    stage = Column(Integer)
    label = Column(String)
    time = Column(DATETIME, nullable=False)
    data = Column(BLOB)
    data_type = Column(Enum(ResultDataType))
    saved_in_hdf5 = Column(Boolean, nullable=False, default=False)

    def __repr__(self):
        return f"<Metadata(id='{self.id}')>"

    def to_record(self):
        data = _decode_serialized_data(self.data, self.data_type)
        return MetadataRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            label=self.label,
            stage=self.stage,
            data=data,
            time=self.time,
        )

    @staticmethod
    def from_model(experiment_id: int, metadata: Metadata):
        data_type, serialized_data = _encode_serialized_data(metadata.data)
        return MetadataTable(
            experiment_id=experiment_id,
            stage=metadata.stage,
            label=metadata.label,
            time=datetime.now(),
            data=serialized_data,
            data_type=data_type,
        )


class NodeTable(Base):
    __tablename__ = "Nodes"
    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    stage_id = Column(Integer)
    label = Column(String)
    start = Column(DATETIME, nullable=False)
    is_key_node = Column(Boolean)

    def __repr__(self):
        return f"<Node(id='{self.id}')>"

    @staticmethod
    def from_model(experiment_id: int, node_data: NodeData):
        return NodeTable(
            experiment_id=experiment_id,
            stage_id=node_data.stage_id,
            start=node_data.start_time,
            label=node_data.label,
            is_key_node=node_data.is_key_node,
        )


class PlotTable(Base):
    __tablename__ = "Plots"

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    plot_data = Column(BLOB)
    data_type = Column(Enum(ResultDataType))
    generator_module = Column(String)
    generator_class = Column(String)
    time = Column(DATETIME)
    label = Column(String)
    story = Column(String)

    def __repr__(self):
        return f"<Plot(id='{self.id}')>"

    def to_record(self) -> PlotRecord:
        data = _decode_serialized_data(self.plot_data, self.data_type)
        generator = _get_class(self.generator_module, self.generator_class)
        return PlotRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            label=self.label,
            story=self.story,
            plot_data=data,
            generator=generator(),
        )

    @staticmethod
    def from_model(experiment_id: int, plot: PlotSpec, data: Any):
        data_type, serialized_data = _encode_serialized_data(data)
        return PlotTable(
            experiment_id=experiment_id,
            plot_data=serialized_data,
            data_type=data_type,
            generator_module=plot.generator.__module__,
            generator_class=plot.generator.__qualname__,
            time=datetime.now(),
            label=plot.label,
            story=plot.story,
        )


class FigureTable(Base):
    __tablename__ = "Figures"
    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    figure = Column(String)
    time = Column(DATETIME)

    def __repr__(self):
        return f"<FigureTable(id='{self.id}')>"

    def to_record(self) -> FigureRecord:
        return FigureRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            figure=from_json(self.figure),
            time=self.time,
        )

    @staticmethod
    def from_model(experiment_id: int, figure: go.Figure):
        return FigureTable(
            experiment_id=experiment_id,
            figure=to_json(figure),
            time=datetime.now(),
        )


class DebugTable(Base):
    __tablename__ = "Debug"

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    python_env = Column(String)
    python_history = Column(String)
    station_specs = Column(String)
    extra = Column(String)

    def __repr__(self):
        return f"<Debug(id='{self.id}')>"

    def to_record(self):
        return DebugRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            python_env=self.python_env,
            python_history=self.python_history,
            station_specs=self.station_specs,
            extra=self.extra,
        )

    @staticmethod
    def from_model(experiment_id: int, debug: Debug):
        return DebugTable(
            experiment_id=experiment_id,
            python_env=debug.python_env,
            python_history=debug.python_history,
            station_specs=debug.station_specs,
            extra=debug.extra,
        )
