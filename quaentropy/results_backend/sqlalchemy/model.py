import enum
import pickle
from datetime import datetime

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

from quaentropy.api.data_reader import (
    ExperimentRecord,
    ScriptViewer,
    ResultRecord,
    MetadataRecord,
    DebugRecord,
    PlotRecord,
)
from quaentropy.api.data_writer import (
    ExperimentInitialData,
    RawResultData,
    Metadata,
    Debug,
    Plot,
    PlotDataType,
    NodeData,
)

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
        return f"<Experiment(id='{self.id}')>"

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
    Pickled = 1
    String = 2


class ResultTable(Base):
    __tablename__ = "Results"

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    stage = Column(Integer)
    story = Column(String)
    label = Column(String)
    time = Column(DATETIME, nullable=False)
    data = Column(BLOB)
    data_type = Column(Enum(ResultDataType))

    def __repr__(self):
        return f"<Result(id='{self.id}')>"

    def to_record(self):
        if self.data_type == ResultDataType.Pickled:
            data = pickle.loads(self.data)
        else:
            data = self.data.decode()
        return ResultRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            label=self.label,
            story=self.story,
            stage=self.stage,
            data=data,
        )

    @staticmethod
    def from_model(experiment_id: int, result: RawResultData):
        # if isinstance(result.data, (np.ndarray, np.generic) ):
        # TODO using repr
        data_type = ResultDataType.Pickled
        try:
            serialized_data = pickle.dumps(result.data)
        except Exception:
            serialized_data = result.data.__repr__().encode(encoding="UTF-8")
            data_type = ResultDataType.String
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

    def __repr__(self):
        return f"<Metadata(id='{self.id}')>"

    def to_record(self):
        return MetadataRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            label=self.label,
            stage=self.stage,
            data=pickle.loads(self.data),
        )

    @staticmethod
    def from_model(experiment_id: int, metadata: Metadata):
        serialized_data = pickle.dumps(metadata.data)
        return MetadataTable(
            experiment_id=experiment_id,
            stage=metadata.stage,
            label=metadata.label,
            time=datetime.now(),
            data=serialized_data,
        )


class NodeTable(Base):
    __tablename__ = "Nodes"

    experiment_id = Column(
        Integer, ForeignKey("Experiments.id", ondelete="CASCADE"), primary_key=True
    )
    id = Column(Integer, primary_key=True)
    label = Column(String)
    start = Column(DATETIME, nullable=False)

    def __repr__(self):
        return f"<Node(exp_id='{self.experiment_id}', id='{self.id}')>"

    @staticmethod
    def from_model(experiment_id: int, node_data: NodeData):
        return NodeTable(
            experiment_id=experiment_id,
            id=node_data.node_id,
            start=node_data.start_time,
            label=node_data.label,
        )


class PlotTable(Base):
    __tablename__ = "Plots"

    id = Column(Integer, primary_key=True)
    experiment_id = Column(Integer, ForeignKey("Experiments.id", ondelete="CASCADE"))
    plot_data = Column(BLOB)
    data_type = Column(Enum(PlotDataType))
    bokeh_generator = Column(BLOB)
    time = Column(DATETIME)
    label = Column(String)
    story = Column(String)

    def __repr__(self):
        return f"<Plot(id='{self.id}')>"

    def to_record(self) -> PlotRecord:
        return PlotRecord(
            experiment_id=self.experiment_id,
            id=self.id,
            label=self.label,
            story=self.story,
            plot_data=pickle.loads(self.plot_data),
            data_type=self.data_type,
            bokeh_generator=pickle.loads(self.bokeh_generator),
        )

    @staticmethod
    def from_model(experiment_id: int, plot: Plot):
        try:
            plot_data = pickle.dumps(plot.data)
        except BaseException:
            plot_data = None
        try:
            generator = pickle.dumps(plot.bokeh_generator)
        except BaseException:
            generator = None
        return PlotTable(
            experiment_id=experiment_id,
            plot_data=plot_data,
            data_type=plot.data_type,
            bokeh_generator=generator,
            time=datetime.now(),
            label=plot.label,
            story=plot.story,
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
