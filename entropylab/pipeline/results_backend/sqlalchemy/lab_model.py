from sqlalchemy import (
    Column,
    Integer,
    String,
    DATETIME,
    ForeignKey,
    Enum,
    Boolean,
)

from entropylab.components.lab_topology import (
    DriverType,
)
from entropylab.pipeline.results_backend.sqlalchemy.model import Base


class Resources(Base):
    __tablename__ = "Resources"

    id = Column(Integer, primary_key=True)
    update_time = Column(DATETIME, nullable=False)
    name = Column(String)
    driver = Column(String)
    module = Column(String)
    class_name = Column(String)
    version = Column(String)
    driver_type = Column(Enum(DriverType))
    args = Column(String)
    kwargs = Column(String)
    deleted = Column(Boolean, default=False)
    number_of_experiment_args = Column(Integer)
    keys_of_experiment_kwargs = Column(String)
    cached_metadata = Column(String)


class ResourcesSnapshots(Base):
    __tablename__ = "ResourcesSnapshots"

    id = Column(Integer, primary_key=True)
    update_time = Column(DATETIME, nullable=False)
    name = Column(String)
    driver_id = Column(Integer, ForeignKey("Resources.id", ondelete="CASCADE"))
    state = Column(String)
