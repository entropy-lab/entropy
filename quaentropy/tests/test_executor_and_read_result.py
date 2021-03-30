import os
from datetime import datetime

from quaentropy import ScriptExperiment
from quaentropy.api.data_writer import RawResultData
from quaentropy.api.execution import EntropyContext
from quaentropy.instruments.lab_topology import LabTopology
from quaentropy.results_backend.sqlalchemy.db import SqlAlchemyDB
from quaentropy.tests.mock_instruments import MockScope

repeats = 30


def an_experiment(experiment: EntropyContext):
    scope = experiment.get_resource("scope_1")
    scope.get_trig()
    for i in range(repeats):
        experiment.add_result(
            RawResultData("a_result", 5 + i + datetime.now().microsecond)
        )
        experiment.add_result(
            RawResultData("b_result", 6 + i + datetime.now().microsecond)
        )
    micro = datetime.now().microsecond
    experiment.add_result(
        RawResultData(
            "plot",
            [
                [
                    1 * micro,
                    2 * micro,
                    3 * micro,
                    4 * micro,
                    5 * micro,
                    6 * micro,
                    7 * micro,
                    8 * micro,
                ],
                [0, 1, 2, 3, 4, 5, 6, 7],
            ],
        )
    )


def test_running_db_and_topology():
    try:
        db = SqlAlchemyDB("test_running_db_and_topology.db")
        topology = LabTopology(db)
        topology.add_resource_if_not_exist("scope_1", MockScope, "scope_1", "1.1.1.1")
        definition = ScriptExperiment(topology, an_experiment, "with_db")
        # run twice
        definition.run(db)
        experiment_runner = definition.run(db)
        reader = experiment_runner.results_reader()
        print(reader.get_experiment_data())
        print(reader.get_experiment_data().script.print_all())
        total_results_in_experiments = repeats * 2 + 1
        assert len(list(reader.get_results())) == total_results_in_experiments
        assert len(list(reader.get_results("a_result"))) == repeats * 1
        assert len(list(db.get_results(label="a_result"))) == repeats * 2
        custom_result = db.custom_query("select * from Results")
        assert len(custom_result) == total_results_in_experiments * 2
    finally:
        os.remove("test_running_db_and_topology.db")
        pass
