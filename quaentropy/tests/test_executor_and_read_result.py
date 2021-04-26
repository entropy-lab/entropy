import os
from datetime import datetime

from quaentropy import ScriptExperiment
from quaentropy.api.data_writer import RawResultData
from quaentropy.api.execution import EntropyContext
from quaentropy.instruments.lab_topology import LabResources, ExperimentResources
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
        db = SqlAlchemyDB("test_running_db_and_topo.db")
        topology = LabResources(db)
        topology.register_resource_if_not_exist(
            "scope_1", MockScope, args=["1.1.1.1", ""]
        )
        # topology.register_private_results_db(db)
        # topology.pause_save_to_results_db()
        # topology.resume_save_to_results_db()

        resources = ExperimentResources(db)
        resources.import_lab_resource("scope_1")

        definition = ScriptExperiment(resources, an_experiment, "with_db")
        # run twice
        definition.run()
        experiment = definition.run()
        reader = experiment.results_reader()
        print(reader.get_experiment_info())
        print(reader.get_experiment_info().script.print_all())
        total_results_in_experiments = repeats * 2 + 1
        assert len(list(reader.get_results())) == total_results_in_experiments
        assert len(list(reader.get_results("a_result"))) == repeats * 1
        assert len(list(db.get_results(label="a_result"))) == repeats * 2

        all_experiments = db.get_experiments()
        with_db_experiments = db.get_experiments(label="with_db")
        assert len(list(all_experiments)) == 2
        assert len(list(with_db_experiments)) == 2
        assert (
            len(list(db.get_experiments(start_after=list(all_experiments)[0].end_time)))
            == 1
        )
        custom_result = db.custom_query("select * from Results")
        assert len(custom_result) == total_results_in_experiments * 2
    finally:
        os.remove("test_running_db_and_topo.db")
        pass
