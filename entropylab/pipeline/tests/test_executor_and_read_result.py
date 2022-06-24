from datetime import datetime

from entropylab.pipeline import Script
from entropylab.pipeline.api.execution import EntropyContext
from entropylab.components.lab_topology import LabResources, ExperimentResources
from entropylab.pipeline.results_backend.sqlalchemy.db import SqlAlchemyDB
from entropylab.pipeline.tests.mock_instruments import MockScope

repeats = 30


def an_experiment(experiment: EntropyContext):
    scope = experiment.get_resource("scope_1")
    scope.get_trig()
    for i in range(repeats):
        experiment.add_result("a_result" + str(i), 5 + i + datetime.now().microsecond)

        experiment.add_result("b_result" + str(i), 6 + i + datetime.now().microsecond)

    micro = datetime.now().microsecond
    experiment.add_result(
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


def test_running_db_and_topology(project_dir_path):
    db = SqlAlchemyDB(project_dir_path, enable_hdf5_storage=False)
    topology = LabResources(db)
    topology.register_resource_if_not_exist("scope_1", MockScope, args=["1.1.1.1", ""])
    # topology.register_private_results_db(db)
    # topology.pause_save_to_results_db()
    # topology.resume_save_to_results_db()

    resources = ExperimentResources(db)
    resources.import_lab_resource("scope_1")

    definition = Script(resources, an_experiment, "with_db")
    # run twice
    definition.run()
    reader = definition.run().results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())
    total_results_in_experiments = repeats * 2 + 1
    assert len(list(reader.get_results())) == total_results_in_experiments
    for i in range(repeats):
        assert len(list(reader.get_results("a_result" + str(i)))) == 1
        assert len(list(db.get_results(label="a_result" + str(i)))) == 2

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
