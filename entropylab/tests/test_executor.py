from datetime import datetime

import pytest

from entropylab.api.data_writer import PlotSpec
from entropylab.api.execution import EntropyContext
from entropylab.api.plot import CirclePlotGenerator, LinePlotGenerator
from entropylab.instruments.lab_topology import LabResources, ExperimentResources
from entropylab.results_backend.sqlalchemy.db import SqlAlchemyDB
from entropylab.script_experiment import Script, script_experiment
from entropylab.tests.mock_instruments import MockScope


def do_something():
    rest = 5
    print(f"a resting for {rest}")
    return rest


def do_something2():
    rest = 3
    print(f"b resting for {rest}")
    return rest


def an_experiment(context: EntropyContext):
    scope = context.get_resource("scope_1")
    print(scope.extra)
    a1 = do_something()
    scope.get_trig()
    for i in range(30):
        context.add_result("a_result" + str(i), a1 + i + datetime.now().microsecond)

        b1 = do_something2()
        context.add_result("b_result" + str(i), b1 + i + datetime.now().microsecond)

    micro = datetime.now().microsecond
    context.add_result(
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


def an_experiment_with_plot(experiment: EntropyContext):
    scope = experiment.get_resource("scope_1")
    a1 = do_something()
    scope.get_trig()
    for i in range(30):
        experiment.add_result("a_result" + str(i), a1 + i + datetime.now().microsecond)

        b1 = do_something2()
        experiment.add_result("b_result" + str(i), b1 + i + datetime.now().microsecond)

    micro = datetime.now().microsecond
    experiment.add_plot(
        PlotSpec(
            label="plot",
            story="created this plot in experiment",
            generator=CirclePlotGenerator,
        ),
        data=[
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

    experiment.add_plot(
        PlotSpec(
            label="another plot",
            story="just showing off now",
            generator=LinePlotGenerator,
        ),
        data=[
            [1, 2, 3, 4, 5, 6, 7, 8],
            [4, 5, 6, 7, 0, 1, 2, 3],
        ],
    )


def test_running_no_db_no_runner():
    experiment = Script(None, do_something, "no_db").run()
    reader = experiment.results
    print(reader.get_experiment_info())


def test_running_no_db():
    resources = ExperimentResources()
    resources.add_temp_resource("scope_1", MockScope("1.1.1.1", ""))
    handle = Script(resources, an_experiment, "no_db").run()
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_results("a_result"))


@pytest.mark.repeat(3)
def test_running_db(project_dir_path):
    resources = ExperimentResources()
    resources.add_temp_resource("scope_1", MockScope("1.1.1.1", ""))
    db = SqlAlchemyDB(project_dir_path)

    definition = Script(resources, an_experiment, "with_db")

    handle = definition.run(db)
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())

    db = SqlAlchemyDB(project_dir_path)
    definition = Script(resources, an_experiment_with_plot, "with_db")
    definition.run(db)


def test_running_db_and_topology(project_dir_path):
    # setup lab environment one time
    db = SqlAlchemyDB(project_dir_path)
    lab = LabResources(db)
    lab.register_resource(
        "scope_1",
        MockScope,
        args=["1.1.1.1"],
        experiment_args=["blah"],
    )
    lab.register_resource_if_not_exist(
        "scope_1", MockScope, args=["1.1.1.1"], experiment_args=["blah"]
    )
    # lab.update_resource("scope_1", "scope_1", "1.1.1.1", experiment_args=["blah"])
    # lab.remove_resource("scope_1")
    lab.all_resources()
    print(lab.get_resource_info("scope_1"))

    # setup experiment
    resources = ExperimentResources(db)
    resources.import_lab_resource(
        "scope_1",
        experiment_args=["blah1"],
    )
    resources.add_temp_resource("temp_scope", MockScope("2.2.2.2", "blah"))

    # revert resource to snapshot if needed
    lab.get_resource("scope_1", experiment_args=["blah1"])
    lab.save_snapshot("scope_1", "snapshot_name")
    snap = lab.get_snapshot("scope_1", "snapshot_name")
    scope = resources.get_resource("scope_1")
    scope.revert_to_snapshot(snap)  # if implemented

    # add resource in a specific snapshot
    # resources.import_lab_resource(
    #     "scope_1", snapshot_name="snapshot_name", experiment_args=["blah2"]
    # )

    # define experiment
    definition = Script(resources, an_experiment, "with_db")
    handle = definition.run(db)
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())

    # setup experiment
    resources = ExperimentResources(db)
    resources.import_lab_resource("scope_1", experiment_args=["blah2"])
    # define experiment
    definition = Script(resources, an_experiment, "with_db")
    handle = definition.run(db)
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())

    # setup experiment
    resources = ExperimentResources(db)
    resources.import_lab_resource("scope_1", experiment_args=["blah3"])
    # define experiment
    definition = Script(resources, an_experiment, "with_db")
    handle = definition.run(db)
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())


def test_running_db_and_topology_with_kwargs(project_dir_path):
    # setup lab environment one time
    db = SqlAlchemyDB(project_dir_path)
    lab = LabResources(db)
    lab.register_resource(
        "scope_1",
        MockScope,
        kwargs={"address": "1.1.1.1"},
        experiment_kwargs={"extra": "blah"},
    )

    resources = ExperimentResources(db)
    resources.import_lab_resource(
        "scope_1",
        experiment_kwargs={"extra": "blah1"},
    )

    # define experiment
    definition = Script(resources, an_experiment, "with_db")
    handle = definition.run(db)
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())

    # setup experiment
    resources = ExperimentResources(db)
    resources.import_lab_resource(
        "scope_1",
        experiment_kwargs={"extra": "blah2"},
    )
    # define experiment
    definition = Script(resources, an_experiment, "with_db")
    handle = definition.run(db)
    reader = handle.results
    print(reader.get_experiment_info())
    print(reader.get_experiment_info().script.print_all())


def test_executor_decorator():
    resources = ExperimentResources()
    resources.add_temp_resource("scope_1", MockScope("1.1.1.1", ""))
    resources.add_temp_resource("scope_2", MockScope("1.1.1.1", ""))
    db = SqlAlchemyDB()

    @script_experiment("the best", resources, db)
    def experiment(experiment_runner: EntropyContext):
        scope = experiment_runner.get_resource("scope_1")
        a1 = do_something()
        scope.get_trig()
        experiment_runner.add_result("a_result", a1)
        a2 = do_something()
        if a2 > 4:
            a2 = do_something()
        experiment_runner.add_result("a2", a2)
        b1 = do_something2()
        experiment_runner.add_result("b_result", b1)
