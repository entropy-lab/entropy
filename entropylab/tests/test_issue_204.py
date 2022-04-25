import os
from datetime import datetime

import pytest

from entropylab import ExperimentResources, SqlAlchemyDB, PyNode, Graph


@pytest.mark.skipif(
    datetime.utcnow() > datetime(2022, 6, 25),
    reason="Please remove after two months have passed since the fix was merged",
)
def test_issue_204(initialized_project_dir_path, capsys):
    # arrange

    # remove DB files because when they are present, issue does not occur
    db_files = [".entropy/params.db", ".entropy/entropy.db", ".entropy/entropy.hdf5"]
    for file in db_files:
        full_path = os.path.join(initialized_project_dir_path, file)
        if os.path.exists(full_path):
            os.remove(full_path)

    # experiment to run
    experiment_resources = ExperimentResources(
        SqlAlchemyDB(initialized_project_dir_path)
    )

    def root_node():
        print("root node")
        # error that should be logged to stderr:
        print(a)
        return {}

    node0 = PyNode(label="root_node", program=root_node)
    experiment = Graph(resources=experiment_resources, graph={node0}, story="run_a")

    # act

    try:
        experiment.run()
    except RuntimeError:
        pass

    # assert

    captured = capsys.readouterr()
    assert "message: name 'a' is not defined" in captured.err
