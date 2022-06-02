import argparse
import importlib

from entropylab.flame.workflow import Workflow

__all__ = ["_get_workflow", "workflow_json", "workflow_summary"]


def _get_workflow(workflow_path):
    workflow_path = workflow_path.replace(".py", "")
    importlib.import_module(workflow_path)
    workflow = Workflow._main_workflow()
    # now prevent workflow to overwrite parameters on exit
    Workflow._prevent_parameter_file_overwrite()
    return workflow


def workflow_json(workflow_path):
    workflow = _get_workflow(workflow_path)

    return workflow._to_json()


def workflow_summary(workflow_path):
    workflow = _get_workflow(workflow_path)
    return workflow._summary_json()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Utilities for workflow")
    parser.add_argument(
        "function",
        type=str,
        default="workflow_json",
        help=" workflow_json = JSON with Cytoscape graph representation;"
        " workflow_summary = JSON with all indexable data for full-text search about workflow",
    )
    parser.add_argument(
        "-w",
        "--workflow",
        type=str,
        default="workflow.py",
        help="Python file that defines main entropylab.Workflow (default workflow.py)",
    )

    args = parser.parse_args()
    if args.function == "workflow_json":
        print(workflow_json(args.workflow))
    elif args.function == "workflow_summary":
        print(workflow_summary(args.workflow))
    else:
        print("ERROR")
