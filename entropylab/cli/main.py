import argparse

from entropylab.results import serve_results
from entropylab.results_backend.sqlalchemy import init_db, upgrade_db


def init(args: argparse.Namespace):
    init_db(args.directory)
    pass


def serve(args: argparse.Namespace):
    serve_results(args.directory)


def update(args: argparse.Namespace):
    upgrade_db(args.directory)


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    directory_arg = {
        "help": "path to a directory containing Entropy project",
        "nargs": "?",
        "default": ".",
    }

    # init
    init_parser = subparsers.add_parser("init", help="initialize a new Entropy project")
    init_parser.add_argument("directory", **directory_arg)
    init_parser.set_defaults(func=init)

    # update
    update_parser = subparsers.add_parser(
        "update", help="update an Entropy project to the latest version"
    )
    update_parser.add_argument("directory", **directory_arg)
    update_parser.set_defaults(func=update)

    # serve
    serve_parser = subparsers.add_parser(
        "serve",
        help="launch results server in a new browser window",
    )
    serve_parser.add_argument("directory", **directory_arg)
    serve_parser.set_defaults(func=serve)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
