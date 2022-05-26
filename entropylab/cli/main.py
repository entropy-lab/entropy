import argparse
import functools
import sys

import pkg_resources

from entropylab.dashboard import serve_dashboard
from entropylab.logger import logger
from entropylab.pipeline.results_backend.sqlalchemy import init_db, upgrade_db


# Decorator for friendly error messages


def command(func: callable) -> callable:
    """Decorator that runs commands. On error, prints friendly message when possible"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except RuntimeError as re:
            command_name = func.__name__
            logger.exception(
                "RuntimeError in Entropy CLI command %s, args: %s", command_name, args
            )
            print(re, file=sys.stderr)
            sys.exit(-1)

    return wrapper


# CLI command functions


@command
def init(args: argparse.Namespace):
    init_db(args.directory)


@command
def upgrade(args: argparse.Namespace):
    upgrade_db(args.directory)


@command
def serve(args: argparse.Namespace):
    serve_dashboard(args.directory, args.host, args.port, args.debug)


# The parser


def _build_parser():
    parser = argparse.ArgumentParser()
    # in case no arguments were supplied:
    parser.set_defaults(func=lambda args: parser.print_help())
    subparsers = parser.add_subparsers()

    directory_arg = {
        "help": "path to a directory containing Entropy project",
        "nargs": "?",
        "default": ".",
    }

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s " + pkg_resources.get_distribution("entropylab").version,
    )

    # init
    init_parser = subparsers.add_parser("init", help="initialize a new Entropy project")
    init_parser.add_argument("directory", **directory_arg)
    init_parser.set_defaults(func=init)

    # upgrade
    upgrade_parser = subparsers.add_parser(
        "upgrade", help="upgrade an Entropy project to the latest version"
    )
    upgrade_parser.add_argument("directory", **directory_arg)
    upgrade_parser.set_defaults(func=upgrade)

    # serve
    serve_parser = subparsers.add_parser(
        "serve", help="serve & launch the results dashboard app in a browser"
    )
    serve_parser.add_argument("directory", **directory_arg)
    serve_parser.add_argument(
        "host",
        help="host name from which to serve the dashboard app",
        nargs="?",
        default=None,
    )
    serve_parser.add_argument(
        "port",
        help="port number from which to serve the dashboard app",
        nargs="?",
        default=None,
    )
    serve_parser.add_argument("--debug", dest="debug", action="store_true")
    serve_parser.set_defaults(func=serve, debug=False)

    return parser


# main


def main():
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
