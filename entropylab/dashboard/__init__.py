import logging
import os
import sys
from typing import Optional

import hupper
import waitress
from hupper.interfaces import ILogger

from entropylab.config import settings
from entropylab.dashboard.app import build_dashboard_app
from entropylab.logger import logger


def serve_dashboard(
    path: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    debug: Optional[bool] = None,
):
    """Serves our "dashboard" app using waitress and opens it in a browser.

    :param path: The path to the Entropy project to connect to the dashboard.
    :param host: The host name from which to server the app. Defaults to "127.0.0.1".
    :param port: The port name from which to server the app. Defaults to 8050.
    :param debug: Start the dashboard in debug mode. Defaults to False.
    """
    if debug:
        file_handler = logging.FileHandler(f"{path}\\.entropy\\spam.log")
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)

        _set_to_debug(logger, file_handler)
        _set_to_debug(logging.getLogger("waitress"), file_handler)
        _set_to_debug(logging.getLogger("hupper"), file_handler)
        hupper_logger = HupperLogger(logging.DEBUG)
    else:
        hupper_logger = None

    if host is None:
        host = settings.get("dashboard.host", "127.0.0.1")
    if port is None:
        port = settings.get("dashboard.port", 8050)
    if debug is None:
        debug = settings.get("dashboard.debug", False)

    sys.path.append(os.path.abspath(path))

    # noinspection PyShadowingNames
    app = build_dashboard_app(path)

    app.enable_dev_tools(debug=True)

    # Hot reloading using hupper
    worker_kwargs = dict(path=path, host=host, port=port, debug=debug)
    hupper.start_reloader(
        "entropylab.dashboard.serve_dashboard",
        worker_kwargs=worker_kwargs,
        reload_interval=0,
        logger=hupper_logger,
    )
    waitress.serve(app.server, host=host, port=port)


# noinspection PyShadowingNames
def _set_to_debug(logger: logging.Logger, file_handler):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)


class HupperLogger(ILogger):
    def __init__(self, level):
        self.logger = logging.getLogger("hupper")
        logger.setLevel(level)

    def error(self, msg):
        self.logger.error(msg)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)
