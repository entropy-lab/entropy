import logging
import os
import sys
from typing import Optional

import hupper
import waitress

from entropylab.config import settings
from entropylab.dashboard.app import build_dashboard_app


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
    if host is None:
        host = settings.get("dashboard.host", "127.0.0.1")
    if port is None:
        port = settings.get("dashboard.port", 8050)
    if debug is None:
        debug = settings.get("dashboard.debug", False)

    sys.path.append(os.path.abspath(path))

    app = build_dashboard_app(path)

    app.enable_dev_tools(debug=True)

    if debug:
        entropy_logger = logging.getLogger("entropy")
        entropy_logger.setLevel(logging.DEBUG)
        waitress_logger = logging.getLogger("waitress")
        waitress_logger.setLevel(logging.DEBUG)

    # Hot reloading using hupper
    worker_kwargs = dict(path=path, host=host, port=port, debug=debug)
    hupper.start_reloader(
        "entropylab.dashboard.serve_dashboard",
        worker_kwargs=worker_kwargs,
        reload_interval=0,
    )

    waitress.serve(app.server, host=host, port=port)
