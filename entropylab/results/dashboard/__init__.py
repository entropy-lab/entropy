import logging

from waitress import serve

from entropylab.config import settings
from entropylab.results.dashboard.dashboard import build_dashboard_app
from entropylab.results.dashboard.theme import theme_stylesheet


def serve_dashboard(path: str, debug: bool = None):
    """Serves our "dashboard" app from dash and opens it in a browser

    :param path: The path to the Entropy project to connect to the dashboard
    :param debug: Start the dashboard in debug mode
    """
    port = settings.get("dashboard.port", 8050)
    host = settings.get("dashboard.host", "127.0.0.1")
    log_level = settings.get("dashboard.log_level", logging.INFO)
    if debug is None:
        debug = settings.get("dashboard.debug", False)

    app = build_dashboard_app(path)
    app.enable_dev_tools(debug=debug)

    logger = logging.getLogger("waitress")
    logger.setLevel(log_level)

    serve(app.server, host=host, port=port)
