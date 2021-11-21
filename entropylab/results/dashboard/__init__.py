import dash

from entropylab.results.dashboard.dashboard import init
from entropylab.results.dashboard.theme import theme_stylesheet


def serve_dashboard(path: str, debug: bool):
    """Serves our Dash "dashboard" app in a web server

    :param path: The path to the Entropy project to connect to the dashboard
    :param debug: Start the dashboard in debug mode
    """
    _app = dash.Dash(__name__, external_stylesheets=[theme_stylesheet])
    init(_app, path)
    _app.run_server(debug=debug)
