from entropylab.results.dashboard.dashboard import build_dashboard_app
from entropylab.results.dashboard.theme import theme_stylesheet


def serve_dashboard(path: str, debug: bool):
    """Serves our "dashboard" app from dash and opens it in a browser

    :param path: The path to the Entropy project to connect to the dashboard
    :param debug: Start the dashboard in debug mode
    """
    app = build_dashboard_app(path)
    app.run_server(debug=debug)
