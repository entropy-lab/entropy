from dash import Dash, html, dcc, callback, Output, Input

from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.pages.param_store.page_layout import (
    build_layout as build_param_store_layout,
)
from entropylab.results.dashboard.pages.results.callbacks import register_callbacks
from entropylab.results.dashboard.pages.results.dashboard_data import (
    SqlalchemyDashboardDataReader,
)
from entropylab.results.dashboard.pages.results.page_layout import (
    build_layout as build_results_layout,
)
from entropylab.results.dashboard.theme import (
    theme_stylesheet,
)
from entropylab.results_backend.sqlalchemy.project import project_name, project_path


def build_dashboard_app(proj_path):
    """Initialize the dashboard Dash app to show an Entropy project

    :param proj_path path where the Entropy project to be used resides."""

    """ Data source for our app """

    dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(proj_path))

    """ Creating and setting up the Dash app """

    app = Dash(
        __name__,
        external_stylesheets=[theme_stylesheet],
        update_title=None,
        suppress_callback_exceptions=True,
    )
    app.title = f"Entropy - {project_name(proj_path)} [{project_path(proj_path)}]"

    app.layout = html.Div(
        [dcc.Location(id="url", refresh=False), html.Div(id="page-content")]
    )

    """ Registering callbacks used in pages """

    register_callbacks(app, dashboard_data_reader)

    """ Callback to route between page layouts based on URL """

    results_layout = build_results_layout(proj_path, dashboard_data_reader)
    param_store_layout = build_param_store_layout(proj_path, dashboard_data_reader)

    @callback(Output("page-content", "children"), Input("url", "pathname"))
    def display_page(pathname):
        if pathname == "/":
            return results_layout
        elif pathname == "/param_store":
            return param_store_layout
        else:
            return "404"

    return app
