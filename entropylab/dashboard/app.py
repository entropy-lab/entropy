from dash import Dash, html, dcc, callback, Output, Input

from entropylab import SqlAlchemyDB
from entropylab.api.in_process_param_store import InProcessParamStore
from entropylab.dashboard.pages import results, params
from entropylab.dashboard.pages.results.dashboard_data import (
    SqlalchemyDashboardDataReader,
)
from entropylab.dashboard.theme import (
    theme_stylesheet,
)
from entropylab.results_backend.sqlalchemy.project import (
    project_name,
    project_path,
    param_store_path,
)


def build_dashboard_app(proj_path):
    """Initialize the dashboard Dash app to show an Entropy project

    :param proj_path path where the Entropy project to be used resides."""

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

    """ Initializing data sources """

    dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(proj_path))
    param_store = InProcessParamStore(param_store_path(proj_path))

    """ Building page layouts """

    results_layout = results.layout.build_layout(proj_path, dashboard_data_reader)
    params_layout = params.layout.build_layout(proj_path, param_store)

    """ Registering callbacks used in pages """

    results.callbacks.register_callbacks(app, dashboard_data_reader)
    params.callbacks.register_callbacks(app, param_store)

    """ Callback to route between page layouts based on URL """

    @callback(Output("page-content", "children"), Input("url", "pathname"))
    def display_page(pathname):
        if pathname == "/":
            return results_layout
        elif pathname == "/params":
            return params_layout
        else:
            return "404"

    return app
