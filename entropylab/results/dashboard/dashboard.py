import dash
import pandas as pd
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from plotly import graph_objects as go

from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results.dashboard.table import table
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

MAX_EXPERIMENTS_NUM = 10000
EXPERIMENTS_PAGE_SIZE = 6


def init(app, path):
    """Initialize the results dashboard Dash app to display an Entropy project

    :param app the Dash app to initialize with the dashboard contents
    :param path path where the Entropy project to be used resides."""

    # App setup

    _title = f"Entropy - {project_name(path)} [{project_path(path)}]"
    app.title = _title

    # Fetching data from DB

    _dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
    _experiments = pd.DataFrame(
        _dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
    )
    # TODO: How to filter this column when its values are emoji?
    _experiments["success"] = _experiments["success"].apply(
        lambda x: "✔️" if x else "❌"
    )
    _records = _experiments.to_dict("records")

    # Components

    _table = table(app, _records)

    _tabs = dbc.Tabs(
        id="plot-tabs",
        # value="",
        children=[],
    )

    # Callbacks

    @app.callback(
        Output("plot-tabs", "children"),
        Input("experiments-table", "selected_row_ids"),
    )
    def render_plot_tabs_from_selected_experiments_table_rows(selected_row_ids):
        colors = ["red", "green", "blue", "hotpink", "purple", "maroon", "brown"]
        result = []
        if selected_row_ids:
            for exp_id in selected_row_ids:
                plots = _dashboard_data_reader.get_plot_data(exp_id)
                for plot in plots:
                    if plot.generator:
                        figure = go.Figure()
                        plot.generator.plot_plotly(
                            figure,
                            plot.plot_data,
                            color=colors[len(result) % len(colors)],
                        )
                        result.append(
                            dbc.Tab(
                                label=f"Plot {plot.id}",
                                children=[
                                    dcc.Graph(
                                        id=f"graph-{plot.id}-tabs",
                                        figure=figure,
                                        className="graph1",
                                    ),
                                ],
                            )
                        )
        if not result:
            result = [
                dcc.Tab(
                    label=f"Select experiment(s) above to display their plots here.",
                )
            ]
            pass
        return result

    # App layout

    app.layout = html.Div(
        className="main",
        children=[
            dbc.Nav(
                children=[
                    html.A(
                        html.Img(
                            id="entropy_logo",
                            src="/assets/images/entropy_logo_dark.svg",
                        ),
                        className="navbar-nav me-auto",
                        href="#",
                    ),
                    html.H3(
                        f"{project_name(path)} ",
                        className="navbar-nav me-auto",
                    ),
                    html.Small(
                        f"[{project_path(path)}]",
                        className="navbar-nav me-auto",
                    ),
                ],
                className="navbar navbar-expand-lg navbar-dark bg-primary",
            ),
            dbc.Row(dbc.Col([html.H5("Experiments"), _table], class_name="bg")),
            dbc.Row(
                [
                    dbc.Col([html.H5("Plots"), _tabs], class_name="bg", width="6"),
                    dbc.Col(
                        [html.H5("Overlay"), html.Div(id="overlay")],
                        class_name="bg",
                        width="6",
                    ),
                ],
            ),
        ],
    )


if __name__ == "__main__":
    """ This is the dash app that hosts our results dashboard """
    _app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
    init(_app, "tests")
    _app.run_server(debug=True)
