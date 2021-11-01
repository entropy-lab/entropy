import dash
import pandas as pd
from dash import html, dcc
from dash.dependencies import Input, Output
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

    _tabs = dcc.Tabs(
        id="plot-tabs",
        value="",
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
                            dcc.Tab(
                                label=f"Tab {plot.id}",
                                children=[
                                    html.H3(f"Plot {plot.id}"),
                                    dcc.Graph(
                                        id=f"graph-{plot.id}-tabs", figure=figure
                                    ),
                                ],
                            )
                        )
        return result

    # App layout

    app.layout = html.Div(
        children=[
            html.H1(_title),
            _table,
            html.Div(children=[_tabs, html.Div(id="overlay")]),
        ],
        className="main",
    )


if __name__ == "__main__":
    """ This is the dash app that hosts our results dashboard """
    _app = dash.Dash(__name__)
    init(_app, "tests")
    _app.run_server(debug=True)
