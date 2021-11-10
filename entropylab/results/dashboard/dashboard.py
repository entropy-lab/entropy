import dash
import pandas as pd
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from plotly import graph_objects as go
from plotly.subplots import make_subplots


from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results.dashboard.table import table
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

MAX_EXPERIMENTS_NUM = 10000
EXPERIMENTS_PAGE_SIZE = 6

_plot_figures = {}
_plot_ids_to_combine = []


def init(app, path):
    """Initialize the results dashboard Dash app to display an Entropy project

    :param app the Dash app to initialize with the dashboard contents
    :param path path where the Entropy project to be used resides."""

    # App setup

    _title = f"Entropy - {project_name(path)} [{project_path(path)}]"
    app.title = _title

    # Fetching data from DB

    dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
    experiments = pd.DataFrame(
        dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
    )
    # TODO: How to filter this column when its values are emoji?
    experiments["success"] = experiments["success"].apply(lambda x: "✔️" if x else "❌")
    records = experiments.to_dict("records")

    @app.callback(
        Output("plot-tabs", "children"),
        Input("experiments-table", "selected_row_ids"),
    )
    def render_plot_tabs_from_selected_experiments_table_rows(selected_row_ids):
        colors = ["red", "green", "blue", "hotpink", "purple", "maroon", "brown"]
        result = []
        if selected_row_ids:
            for exp_id in selected_row_ids:
                plots = dashboard_data_reader.get_plot_data(exp_id)
                # TODO: If plots is None, auto render results from last graph node
                for plot in plots:
                    if plot.generator:
                        plot_figure = go.Figure()
                        plot.generator.plot_plotly(
                            plot_figure,
                            plot.plot_data,
                            color=colors[len(result) % len(colors)],
                        )
                        _plot_figures[plot.id] = plot_figure
                        result.append(
                            dbc.Tab(
                                dcc.Graph(
                                    figure=plot_figure,
                                ),
                                label=f"Plot {plot.id}",
                                id=f"plot-tab-{plot.id}",
                                tab_id=f"plot-tab-{plot.id}",
                            )
                        )
        else:
            result = [
                dbc.Tab(
                    html.Div(
                        html.Div(
                            "Select an experiment above to display its plots here",
                            className="plot-tab-placeholder-text",
                        ),
                        className="plot-tab-placeholder",
                    ),
                    label=f"Plots",
                    tab_id="placeholder",
                )
            ]
            pass
        return result

    @app.callback(
        Output("aggregate-tab", "children"),
        Input("add-button", "n_clicks"),
        State("plot-tabs", "active_tab"),
    )
    def add_plot_to_combined_plot(n_clicks, active_tab):
        if active_tab:
            plot_id = int(active_tab.replace("plot-tab-", ""))
            if plot_id not in _plot_ids_to_combine:
                _plot_ids_to_combine.append(plot_id)
            combined_figure = make_subplots(specs=[[{"secondary_y": True}]])
            for _id in _plot_ids_to_combine:
                figure = _plot_figures[_id]
                combined_figure.add_trace(figure.data[0])
            return dcc.Graph(figure=combined_figure)
        else:
            return [html.Div()]

    @app.callback(
        Output("plot-tabs", "active_tab"),
        Input("plot-tabs", "children"),
    )
    def activate_last_plot_tab_when_tabs_are_changed(children):
        if len(children) > 0:
            last_tab = len(children) - 1
            return children[last_tab]["props"]["tab_id"]
        return 0

    # App layout

    app.layout = html.Div(
        className="main",
        children=[
            dbc.Row(
                dbc.Col(
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
                    width="8",
                ),
                className="bg-primary",
            ),
            dbc.Row(dbc.Col([html.H5("Experiments"), (table(records))], width="12")),
            dbc.Row(
                [
                    dbc.Col(
                        (
                            dbc.Tabs(
                                id="plot-tabs",
                            ),
                        ),
                        width="5",
                    ),
                    dbc.Col(
                        html.Div(
                            dbc.Button(
                                "Add >>",
                                id="add-button",
                                className="add-button",
                            ),
                            className="add-button-col-container",
                        ),
                        width="1",
                        className="add-button-col",
                    ),
                    dbc.Col(
                        dbc.Tabs(
                            id="aggregate-tabs",
                            children=[
                                dbc.Tab(
                                    dcc.Graph(),
                                    id="aggregate-tab",
                                    label="Aggregate",
                                ),
                            ],
                        ),
                        width="5",
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
