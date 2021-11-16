import json

import dash
import pandas as pd
from dash import html, dcc
from dash.dependencies import Input, Output, State, ALL
import dash_bootstrap_components as dbc
from plotly import graph_objects as go
from plotly.subplots import make_subplots


from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results.dashboard.layout import layout
from entropylab.results.dashboard.theme import (
    colors,
    theme_stylesheet,
    plot_legend_font_color,
    plot_paper_bgcolor,
    plot_plot_bgcolor,
    plot_font_color,
)
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
        result = []
        if selected_row_ids:
            for exp_id in selected_row_ids:
                plots = dashboard_data_reader.get_plot_data(exp_id)
                # TODO: If plots is None, auto render results from last graph node
                for plot in plots:
                    if plot.generator:
                        plot_figure = go.Figure()
                        color = colors[len(result) % len(colors)]
                        plot.generator.plot_plotly(
                            plot_figure,
                            plot.plot_data,
                            name=f"Plot {plot.id}",
                            color=color,
                            showlegend=False,
                        )
                        plot_figure.update_layout(
                            width=500,
                            font_color=plot_font_color,
                            legend_font_color=plot_legend_font_color,
                            paper_bgcolor=plot_paper_bgcolor,
                            plot_bgcolor=plot_plot_bgcolor,
                        )
                        _plot_figures[plot.id] = dict(figure=plot_figure, color=color)
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
                            className="tab-placeholder-text",
                        ),
                        className="tab-placeholder-container",
                    ),
                    label=f"Plots",
                    tab_id="plot-tab-placeholder",
                )
            ]
            pass
        return result

    @app.callback(
        Output("aggregate-tab", "children"),
        Output("remove-buttons", "children"),
        Input({"type": "remove-button", "index": ALL}, "n_clicks"),
        Input("add-button", "n_clicks"),
        State("plot-tabs", "active_tab"),
    )
    def add_or_remove_plot_in_combined_plot(n_clicks1, n_clicks2, active_tab):
        prop_id = dash.callback_context.triggered[0]["prop_id"]
        # trigger was a click on the "Add >>" button
        if prop_id == "add-button.n_clicks" and active_tab:
            active_plot_id = int(active_tab.replace("plot-tab-", ""))
            if active_plot_id not in _plot_ids_to_combine:
                _plot_ids_to_combine.append(active_plot_id)
            return build_aggregate_graph_and_remove_buttons()
        # trigger was a click on one of the remove buttons
        elif "remove-button" in prop_id:
            id_dict = json.loads(prop_id.replace(".n_clicks", ""))
            remove_plot_id = id_dict["index"]
            if remove_plot_id in _plot_ids_to_combine:
                _plot_ids_to_combine.remove(remove_plot_id)
            return build_aggregate_graph_and_remove_buttons()
        # default case
        else:
            return [build_aggregate_tab_placeholder()], [html.Div()]

    def build_aggregate_graph_and_remove_buttons():
        combined_figure = make_subplots(specs=[[{"secondary_y": True}]])
        combined_figure.update_layout(
            width=500,
            font_color=plot_font_color,
            legend_font_color=plot_legend_font_color,
            paper_bgcolor=plot_paper_bgcolor,
            plot_bgcolor=plot_plot_bgcolor,
            showlegend=True,
        )
        remove_buttons = []
        for plot_id in _plot_ids_to_combine:
            figure = _plot_figures[plot_id]["figure"]
            color = _plot_figures[plot_id]["color"]
            combined_figure.add_trace(figure.data[0])
            button = build_remove_button(plot_id, color)
            remove_buttons.append(button)
        return dcc.Graph(figure=combined_figure), remove_buttons

    def build_remove_button(plot_id, color):
        return dbc.Button(
            f"Plot {plot_id} ✖️",
            style={"background-color": color},
            class_name="remove-button",
            id={"type": "remove-button", "index": plot_id},
        )

    def build_aggregate_tab_placeholder():
        return html.Div(
            html.Div(
                "Add a plot on the left to aggregate it here",
                className="tab-placeholder-text",
            ),
            className="tab-placeholder-container",
        )

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
    app.layout = layout(path, records)


if __name__ == "__main__":
    """ This is the dash app that hosts our results dashboard """
    _app = dash.Dash(__name__, external_stylesheets=[theme_stylesheet])
    init(_app, "tests")
    _app.run_server(debug=True)
