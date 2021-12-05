import json
from typing import Dict

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output, State, ALL
from plotly import graph_objects as go
from plotly.subplots import make_subplots

from entropylab import SqlAlchemyDB
from entropylab.api.data_reader import PlotRecord
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results.dashboard.layout import layout
from entropylab.results.dashboard.theme import (
    colors,
    dark_plot_layout,
    theme_stylesheet,
)
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

MAX_EXPERIMENTS_NUM = 10000
EXPERIMENTS_PAGE_SIZE = 6


def build_dashboard_app(path):
    """Initialize the results dashboard Dash app to display an Entropy project

    :param path path where the Entropy project to be used resides."""

    _dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
    # _plot_figures = {}
    # _plot_keys_to_combine = []

    def serve_layout():
        """This function is called on "page load", fetching Experiment records
        from the project DB and embedding them in the app layout."""
        experiments = _dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
        # TODO: How to filter this column when its values are emoji?
        experiments["success"] = experiments["success"].apply(
            lambda x: "✔️" if x else "❌"
        )
        records = experiments.to_dict("records")
        return layout(path, records)

    _app = dash.Dash(__name__, external_stylesheets=[theme_stylesheet])
    _app.title = f"Entropy - {project_name(path)} [{project_path(path)}]"
    _app.layout = serve_layout

    """ CALLBACKS and their helper functions """

    @_app.callback(
        Output("plot-tabs", "children"),
        Output("plot-figures", "data"),
        Input("experiments-table", "selected_rows"),
        State("experiments-table", "data"),
        State("plot-figures", "data"),
    )
    def render_plot_tabs_from_selected_experiments_table_rows(
        selected_rows, data, plot_figures
    ):
        plot_figures = plot_figures or {}
        result = []
        failed_exp_ids = []
        if selected_rows:
            for row_num in selected_rows:
                exp_id = data[row_num]["id"]
                plots = _dashboard_data_reader.get_plot_data(exp_id)
                if plots:
                    for plot in plots:
                        if plot.generator:
                            color = colors[len(result) % len(colors)]
                            plot_tab, plot_figures = build_plot_tab_from_plot(
                                plot_figures, plot, color
                            )
                            result.append(plot_tab)
                        else:
                            failed_exp_ids.append(exp_id)
                else:
                    failed_exp_ids.append(exp_id)
        # TODO: Show notification to user that exp cannot be plotted (failed_exp_ids)
        if len(result) > 0:
            return result, plot_figures
        else:
            return [build_plot_tabs_placeholder()], plot_figures

    def build_plot_tabs_placeholder():
        return dbc.Tab(
            html.Div(
                html.Div(
                    "Select an experiment above to display its plots here",
                    className="tab-placeholder-text",
                ),
                className="tab-placeholder-container",
            ),
            label="Plots",
            tab_id="plot-tab-placeholder",
        )

    def build_plot_tab_from_plot(
        plot_figures, plot: PlotRecord, color: str
    ) -> (dbc.Tab, Dict):
        plot_key = f"{plot.experiment_id}/{plot.id}"
        plot_name = f"Plot {plot_key}"
        plot_figure = go.Figure()
        plot.generator.plot_plotly(
            plot_figure,
            plot.plot_data,
            name=plot_name,
            color=color,
            showlegend=False,
        )
        plot_figure.update_layout(dark_plot_layout)
        plot_figures[plot_key] = dict(figure=plot_figure, color=color)
        return build_plot_tab(plot_figure, plot_name, plot_key), plot_figures

    def build_plot_tab(
        plot_figure: go.Figure, plot_name: str, plot_key: str
    ) -> dbc.Tab:
        return dbc.Tab(
            dcc.Graph(figure=plot_figure, responsive=True),
            label=plot_name,
            id=f"plot-tab-{plot_key}",
            tab_id=f"plot-tab-{plot_key}",
        )

    @_app.callback(
        Output("aggregate-tab", "children"),
        Output("remove-buttons", "children"),
        Output("plot-keys-to-combine", "data"),
        Input({"type": "remove-button", "index": ALL}, "n_clicks"),
        Input("add-button", "n_clicks"),
        State("plot-tabs", "active_tab"),
        State("plot-figures", "data"),
        State("plot-keys-to-combine", "data"),
    )
    def add_or_remove_plot_in_combined_plot(
        n_clicks1, n_clicks2, active_tab, plot_figures, plot_keys_to_combine
    ):
        plot_keys_to_combine = plot_keys_to_combine or []
        prop_id = dash.callback_context.triggered[0]["prop_id"]
        # trigger was a click on the "Add >>" button
        if prop_id == "add-button.n_clicks" and active_tab:
            active_plot_key = active_tab.replace("plot-tab-", "")
            if active_plot_key not in plot_keys_to_combine:
                plot_keys_to_combine.append(active_plot_key)
            tabs, remove_buttons = build_aggregate_graph_and_remove_buttons(
                plot_figures, plot_keys_to_combine
            )
            return (
                tabs,
                remove_buttons,
                plot_keys_to_combine,
            )
        # trigger was a click on one of the remove buttons
        elif "remove-button" in prop_id:
            id_dict = json.loads(prop_id.replace(".n_clicks", ""))
            remove_plot_key = id_dict["index"]
            if remove_plot_key in plot_keys_to_combine:
                plot_keys_to_combine.remove(remove_plot_key)

                tabs, remove_buttons = build_aggregate_graph_and_remove_buttons(
                    plot_figures, plot_keys_to_combine
                )
                return (
                    tabs,
                    remove_buttons,
                    plot_keys_to_combine,
                )

        # default case
        else:
            return (
                [build_aggregate_tab_placeholder()],
                [html.Div()],
                plot_keys_to_combine,
            )

    def build_aggregate_graph_and_remove_buttons(plot_figures, plot_keys_to_combine):
        combined_figure = make_subplots(specs=[[{"secondary_y": True}]])
        remove_buttons = []
        for plot_id in plot_keys_to_combine:
            figure = plot_figures[plot_id]["figure"]
            color = plot_figures[plot_id]["color"]
            combined_figure.add_trace(figure["data"][0])
            button = build_remove_button(plot_id, color)
            remove_buttons.append(button)
        combined_figure.update_layout(dark_plot_layout)
        return dcc.Graph(figure=combined_figure, responsive=True), remove_buttons

    def build_remove_button(plot_id, color):
        return dbc.Button(
            f"{plot_id} ✖️",
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

    @_app.callback(
        Output("plot-tabs", "active_tab"),
        Input("plot-tabs", "children"),
    )
    def activate_last_plot_tab_when_tabs_are_changed(children):
        if len(children) > 0:
            last_tab = len(children) - 1
            return children[last_tab]["props"]["tab_id"]
        return 0

    return _app
