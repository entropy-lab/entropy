import json
from typing import Dict, List

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output, State, ALL
from plotly import graph_objects as go
from plotly.subplots import make_subplots

from entropylab import SqlAlchemyDB
from entropylab.api.data_reader import PlotRecord
from entropylab.api.errors import EntropyError
from entropylab.logger import logger
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


def build_dashboard_app(proj_path):
    """Initialize the results dashboard Dash app to display an Entropy project

    :param proj_path path where the Entropy project to be used resides."""

    """ Creating and setting up our Dash app """

    _dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(proj_path))

    def _build_layout():
        records = _dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
        return layout(proj_path, records)

    _app = dash.Dash(
        __name__,
        external_stylesheets=[theme_stylesheet],
        update_title=None,
        suppress_callback_exceptions=True,
    )
    _app.title = f"Entropy - {project_name(proj_path)} [{project_path(proj_path)}]"
    _app.layout = _build_layout  # See: https://dash.plotly.com/live-updates

    """ CALLBACKS and their helper functions """

    @_app.callback(
        Output("experiments-table", "data"),
        Output("empty-project-modal", "is_open"),
        Input("interval", "n_intervals"),
    )
    def refresh_experiments_table(_):
        """{Periodically refresh the experiments table.
        See https://dash.plotly.com/live-updates}"""
        records = _dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
        open_empty_project_modal = len(records) == 0
        return records, open_empty_project_modal

    @_app.callback(
        Output("failed-plotting-alert", "is_open"),
        Input("failed-plotting-alert", "children"),
    )
    def open_failed_plotting_alert_when_its_not_empty(children):
        return children != ""

    @_app.callback(
        Output("plot-tabs", "children"),
        Output("plot-figures", "data"),
        Output("prev-selected-rows", "data"),
        Output("failed-plotting-alert", "children"),
        Output("no-paging-spacer", "hidden"),
        Input("experiments-table", "selected_rows"),
        State("experiments-table", "data"),
        State("plot-figures", "data"),
        State("prev-selected-rows", "data"),
    )
    def render_plot_tabs_from_selected_experiments_table_rows(
        selected_rows, data, plot_figures, prev_selected_rows
    ):
        result = []
        plot_figures = plot_figures or {}
        selected_rows = selected_rows or {}
        prev_selected_rows = prev_selected_rows or {}
        alert_text = ""
        added_row = get_added_row(prev_selected_rows, selected_rows)
        spacer_hidden = len(data) > EXPERIMENTS_PAGE_SIZE
        if data and selected_rows:
            for row_num in selected_rows:
                alert_on_fail = row_num == added_row
                exp_id = data[row_num]["id"]
                try:
                    plots = _dashboard_data_reader.get_plot_data(exp_id)
                except EntropyError:
                    logger.exception(
                        f"Exception when getting plot data for exp_id={exp_id}"
                    )
                    if alert_on_fail:
                        alert_text = (
                            f"⚠ Error when reading plot data for this "
                            f"experiment. (id: {exp_id})"
                        )
                    plots = None
                if plots and len(plots) > 0:
                    failed_plot_ids = []
                    plot_figures = build_plot_tabs(
                        alert_on_fail, failed_plot_ids, plot_figures, plots, result
                    )
                    if len(failed_plot_ids) > 0:
                        alert_text = (
                            f"⚠ Some plots could not be rendered. "
                            f"(ids: {','.join(failed_plot_ids)})"
                        )
                else:
                    if alert_on_fail and alert_text == "":
                        alert_text = (
                            f"⚠ Experiment has no plots to render. (id: {exp_id})"
                        )
        if len(result) == 0:
            result = [build_plot_tabs_placeholder()]
        return result, plot_figures, selected_rows, alert_text, spacer_hidden

    def build_plot_tabs(alert_on_fail, failed_plot_ids, plot_figures, plots, result):
        for plot in plots:
            try:
                color = colors[len(result) % len(colors)]
                plot_tab, plot_figures = build_plot_tab_from_plot(
                    plot_figures, plot, color
                )
                result.append(plot_tab)
            except (EntropyError, TypeError):
                logger.exception(f"Failed to render plot id [{plot.id}]")
                if alert_on_fail:
                    plot_key = f"{plot.experiment_id}/{plot.id}"
                    failed_plot_ids.append(plot_key)
        return plot_figures

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
        Output("plot-keys-to-combine", "data"),
        Input("add-button", "n_clicks"),
        Input({"type": "remove-button", "index": ALL}, "n_clicks"),
        State("plot-tabs", "active_tab"),
        State("plot-keys-to-combine", "data"),
    )
    def add_or_remove_plot_keys_based_on_click_events(
        _, __, active_tab, plot_keys_to_combine
    ):
        plot_keys_to_combine = plot_keys_to_combine or []
        prop_id = dash.callback_context.triggered[0]["prop_id"]
        if prop_id == "add-button.n_clicks" and active_tab:
            active_plot_key = active_tab.replace("plot-tab-", "")
            if active_plot_key not in plot_keys_to_combine:
                plot_keys_to_combine.append(active_plot_key)
        elif "remove-button" in prop_id:
            id_dict = json.loads(prop_id.replace(".n_clicks", ""))
            remove_plot_key = id_dict["index"]
            if remove_plot_key in plot_keys_to_combine:
                plot_keys_to_combine.remove(remove_plot_key)
        return plot_keys_to_combine

    @_app.callback(
        Output("aggregate-tab", "children"),
        Output("remove-buttons", "children"),
        Input("plot-keys-to-combine", "data"),
        State("plot-figures", "data"),
    )
    def build_combined_plot_from_plot_keys(plot_keys_to_combine, plot_figures):
        if plot_keys_to_combine and len(plot_keys_to_combine) > 0:
            combined_figure = make_subplots(specs=[[{"secondary_y": True}]])
            remove_buttons = []
            for plot_id in plot_keys_to_combine:
                figure = plot_figures[plot_id]["figure"]
                color = plot_figures[plot_id]["color"]
                combined_figure.add_trace(figure["data"][0])
                button = build_remove_button(plot_id, color)
                remove_buttons.append(button)
            combined_figure.update_layout(dark_plot_layout)
            return (
                dcc.Graph(
                    id="aggregate-graph",
                    figure=combined_figure,
                    responsive=True,
                ),
                remove_buttons,
            )
        else:
            return (
                [build_aggregate_tab_placeholder()],
                [html.Div()],
            )

    def build_remove_button(plot_id, color):
        return dbc.Button(
            f"{plot_id} ✖️",
            style={"background-color": color},
            class_name="remove-button",
            id={"type": "remove-button", "index": plot_id},
        )

    def build_aggregate_tab_placeholder():
        return html.Div(
            [
                html.Div(
                    "Add a plot on the left to aggregate it here",
                    className="tab-placeholder-text",
                ),
                dcc.Graph(
                    id="aggregate-graph",
                    figure=go.Figure(),
                    responsive=True,
                    style={"display": "none"},
                ),
            ],
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

    @_app.callback(
        Output("aggregate-clipboard", "content"),
        Input("aggregate-clipboard", "n_clicks"),
        State("aggregate-graph", "figure"),
        prevent_initial_call=True,
    )
    def copy_aggregate_data_to_clipboard_as_python_code(_, figure):
        return _copy_aggregate_data_to_clipboard_as_python_code(_, figure)

    return _app


def get_added_row(prev: List[int], curr: List[int]) -> int or None:
    added_rows = list(set(curr) - set(prev))
    if len(added_rows) != 0:
        return added_rows[0]
    else:
        return None


def _copy_aggregate_data_to_clipboard_as_python_code(_, figure):
    _dict = list(map(lambda d: {d["name"]: {"x": d["x"], "y": d["y"]}}, figure["data"]))
    return f"data = {_dict}".replace("array(", "np.array(")
