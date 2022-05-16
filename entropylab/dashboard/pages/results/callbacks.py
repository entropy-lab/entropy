from __future__ import annotations

import json
from typing import Dict, List, cast

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output, State, ALL
from plotly import graph_objects as go
from plotly.subplots import make_subplots

from entropylab.api.data_reader import PlotRecord, FigureRecord
from entropylab.api.errors import EntropyError
from entropylab.dashboard.theme import (
    colors,
    dark_plot_layout,
)
from entropylab.logger import logger

REFRESH_INTERVAL_IN_MILLIS = 3000
EXPERIMENTS_PAGE_SIZE = 6


def register_callbacks(app, dashboard_data_reader):
    """Initialize the results dashboard Dash app to display an Entropy project

    :param app the Dash app to add the callbacks to
    :param dashboard_data_reader a DashboardDataReader instance to read dashboard
    data from"""

    """ Creating and setting up our Dash app """

    """ CALLBACKS and their helper functions """

    @app.callback(
        Output("experiments-table", "data"),
        Output("empty-project-modal", "is_open"),
        Input("interval", "n_intervals"),
    )
    def refresh_experiments_table(_):
        """Periodically refresh the experiments table (See
        https://dash.plotly.com/live-updates), or when the filter on the 'success'
        column is changed"""
        records = dashboard_data_reader.get_last_experiments()
        open_empty_project_modal = (
            len(records) == 0
        ) and not callback_triggered_by_success_filter(dash.callback_context)
        return records, open_empty_project_modal

    # @app.callback(
    #     Output("experiments-table", "data"),
    #     Output("empty-project-modal", "is_open"),
    #     Output("success-filter-checklist", "value"),
    #     Input("interval", "n_intervals"),
    #     Input("success-filter-checklist", "value"),
    # )
    # def refresh_experiments_table(_, success_filter_checklist_value):
    #     """Periodically refresh the experiments table (See
    #     https://dash.plotly.com/live-updates), or when the filter on the 'success'
    #     column is changed"""
    #     success = checklist_value_to_bool(success_filter_checklist_value)
    #     records = dashboard_data_reader.get_last_experiments(success=success)
    #     open_empty_project_modal = (
    #         len(records) == 0
    #     ) and not callback_triggered_by_success_filter(dash.callback_context)
    #     return records, open_empty_project_modal, success_filter_checklist_value
    #
    # def checklist_value_to_bool(checklist_value: [bool]) -> Optional[bool]:
    #     """Translate the value of the success-filter Checklist component to a
    #     value understood by the data access API"""
    #     if True in checklist_value and False in checklist_value:
    #         return None  # None = Both True and False
    #     elif True in checklist_value:
    #         return True
    #     elif False in checklist_value:
    #         return False
    #     else:
    #         return None

    @app.callback(
        Output("failed-plotting-alert", "is_open"),
        Input("failed-plotting-alert", "children"),
    )
    def open_failed_plotting_alert_when_its_not_empty(children):
        return children != ""

    def callback_triggered_by_success_filter(callback_context) -> bool:
        return any(
            inputs["prop_id"] == "success-filter-checklist.value"
            for inputs in callback_context.triggered
        )

    @app.callback(
        Output("plot-tabs", "children"),
        Output("figures-by-key", "data"),
        Output("prev-selected-rows", "data"),
        Output("failed-plotting-alert", "children"),
        Output("no-paging-spacer", "hidden"),
        Input("experiments-table", "selected_rows"),
        State("experiments-table", "data"),
        State("figures-by-key", "data"),
        State("prev-selected-rows", "data"),
    )
    def render_plot_tabs_from_selected_experiments_table_rows(
        selected_rows, data, figures_by_key, prev_selected_rows
    ):
        result = []
        figures_by_key = figures_by_key or {}
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
                    plots_and_figures = dashboard_data_reader.get_plot_and_figure_data(
                        exp_id
                    )
                except EntropyError:
                    logger.exception(
                        f"Exception when getting plot/figure data for exp_id={exp_id}"
                    )
                    if alert_on_fail:
                        alert_text = (
                            f"⚠ Error when reading plot/figure data for this "
                            f"experiment. (id: {exp_id})"
                        )
                    plots_and_figures = None
                if plots_and_figures and len(plots_and_figures) > 0:
                    failed_plot_ids = []
                    figures_by_key = build_plot_tabs(
                        alert_on_fail,
                        failed_plot_ids,
                        figures_by_key,
                        plots_and_figures,
                        result,
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
        return result, figures_by_key, selected_rows, alert_text, spacer_hidden

    def build_plot_tabs(
        alert_on_fail, failed_plot_ids, figures_by_key, plots_and_figures, result
    ):
        for plot_or_figure in plots_and_figures:
            try:
                color = colors[len(result) % len(colors)]
                plot_tab, figures_by_key = build_plot_tab_from_plot_or_figure(
                    figures_by_key, plot_or_figure, color
                )
                result.append(plot_tab)
            except (EntropyError, TypeError):
                logger.exception(f"Failed to render plot id [{plot_or_figure.id}]")
                if alert_on_fail:
                    plot_key = f"{plot_or_figure.experiment_id}/{plot_or_figure.id}"
                    failed_plot_ids.append(plot_key)
        return figures_by_key

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

    def build_plot_tab_from_plot_or_figure(
        figures_by_key, plot_or_figure: PlotRecord | FigureRecord, color: str
    ) -> (dbc.Tab, Dict):
        if isinstance(plot_or_figure, PlotRecord):
            # For backwards compatibility with soon to be deprecated Plots API:
            plot_rec = cast(PlotRecord, plot_or_figure)
            key = f"{plot_rec.experiment_id}/{plot_rec.id}/p"
            name = f"Plot {key[:-2]}"
            figure = go.Figure()
            plot_or_figure.generator.plot_plotly(
                figure,
                plot_or_figure.plot_data,
                name=name,
                color=color,
                showlegend=False,
            )
        else:
            figure_rec = cast(FigureRecord, plot_or_figure)
            key = f"{figure_rec.experiment_id}/{figure_rec.id}/f"
            name = f"Figure {key[:-2]}"
            figure = figure_rec.figure
        figure.update_layout(dark_plot_layout)
        figures_by_key[key] = dict(figure=figure, color=color)
        return build_plot_tab(figure, name, key), figures_by_key

    def build_plot_tab(
        plot_figure: go.Figure, plot_name: str, plot_key: str
    ) -> dbc.Tab:
        return dbc.Tab(
            dcc.Graph(figure=plot_figure, responsive=True),
            label=plot_name,
            id=f"plot-tab-{plot_key}",
            tab_id=f"plot-tab-{plot_key}",
        )

    @app.callback(
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

    @app.callback(
        Output("aggregate-tab", "children"),
        Output("remove-buttons", "children"),
        Input("plot-keys-to-combine", "data"),
        State("figures-by-key", "data"),
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

    @app.callback(
        Output("plot-tabs", "active_tab"),
        Input("plot-tabs", "children"),
    )
    def activate_last_plot_tab_when_tabs_are_changed(children):
        if len(children) > 0:
            last_tab = len(children) - 1
            return children[last_tab]["props"]["tab_id"]
        return 0

    @app.callback(
        Output("aggregate-clipboard", "content"),
        Input("aggregate-clipboard", "n_clicks"),
        State("aggregate-graph", "figure"),
        prevent_initial_call=True,
    )
    def copy_aggregate_data_to_clipboard_as_python_code(_, figure):
        return _copy_aggregate_data_to_clipboard_as_python_code(_, figure)


def get_added_row(prev: List[int], curr: List[int]) -> int or None:
    added_rows = list(set(curr) - set(prev))
    if len(added_rows) != 0:
        return added_rows[0]
    else:
        return None


def _copy_aggregate_data_to_clipboard_as_python_code(_, figure):
    _dict = list(map(lambda d: {d["name"]: {"x": d["x"], "y": d["y"]}}, figure["data"]))
    return f"data = {_dict}".replace("array(", "np.array(")
