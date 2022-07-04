from __future__ import annotations

import json
from typing import Dict, List, cast, Optional

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output, State, ALL
from plotly import graph_objects as go
from plotly.subplots import make_subplots

from entropylab.dashboard.pages.results.dashboard_data import FAVORITE_TRUE
from entropylab.dashboard.theme import (
    colors,
    dark_figure_layout,
)
from entropylab.logger import logger
from entropylab.pipeline.api.data_reader import (
    FigureRecord,
    MatplotlibFigureRecord,
)
from entropylab.pipeline.api.errors import EntropyError

REFRESH_INTERVAL_IN_MILLIS = 3000
EXPERIMENTS_PAGE_SIZE = 6
IMG_TAB_KEY = "m"
FIGURE_TAB_KEY = "f"


def register_callbacks(app, dashboard_data_reader):
    """Add callbacks and helper methods to the dashboard Dash app

    :param app the Dash app to add the callbacks to
    :param dashboard_data_reader a DashboardDataReader instance to read dashboard
    data from"""

    @app.callback(
        Output("experiments-table", "data"),
        Output("empty-project-modal", "is_open"),
        Output("experiments-table", "active_cell"),
        Input("interval", "n_intervals"),
        Input("experiments-table", "active_cell"),
        State("experiments-table", "data"),
    )
    def refresh_experiments_table(_, active_cell, data):
        """Periodically refresh the experiments table (See
        https://dash.plotly.com/live-updates), or when a user clicks a "favorite"
        column star"""
        records = dashboard_data_reader.get_last_experiments()
        if active_cell and active_cell["column_id"] == "favorite":
            update_favorite_by_active_cell(active_cell, data)
        open_empty_project_modal = len(records) == 0
        return records, open_empty_project_modal, None  # <- cancels active_cell!

    def update_favorite_by_active_cell(active_cell, data):
        exp_id = active_cell["row_id"]
        record = [record for record in data if record["id"] == exp_id][0]
        if record["favorite"] == FAVORITE_TRUE:
            dashboard_data_reader.update_experiment_favorite(exp_id, False)
        else:
            dashboard_data_reader.update_experiment_favorite(exp_id, True)

    @app.callback(
        Output("failed-plotting-alert", "is_open"),
        Input("failed-plotting-alert", "children"),
    )
    def open_failed_plotting_alert_when_its_not_empty(children):
        return children != ""

    @app.callback(
        Output("figure-tabs", "children"),
        Output("figures-by-key", "data"),
        Output("prev-selected-rows", "data"),
        Output("failed-plotting-alert", "children"),
        Input("experiments-table", "selected_rows"),
        State("experiments-table", "data"),
        State("figures-by-key", "data"),
        State("prev-selected-rows", "data"),
    )
    def render_figure_tabs_from_selected_experiments_table_rows(
        selected_rows, data, figures_by_key, prev_selected_rows
    ):
        result = []
        figures_by_key = figures_by_key or {}
        selected_rows = selected_rows or {}
        prev_selected_rows = prev_selected_rows or {}
        alert_text = ""
        added_row = get_added_row(prev_selected_rows, selected_rows)
        if data and selected_rows:
            for row_num in selected_rows:
                alert_on_fail = row_num == added_row
                exp_id = data[row_num]["id"]
                try:
                    figure_records = dashboard_data_reader.get_plot_and_figure_data(
                        exp_id
                    )
                except EntropyError:
                    logger.exception(
                        f"Exception when getting figure data for exp_id={exp_id}"
                    )
                    if alert_on_fail:
                        alert_text = (
                            f"⚠ Error when reading figure data for this "
                            f"experiment. (id: {exp_id})"
                        )
                    figure_records = None
                if figure_records and len(figure_records) > 0:
                    failed_figure_keys = []
                    figures_by_key = build_figure_tabs(
                        alert_on_fail,
                        failed_figure_keys,
                        figures_by_key,
                        figure_records,
                        result,
                    )
                    if len(failed_figure_keys) > 0:
                        alert_text = (
                            f"⚠ Some figures could not be rendered. "
                            f"(ids: {','.join(failed_figure_keys)})"
                        )
                else:
                    if alert_on_fail and alert_text == "":
                        alert_text = (
                            f"⚠ Experiment has no figures to render. (id: {exp_id})"
                        )
        if len(result) == 0:
            result = [build_figure_tabs_placeholder()]
        return (
            result,
            figures_by_key,
            selected_rows,
            alert_text,
        )

    def build_figure_tabs(
        alert_on_fail, failed_figure_keys, figures_by_key, figure_records, result
    ):
        for figure_record in figure_records:
            try:
                color = colors[len(result) % len(colors)]
                figure_tab, figures_by_key = build_tab_from_figure(
                    figures_by_key, figure_record, color
                )
                result.append(figure_tab)
            except (EntropyError, TypeError):
                logger.exception(
                    f"Failed to render figure record id [{figure_record.id}]"
                )
                if alert_on_fail:
                    figure_key = f"{figure_record.experiment_id}/{figure_record.id}"
                    failed_figure_keys.append(figure_key)
        return figures_by_key

    def build_figure_tabs_placeholder():
        return dbc.Tab(
            html.Div(
                html.Div(
                    "Select an experiment above to display its figures here",
                    className="tab-placeholder-text",
                ),
                className="tab-placeholder-container",
            ),
            label="Figures",
            tab_id="figure-tab-placeholder",
        )

    def build_tab_from_figure(
        figures_by_key,
        figure_record: FigureRecord | MatplotlibFigureRecord,
        color: str,
    ) -> (dbc.Tab, Dict):
        if isinstance(figure_record, MatplotlibFigureRecord):
            record = cast(MatplotlibFigureRecord, figure_record)
            key = f"{record.experiment_id}/{record.id}/{IMG_TAB_KEY}"
            name = f"Image {key[:-2]}"
            return build_img_tab(record.img_src, name, key), figures_by_key
        else:
            record = cast(FigureRecord, figure_record)
            key = f"{record.experiment_id}/{record.id}/{FIGURE_TAB_KEY}"
            name = f"Figure {key[:-2]}"
            figure = record.figure
            figure.update_layout(dark_figure_layout)
            figures_by_key[key] = dict(figure=figure, color=color)
            return build_figure_tab(figure, name, key), figures_by_key

    def build_figure_tab(
        figure: go.Figure, figure_name: str, figure_key: str
    ) -> dbc.Tab:
        return dbc.Tab(
            dcc.Graph(figure=figure, responsive=True),
            label=figure_name,
            id=f"figure-tab-{figure_key}",
            tab_id=f"figure-tab-{figure_key}",
        )

    def build_img_tab(img_src: str, figure_name: str, figure_key: str) -> dbc.Tab:
        return dbc.Tab(
            html.Img(src=img_src),
            label=figure_name,
            id=f"figure-tab-{figure_key}",
            tab_id=f"figure-tab-{figure_key}",
        )

    @app.callback(
        Output("figure-keys-to-combine", "data"),
        Input("add-button", "n_clicks"),
        Input({"type": "remove-button", "index": ALL}, "n_clicks"),
        State("figure-tabs", "active_tab"),
        State("figure-keys-to-combine", "data"),
    )
    def add_or_remove_figure_keys_based_on_click_events(
        _, __, active_tab, figure_keys_to_combine
    ):
        figure_keys_to_combine = figure_keys_to_combine or []
        prop_id = dash.callback_context.triggered[0]["prop_id"]
        if prop_id == "add-button.n_clicks" and active_tab:
            active_figure_key = active_tab.replace("figure-tab-", "")
            if active_figure_key not in figure_keys_to_combine:
                figure_keys_to_combine.append(active_figure_key)
        elif "remove-button" in prop_id:
            id_dict = json.loads(prop_id.replace(".n_clicks", ""))
            remove_figure_key = id_dict["index"]
            if remove_figure_key in figure_keys_to_combine:
                figure_keys_to_combine.remove(remove_figure_key)
        return figure_keys_to_combine

    @app.callback(
        Output("aggregate-tab", "children"),
        Output("remove-buttons", "children"),
        Input("figure-keys-to-combine", "data"),
        State("figures-by-key", "data"),
    )
    def build_combined_figure_from_figure_keys(figure_keys_to_combine, figures_by_key):
        if figure_keys_to_combine and len(figure_keys_to_combine) > 0:
            combined_figure = make_subplots(specs=[[{"secondary_y": True}]])
            remove_buttons = []
            for figure_key in figure_keys_to_combine:
                figure = figures_by_key[figure_key]["figure"]
                color = figures_by_key[figure_key]["color"]
                combined_figure.add_trace(figure["data"][0])
                button = build_remove_button(figure_key, color)
                remove_buttons.append(button)
            combined_figure.update_layout(dark_figure_layout)
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

    def build_remove_button(figure_key, color):
        return dbc.Button(
            dbc.Row(
                children=[
                    dbc.Col(
                        "✖",
                    ),
                    dbc.Col(f"{figure_key}", className="remove-button-label"),
                ],
            ),
            style={"background-color": color},
            class_name="remove-button",
            id={"type": "remove-button", "index": figure_key},
        )

    def build_aggregate_tab_placeholder():
        return html.Div(
            [
                html.Div(
                    "Add a figure (above) to aggregate it here",
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
        Output("figure-tabs", "active_tab"),
        Input("figure-tabs", "children"),
    )
    def activate_last_figure_tab_when_tabs_are_changed(children):
        if len(children) > 0:
            last_tab = len(children) - 1
            return children[last_tab]["props"]["tab_id"]
        return 0

    @app.callback(
        Output("add-button", "disabled"),
        Input("figure-tabs", "active_tab"),
    )
    def disable_add_button_when_active_tab_is_img_or_placeholder(active_tab):
        return active_tab.endswith("/m") or active_tab == "figure-tab-placeholder"

    @app.callback(
        Output("aggregate-clipboard", "content"),
        Input("aggregate-clipboard", "n_clicks"),
        State("aggregate-graph", "figure"),
        prevent_initial_call=True,
    )
    def copy_aggregate_data_to_clipboard_as_python_code(_, figure):
        return _copy_aggregate_data_to_clipboard_as_python_code(_, figure)


def get_added_row(prev: List[int], curr: List[int]) -> Optional[int]:
    added_rows = list(set(curr) - set(prev))
    if len(added_rows) != 0:
        return added_rows[0]
    else:
        return None


def _copy_aggregate_data_to_clipboard_as_python_code(_, figure):
    _dict = list(map(lambda d: {d["name"]: {"x": d["x"], "y": d["y"]}}, figure["data"]))
    return f"data = {_dict}".replace("array(", "np.array(")
