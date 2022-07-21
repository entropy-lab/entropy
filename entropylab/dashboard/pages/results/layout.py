import dash_bootstrap_components as dbc
from dash import html, dcc

from entropylab.dashboard.pages.components.footer import footer
from entropylab.dashboard.pages.components.top_bar import top_bar
from entropylab.dashboard.pages.results.dashboard_data import (
    DashboardDataReader,
)
from entropylab.dashboard.pages.results.table import table
from entropylab.pipeline.results_backend.sqlalchemy.project import (
    project_name,
)

REFRESH_INTERVAL_IN_MILLIS = 3000


def build_layout(path: str, dashboard_data_reader: DashboardDataReader):
    records = dashboard_data_reader.get_last_experiments()

    return (
        dbc.Container(
            fluid=True,
            className="main",
            children=[
                dcc.Store(id="figures-by-key", storage_type="session"),
                dcc.Store(id="plot-keys-to-combine", storage_type="session"),
                dcc.Store(id="prev-selected-rows", storage_type="session"),
                dcc.Store(id="favorites", storage_type="session"),
                dcc.Interval(
                    id="interval", interval=REFRESH_INTERVAL_IN_MILLIS, n_intervals=0
                ),
                dbc.Alert(
                    "",
                    id="failed-plotting-alert",
                    color="warning",
                    is_open=False,
                    fade=True,
                ),
                dbc.Modal(
                    [
                        dbc.ModalHeader(
                            dbc.ModalTitle(
                                f"ℹ️ Project '{project_name(path)}' contains no "
                                f"experiments"
                            ),
                            close_button=False,
                        ),
                        dbc.ModalBody(
                            "As soon as experiments are saved to the project this "
                            "notice will be closed and the experiments will be shown "
                            "below."
                        ),
                        dbc.ModalFooter(),
                    ],
                    id="empty-project-modal",
                    backdrop="static",
                    centered=True,
                    is_open=False,
                ),
                top_bar(path),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H5("Experiments", id="experiments-title"),
                                (table(records)),
                            ],
                            width="5",
                        ),
                        dbc.Col(
                            [
                                dbc.Row(
                                    [
                                        html.H5("Plots and Figures", id="plots-title"),
                                        dcc.Loading(
                                            id="plot-tabs-loading",
                                            children=[dbc.Tabs(id="plot-tabs")],
                                            type="default",
                                        ),
                                    ]
                                ),
                                dbc.Row(
                                    dbc.Button(
                                        "➕ Add Plot to Aggregate View",
                                        id="add-button",
                                    ),
                                    className="add-button-container",
                                ),
                                dbc.Row(
                                    [
                                        html.H5(
                                            "Aggregate View", id="experiments-title"
                                        ),
                                        dbc.Tabs(
                                            id="aggregate-tabs",
                                            children=[
                                                dbc.Tab(
                                                    "",
                                                    label="",
                                                    id="aggregate-tab",
                                                )
                                            ],
                                            persistence=True,
                                        ),
                                        dbc.Button(
                                            [
                                                " Copy Data to Clipboard",
                                                dcc.Clipboard(
                                                    title="Copy data to clipboard",
                                                    id="aggregate-clipboard",
                                                    className="position-absolute "
                                                    # "start-0 top-0 "
                                                    # "h-100 w-100 opacity-0",
                                                ),
                                            ],
                                            id="copy-data-button",
                                            color="primary",
                                        ),
                                    ],
                                    id="aggregate-container",
                                ),
                                dbc.Row(
                                    [
                                        html.Div(
                                            "Click to remove from Aggregate View:",
                                            id="remove-title",
                                        ),
                                        html.Div(id="remove-buttons"),
                                    ],
                                ),
                            ],
                            width="7",
                        ),
                    ],
                ),
                footer(),
            ],
        ),
    )
