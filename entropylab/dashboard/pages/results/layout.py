import dash_bootstrap_components as dbc
from dash import html, dcc

from entropylab.dashboard.pages.results.dashboard_data import (
    DashboardDataReader,
)
from entropylab.dashboard.pages.results.table import table
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

REFRESH_INTERVAL_IN_MILLIS = 3000


def build_layout(path: str, dashboard_data_reader: DashboardDataReader):
    records = dashboard_data_reader.get_last_experiments()

    return dbc.Container(
        fluid=True,
        className="main",
        children=[
            dcc.Store(id="figures-by-key", storage_type="session"),
            dcc.Store(id="plot-keys-to-combine", storage_type="session"),
            dcc.Store(id="prev-selected-rows", storage_type="session"),
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
                            f"ℹ️ Project '{project_name(path)}' contains no experiments"
                        ),
                        close_button=False,
                    ),
                    dbc.ModalBody(
                        "As soon as experiments are saved to the project this notice "
                        "will be closed and the experiments will be shown below."
                    ),
                    dbc.ModalFooter(),
                ],
                id="empty-project-modal",
                backdrop="static",
                centered=True,
                is_open=False,
            ),
            dbc.Row(
                dbc.Navbar(
                    [
                        dbc.Col(
                            dbc.NavbarBrand(
                                html.Img(
                                    src="/assets/images/entropy_logo_dark.svg",
                                    width=150,
                                    id="entropy-logo",
                                ),
                                href="#",
                            ),
                            width="2",
                            id="logo-col",
                        ),
                        dbc.Col(
                            [
                                html.Div(
                                    f"Project: {project_name(path)}", id="project-name"
                                ),
                                html.Div(
                                    f"{project_path(path)}",
                                    id="project-name",
                                    style={"fontSize": "11px"},
                                ),
                            ],
                            width="4",
                        ),
                        dbc.Col(
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.NavItem(
                                            dbc.NavLink(
                                                "Experiment Results",
                                                href="/",
                                                active=True,
                                            )
                                        )
                                    ),
                                    dbc.Col(
                                        dbc.NavItem(
                                            dbc.NavLink("Params", href="/params")
                                        )
                                    ),
                                ]
                            ),
                            width="6",
                        ),
                    ],
                    color="primary",
                ),
            ),
            dbc.Row(
                dbc.Col(
                    [
                        html.H5("Experiments", id="experiments-title"),
                        (table(records)),
                        html.Div(id="no-paging-spacer"),
                    ],
                    width="12",
                )
            ),
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
                            ),
                            className="add-button-col-container",
                        ),
                        width="1",
                        className="add-button-col",
                    ),
                    dbc.Col(
                        [
                            dbc.Tabs(
                                id="aggregate-tabs",
                                children=[
                                    dbc.Tab(
                                        "",
                                        label="Aggregate",
                                        id="aggregate-tab",
                                    )
                                ],
                                persistence=True,
                            ),
                            dbc.Button(
                                [
                                    "Copy Data to Clipboard",
                                    dcc.Clipboard(
                                        title="Copy data to clipboard",
                                        id="aggregate-clipboard",
                                        className="position-absolute start-0 top-0 "
                                        "h-100 w-100 opacity-0",
                                    ),
                                ],
                                id="copy-data-button",
                                color="primary",
                            ),
                        ],
                        id="aggregate-container",
                        width="5",
                    ),
                    dbc.Col(
                        [
                            html.Div("<< Remove", id="remove-title"),
                            html.Div(id="remove-buttons"),
                        ],
                        width="1",
                    ),
                ]
            ),
            # Disabled temporarily. See ../../assets/custom-script.js for details.
            # dcc.Checklist(
            #     [
            #         {"label": "✔️", "value": True},
            #         {"label": "❌", "value": False},
            #     ],
            #     [True, False],
            #     inline=False,
            #     inputClassName="success-filter-checklist-input",
            #     labelClassName="success-filter-checklist-label",
            #     labelStyle={"display": "flex"},
            #     id="success-filter-checklist",
            # ),
        ],
    )
