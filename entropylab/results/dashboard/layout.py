from typing import List, Dict

import dash_bootstrap_components as dbc
from dash import html, dcc

from entropylab.results.dashboard.table import table
from entropylab.results_backend.sqlalchemy.project import project_name, project_path


def layout(path: str, records: List[Dict], refresh_interval_in_millis: int):
    return dbc.Container(
        fluid=True,
        className="main",
        children=[
            dcc.Store(id="figures-by-key", storage_type="session"),
            dcc.Store(id="plot-keys-to-combine", storage_type="session"),
            dcc.Store(id="prev-selected-rows", storage_type="session"),
            dcc.Interval(
                id="interval", interval=refresh_interval_in_millis, n_intervals=0
            ),
            dbc.Alert(
                "",
                id="failed-plotting-alert",
                color="warning",
                is_open=False,
                fade=True,
                # duration=3000,
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
                                html.A(
                                    html.Img(
                                        src="/assets/images/entropy_logo_dark.svg",
                                        width=150,
                                        id="entropy-logo",
                                    ),
                                    href="#",
                                ),
                                href="#",
                            ),
                            width="3",
                            id="logo-col",
                        ),
                        dbc.Col(
                            [
                                html.Div(f"{project_name(path)}", id="project-name"),
                                dbc.Tooltip(
                                    f"{project_path(path)}",
                                    target="project-name",
                                ),
                            ],
                            width="3",
                        ),
                        dbc.Col(
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.NavItem(
                                            dbc.NavLink(
                                                "Dashboard", href="#", active=True
                                            )
                                        )
                                    ),
                                    dbc.Col(
                                        dbc.NavItem(
                                            dbc.NavLink("Configuration", href="#")
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
            dcc.Checklist(
                [
                    {"label": "✔️", "value": True},
                    {"label": "❌", "value": False},
                ],
                [True, False],
                inline=False,
                inputClassName="success-filter-checklist-input",
                labelClassName="success-filter-checklist-label",
                labelStyle={"display": "flex"},
                id="success-filter-checklist",
            ),
        ],
    )
