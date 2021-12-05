import dash_bootstrap_components as dbc
from dash import html, dcc

from entropylab.results.dashboard.table import table
from entropylab.results_backend.sqlalchemy.project import project_name, project_path


def layout(path: str, records: dict):
    return dbc.Container(
        className="main",
        children=[
            dcc.Store(id="plot-figures", storage_type="session"),
            dcc.Store(id="plot-keys-to-combine", storage_type="session"),
            dbc.Row(
                dbc.Navbar(
                    dbc.Container(
                        [
                            dbc.NavbarBrand(
                                html.A(
                                    html.Img(
                                        src="/assets/images/entropy_logo_dark.svg",
                                        width=150,
                                    ),
                                ),
                                href="#",
                            ),
                            html.H4(f"{project_name(path)} ", id="project-name"),
                            dbc.Tooltip(
                                f"{project_path(path)}",
                                target="project-name",
                            ),
                            dbc.NavItem(
                                dbc.NavLink(
                                    "Dashboard",
                                    href="#",
                                    active=True,
                                )
                            ),
                            dbc.NavItem(dbc.NavLink("Configuration", href="#")),
                        ]
                    ),
                    color="primary",
                ),
            ),
            dbc.Row(
                dbc.Col(
                    [html.H5("Experiments", id="experiments-title"), (table(records))],
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
        ],
    )
