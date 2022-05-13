import dash_bootstrap_components as dbc
from dash import html, dcc

from entropylab.results.dashboard.pages.results.dashboard_data import (
    DashboardDataReader,
)
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

REFRESH_INTERVAL_IN_MILLIS = 3000


def build_layout(path: str, dashboard_data_reader: DashboardDataReader):

    return dbc.Container(
        fluid=True,
        className="main",
        children=[
            dcc.Interval(
                id="interval", interval=REFRESH_INTERVAL_IN_MILLIS, n_intervals=0
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
                                            dbc.NavLink("Main", href="/", active=True)
                                        )
                                    ),
                                    dbc.Col(
                                        dbc.NavItem(
                                            dbc.NavLink(
                                                "ParamStore", href="/param_store"
                                            )
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
                        html.H5("Params", id="params-title"),
                        html.Div(id="no-paging-spacer"),
                    ],
                    width="12",
                )
            ),
        ],
    )
