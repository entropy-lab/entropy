import dash_bootstrap_components as dbc
import shutil
import os
from dash import html

from entropylab.pipeline.results_backend.sqlalchemy.project import (
    project_name,
    project_path,
)


def top_bar(path: str):
    hdd = shutil.disk_usage(path)
    return dbc.Row(
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
                            f"Project: {project_name(path)}",
                            id="project-name",
                        ),
                        html.Div(
                            f"{project_path(path)} | Project size:{os.path.getsize(path)/2**20:.4f} MB | Free space:{hdd.free//2**30} GB",
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
                                        "🔬 Experiment Results",
                                        href="/",
                                        active=True,
                                    )
                                )
                            ),
                            dbc.Col(
                                dbc.NavItem(dbc.NavLink("🧮 Params", href="/params"))
                            ),
                        ]
                    ),
                    width="6",
                ),
            ],
            color="primary",
        ),
    )
