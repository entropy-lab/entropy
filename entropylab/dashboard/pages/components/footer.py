import dash_bootstrap_components as dbc
import pkg_resources
from dash import html


def footer():
    version = pkg_resources.get_distribution("entropylab").version

    href_home = "https://github.com/entropy-lab/entropy/"
    href_documentation = "https://entropylab-docs-open.netlify.app/"
    href_feedback = "https://github.com/entropy-lab/entropy/issues/"
    return dbc.Row(
        children=[
            dbc.Col(
                dbc.Row(
                    children=[
                        dbc.Col(f"Entropylab v{version}", width=4),
                        dbc.Col(" · ", width=1),
                        dbc.Col(
                            dbc.Row(
                                children=[
                                    dbc.Col(
                                        html.A(
                                            "Home",
                                            href=href_home,
                                            target="_blank",
                                        ),
                                        width=3,
                                    ),
                                    dbc.Col("|", width=1),
                                    dbc.Col(
                                        html.A(
                                            "Documentation",
                                            href=href_documentation,
                                            target="_blank",
                                        ),
                                        width=4,
                                    ),
                                    dbc.Col("|", width=1),
                                    dbc.Col(
                                        html.A(
                                            "Feedback",
                                            href=href_feedback,
                                            target="_blank",
                                        ),
                                        width=3,
                                    ),
                                ],
                                id="footer-links",
                            ),
                            width=7,
                        ),
                    ]
                ),
                width=dict(size=4, offset=4),
            ),
        ],
        style=dict(marginTop="40px", marginBottom="40px"),
    )
