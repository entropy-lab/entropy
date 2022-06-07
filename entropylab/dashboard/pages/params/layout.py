import dash_bootstrap_components as dbc
from dash import html, dcc, dash_table

from entropylab.dashboard.pages.components.footer import footer
from entropylab.dashboard.pages.components.top_bar import top_bar
from entropylab.dashboard.pages.params.utils import (
    param_store_to_commits_df,
    param_store_to_df,
)
from entropylab.dashboard.theme import (
    table_style_header,
    table_style_filter,
    table_style_data,
    table_style_cell,
    table_active_cell_conditional,
)
from entropylab.pipeline.api.param_store import ParamStore

REFRESH_INTERVAL_IN_MILLIS = 3000


def build_layout(path: str, param_store: ParamStore):

    return dbc.Container(
        fluid=True,
        className="main",
        children=[
            dcc.Interval(
                id="interval", interval=REFRESH_INTERVAL_IN_MILLIS, n_intervals=0
            ),
            top_bar(path),
            dbc.Row(
                children=[
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(html.H5("Params and their values")),
                                    dbc.Col(
                                        dbc.Collapse(
                                            dbc.Badge(
                                                "Dirty state",
                                                color="white",
                                                text_color="danger",
                                                id="dirty-badge",
                                                className="border ms-1",
                                                pill=True,
                                            ),
                                            is_open=False,
                                            id="dirty-badge-collapse",
                                        )
                                    ),
                                ]
                            ),
                            dbc.Row(
                                dbc.InputGroup(
                                    [
                                        dbc.Button(
                                            "➕ Add",
                                            color="primary",
                                            id="add-key-button",
                                            className="me-1",
                                        ),
                                        dbc.Button(
                                            "Commit",
                                            color="secondary",
                                            id="commit-button",
                                        ),
                                        dbc.Input(
                                            placeholder="Enter commit label",
                                            id="commit-label",
                                        ),
                                        dbc.Button(
                                            "Load temp",
                                            id="load-temp-button",
                                            className="me-1",
                                        ),
                                        dbc.Button(
                                            "Save temp",
                                            id="save-temp-button",
                                            className="me-1",
                                        ),
                                    ]
                                )
                            ),
                            dash_table.DataTable(
                                columns=[
                                    dict(
                                        name="key",
                                        id="key",
                                        type="text",
                                        editable=True,
                                    ),
                                    dict(
                                        name="value",
                                        id="value",
                                        type="any",
                                        editable=True,
                                    ),
                                    dict(
                                        name="tag",
                                        id="tag",
                                        type="text",
                                        editable=True,
                                    ),
                                ],
                                data=param_store_to_df(param_store).to_dict("records"),
                                id="data-table",
                                sort_action="native",
                                row_deletable=True,
                                filter_action="native",
                                editable=True,
                                fixed_rows={"headers": True, "data": 0},
                                style_data=table_style_data,
                                style_filter=table_style_filter,
                                style_header=table_style_header,
                                style_cell=table_style_cell,
                                style_data_conditional=[table_active_cell_conditional],
                            ),
                        ],
                        width=6,
                    ),
                    dbc.Col(
                        [
                            dbc.Row(html.H5("Commits")),
                            dbc.Row(
                                dbc.Col(
                                    dbc.Button("Checkout", id="checkout-button"),
                                    width="1",
                                ),
                            ),
                            dbc.Row(
                                dash_table.DataTable(
                                    columns=[
                                        dict(
                                            name="id",
                                            id="commit_id",
                                            type="text",
                                        ),
                                        dict(
                                            name="label",
                                            id="commit_label",
                                            type="text",
                                        ),
                                        dict(
                                            name="time",
                                            id="commit_time",
                                            type="datetime",
                                        ),
                                    ],
                                    data=param_store_to_commits_df(param_store).to_dict(
                                        "records"
                                    ),
                                    sort_action="native",
                                    sort_mode="multi",
                                    id="commit-table",
                                    filter_action="native",
                                    row_selectable="single",
                                    sort_by=[
                                        {
                                            "column_id": "commit_time",
                                            "direction": "desc",
                                        }
                                    ],
                                    fixed_rows={"headers": True, "data": 0},
                                    editable=True,
                                    style_data=table_style_data,
                                    style_filter=table_style_filter,
                                    style_header=table_style_header,
                                    style_cell=table_style_cell,
                                    style_data_conditional=[
                                        table_active_cell_conditional
                                    ],
                                )
                            ),
                        ],
                        width=6,
                    ),
                ],
            ),
            footer(),
        ],
    )
