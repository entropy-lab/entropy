from dash import dash_table

from entropylab.dashboard.theme import (
    table_style_data,
    table_style_filter,
    table_style_header,
    table_style_cell,
)

EXPERIMENTS_PAGE_SIZE = 6


def table(records):
    tbl = dash_table.DataTable(
        id="experiments-table",
        columns=[
            dict(name="id", id="id", type="numeric"),
            dict(name="label", id="label", type="text"),
            dict(name="start_time", id="start_time", type="datetime"),
            dict(name="end_time", id="end_time", type="datetime"),
            dict(name="user", id="user", type="text"),
            dict(name="success", id="success"),
        ],
        data=records,
        persistence=True,
        persistence_type="session",
        row_selectable="multi",
        cell_selectable=False,
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_size=EXPERIMENTS_PAGE_SIZE,
        style_data=table_style_data,
        style_filter=table_style_filter,
        style_header=table_style_header,
        style_cell=table_style_cell,
        style_cell_conditional=[
            {"if": {"column_id": "id"}, "width": "7%"},
            {"if": {"column_id": "label"}, "width": "30%"},
            {
                "if": {"column_id": "start_time"},
                "width": "18%",
            },
            {"if": {"column_id": "end_time"}, "width": "18%"},
            {"if": {"column_id": "user"}, "width": "17%"},
            {"if": {"column_id": "success"}, "width": "10%"},
        ],
        style_data_conditional=[
            {
                "if": {
                    "column_id": "success",
                },
                "textAlign": "center",
            }
        ],
        # tooltip_data=[
        #     {
        #         column: {"value": str(value), "type": "markdown"}
        #         for column, value in row.items()
        #     }
        #     for row in records
        # ],
    )

    return tbl
