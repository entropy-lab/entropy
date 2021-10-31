from dash import dash_table, Output, Input, html

EXPERIMENTS_PAGE_SIZE = 6


def table(app, records):
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
        # {"name": i, "id": i} for i in self._experiments.columns],
        data=records,
        row_selectable="multi",
        cell_selectable=False,
        sort_action="native",
        filter_action="native",
        page_action="native",
        page_size=EXPERIMENTS_PAGE_SIZE,
        style_cell={
            "textAlign": "left",
            "textOverflow": "ellipsis",
            "maxWidth": 0,
        },
        style_cell_conditional=[
            {"if": {"column_id": "id"}, "width": "7%"},
            {"if": {"column_id": "label"}, "width": "25%"},
            {
                "if": {"column_id": "start_time"},
                "width": "20%",
            },
            {"if": {"column_id": "end_time"}, "width": "20%"},
            {"if": {"column_id": "user"}, "width": "20%"},
            {"if": {"column_id": "success"}, "width": "8%"},
        ],
        style_data_conditional=[
            {
                "if": {
                    "column_id": "success",
                },
                "textAlign": "center",
            }
        ],
        tooltip_data=[
            {
                column: {"value": str(value), "type": "markdown"}
                for column, value in row.items()
            }
            for row in records
        ],
    )

    return tbl
