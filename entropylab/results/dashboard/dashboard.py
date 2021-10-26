import dash
import pandas as pd
from dash import dash_table
from dash import html
from dash.dependencies import Input, Output, State

from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

MAX_EXPERIMENTS_NUM = 10000
EXPERIMENTS_PAGE_SIZE = 6

app = dash.Dash(__name__)


@app.callback(
    Output("output_div", "children"),
    Input("table", "selected_rows"),
    # State("table", "data"),
)
def get_active_cell(selected_rows):
    return html.P(selected_rows)


def init(path):
    title = f"Entropy - {project_name(path)} [{project_path(path)}]"

    _dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
    _experiments = pd.DataFrame(
        _dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
    )

    # df[date_col] = df[date_col].dt.date
    _experiments["success"] = _experiments["success"].apply(
        lambda x: "✔️" if x else "❌"
    )
    records = _experiments.to_dict("records")
    app.layout = html.Div(
        children=[
            html.H1(title),
            dash_table.DataTable(
                id="table",
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
            ),
            html.Div(id="output_div"),
        ],
        className="main",
    )


if __name__ == "__main__":
    init("tests")
    app.run_server(debug=True)
    # dashboard = Dashboard("tests")
    # dashboard.serve()
