import dash
import pandas as pd
from dash import dcc
from dash import html
from dash import dash_table

from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results_backend.sqlalchemy.project import project_name, project_path


MAX_EXPERIMENTS_NUM = 10000
EXPERIMENTS_PAGE_SIZE = 6


class Dashboard:
    def __init__(self, path):
        self.path = path
        self.project_name = project_name(path)
        self.title = f"Entropy - {project_name(path)} [{project_path(path)}]"

        self._dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
        self._experiments = pd.DataFrame(
            self._dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
        )

        self.app = dash.Dash(__name__, title=self.title)
        # df[date_col] = df[date_col].dt.date
        self._experiments["success"] = self._experiments["success"].apply(
            lambda x: "✔️" if x else "❌"
        )
        records = self._experiments.to_dict("records")
        self.app.layout = html.Div(
            children=[
                html.H1(self.title),
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
            ],
            className="main",
        )

    def serve(self):
        self.app.run_server(debug=True)


if __name__ == "__main__":
    dashboard = Dashboard("tests")
    dashboard.serve()
