import dash
import pandas as pd
from dash import dcc
from dash import html
from dash import dash_table

from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results_backend.sqlalchemy.project import project_name, project_path


MAX_EXPERIMENTS_NUM = 10000


class Dashboard:
    def __init__(self, path):
        self.path = path
        self.project_name = project_name(path)
        self.title = f"Entropy - {project_name(path)} [{project_path(path)}]"

        self._dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
        self._experiments = pd.DataFrame(
            self._dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
        )

        self.app = dash.Dash(self.project_name, title=self.title)

        self.app.layout = html.Div(
            children=[
                html.H1(self.title),
                dash_table.DataTable(
                    id="table",
                    columns=[{"name": i, "id": i} for i in self._experiments.columns],
                    data=self._experiments.to_dict("records"),
                ),
            ],
        )

    def serve(self):
        self.app.run_server(debug=True)


if __name__ == "__main__":
    dashboard = Dashboard("tests")
    dashboard.serve()
