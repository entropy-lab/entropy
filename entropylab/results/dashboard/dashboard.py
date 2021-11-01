import dash
import pandas as pd
from dash import html, dcc
from dash.dependencies import Input, Output
from plotly import graph_objects as go

from entropylab import SqlAlchemyDB
from entropylab.results.dashboard.dashboard_data import SqlalchemyDashboardDataReader
from entropylab.results.dashboard.table import table
from entropylab.results_backend.sqlalchemy.project import project_name, project_path

MAX_EXPERIMENTS_NUM = 10000
EXPERIMENTS_PAGE_SIZE = 6

app = dash.Dash(__name__)


def init(path):
    # App setup

    _title = f"Entropy - {project_name(path)} [{project_path(path)}]"
    app.title = _title

    _dashboard_data_reader = SqlalchemyDashboardDataReader(SqlAlchemyDB(path))
    _experiments = pd.DataFrame(
        _dashboard_data_reader.get_last_experiments(MAX_EXPERIMENTS_NUM)
    )

    # Fetching data from DB

    # TODO: How to filter this column when its values are emoji?
    _experiments["success"] = _experiments["success"].apply(
        lambda x: "✔️" if x else "❌"
    )
    _records = _experiments.to_dict("records")

    # Components
    _table = table(app, _records)

    _tabs = dcc.Tabs(
        id="plot-tabs",
        value="",
        children=[],
    )

    # Callbacks

    @app.callback(
        Output("plot-tabs", "children"),
        Input("experiments-table", "selected_row_ids"),
        # State("table", "data"),
    )
    def get_selected_row_ids(selected_row_ids):
        colors = ["red", "green", "blue", "hotpink", "yellow", "maroon", "lilach"]
        result = []
        if selected_row_ids:
            for exp_id in selected_row_ids:
                plots = _dashboard_data_reader.get_plot_data(exp_id)
                # if plots:
                for plot in plots:
                    # if plot.generator:
                    figure = go.Figure()
                    plot.generator.plot_plotly(
                        figure,
                        plot.plot_data,
                        color="blue",
                        # label=f"{plot.experiment_id}",
                    )
                    result.append(
                        dcc.Tab(
                            label=f"Tab {plot.id}",
                            children=[
                                html.H3(f"Plot {plot.id}"),
                                dcc.Graph(id=f"graph-{plot.id}-tabs", figure=figure),
                            ],
                        )
                    )

                    # result.append(
                    #     dcc.Tab(
                    #         label=f"Tab {plot.id}",
                    #         children=[
                    #             html.H3(f"Plot {plot.id}"),
                    #             dcc.Graph(
                    #                 id=f"graph-{plot.id}-tabs",
                    #                 figure={
                    #                     "data": [
                    #                         {
                    #                             "x": [1, 2, 3],
                    #                             "y": [3, 1, 2],
                    #                             "type": "bar",
                    #                             "marker": {
                    #                                 "color": colors[plot.id]
                    #                             },
                    #                         }
                    #                     ]
                    #                 },
                    #             ),
                    #         ],
                    #     )
                    # )
        return result

    # App layout

    app.layout = html.Div(
        children=[html.H1(_title), _table, html.Div(id="output_div"), _tabs],
        className="main",
    )


if __name__ == "__main__":
    init("tests")
    app.run_server(debug=True)
