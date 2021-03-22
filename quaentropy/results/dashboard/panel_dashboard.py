import itertools
from io import StringIO
from typing import Any

import numpy
import pandas as pd
import panel as pn
import param
from bokeh.palettes import Dark2_5 as palette
from bokeh.plotting import Figure

from quaentropy.api.data_reader import PlotRecord
from quaentropy.results.dashboard.dashboard_data import (
    SqlalchemyDashboardDataReader,
)
from quaentropy.results_backend.sqlalchemy.database import (
    SqlalchemySqlitePandasConnector,
)

colors = itertools.cycle(palette)


class Dashboard(param.Parameterized):
    def __init__(self, **params):
        super().__init__(**params)
        # self.data_reader = MockDashboardDataReader()
        connector = SqlalchemySqlitePandasConnector("my_db.db", False)
        self.dashboard_data_reader = SqlalchemyDashboardDataReader(connector)

        self.res_num = pn.widgets.Select(
            name="Result number", value=10, options=[5, 10, 20]
        )

        self._data_frame: pd.DataFrame = pd.DataFrame()
        self._curr_experiment_plots: Any = None
        self.update_df()
        self.df_widget = pn.widgets.DataFrame(
            self._data_frame, name="DataFrame", disabled=True, show_index=False
        )
        self.dashboard_title = "# Entropy Dataviewer 0.0.1"
        self.dashboard_desc = "A viewer for the most recent experiments on the database"

        table_watcher = self.df_widget.param.watch(
            self.table_callback, ["selection"], onlychanged=False
        )
        res_num_watcher = self.res_num.param.watch(
            self.res_num_callback, ["value"], onlychanged=False
        )
        self.plot_tabs = pn.Tabs(Figure(name=""))
        self.plot_tabs_records = []
        self.add_plot_to_combined = pn.widgets.Button(name="add plot")
        self.add_plot_to_combined.on_click(self.add_plot_to_combined_callback)

        self.clear_plots = pn.widgets.Button(name="clear plots")
        self.clear_plots.on_click(self.clear_button_callback)
        self.add_plot_figure = Figure(name="",tools="pan,lasso_select,box_select,crosshair,xwheel_zoom,ywheel_zoom,zoom_in,reset,save,hover")
        self.add_plot_figure.line()
        self.export_to_csv = pn.widgets.FileDownload(
            label="export to csv",
            on_click=self.export_to_csv_callback,
            callback=self.export_to_csv_callback,
            filename="plot_data.csv",
        )
        self.export_to_csv.on_click(self.export_to_csv_callback)

    def clear_button_callback(self, *events):
        self.add_plot_figure.renderers = []
        self.add_plot_figure.line()

    def add_plot_to_combined_callback(self, *events):

        plot: PlotRecord = self.plot_tabs_records[self.plot_tabs.active]
        plot.bokeh_generator.plot_in_figure(
            self.add_plot_figure, plot.plot_data, plot.data_type, color=next(colors), label=f"{plot.experiment_id}"
        )
        self.add_plot_figure.legend.click_policy = "hide"


    def export_to_csv_callback(self, *events):
        sio = StringIO()
        for x in self.add_plot_figure.renderers:
            if "x" in x.data_source.data:
                print(x.data_source.data["x"])
                numpy.savetxt(sio, x.data_source.data["x"], delimiter=",", newline=",")
            sio.write("\n")
            if "y" in x.data_source.data:
                print(x.data_source.data["y"])
                numpy.savetxt(sio, x.data_source.data["y"], delimiter=",", newline=",")
            sio.write("\n")

        sio.seek(0)
        return sio

    def update_df(self):
        experiments = self.dashboard_data_reader.get_last_experiments(
            self.res_num.value
        )
        self._data_frame = pd.DataFrame(experiments)

    def res_num_callback(self, *events):
        self.update_df()
        self.df_widget.value = self._data_frame

    def table_callback(self, *events):
        # load_selected_experiment
        idx = self.df_widget.selection[0]
        # todo disable sort or handle selection better
        exp_id = self._data_frame.id[idx]

        self._curr_experiment_plots = self.dashboard_data_reader.get_plot_data(exp_id)

        self.plot_current_experiment()

    def plot_current_experiment(self, *events):
        # if self.render_selector.value == "line":
        if self._curr_experiment_plots:
            self.plot_tabs.clear()
            self.plot_tabs_records.clear()
            last_experiment_plots = self._curr_experiment_plots
            for plot in last_experiment_plots:
                bokeh_generator = plot.bokeh_generator
                if bokeh_generator:
                    # label_opts = dict(
                    #     x = 0, y = 0,                     x_units = 'screen', y_units = 'screen'
                    # )
                    # caption2 = Label(text=plot.story, **label_opts)
                    figure = Figure(name=plot.label, title=plot.story)
                    # figure.add_layout(caption2, 'above')
                    bokeh_generator.plot_in_figure(
                        figure, plot.plot_data, plot.data_type, color="blue", label=f"{plot.experiment_id}"
                    )
                    self.plot_tabs_records.append(plot)

                    self.plot_tabs.append(figure)
                else:
                    pass  # TODO do our best

    def servable(self):
        contents = pn.Row(
            pn.Column(self.df_widget, self.res_num),
            pn.Column(self.plot_tabs, self.add_plot_to_combined),
            pn.Column(
                self.add_plot_figure, pn.Row(self.clear_plots, self.export_to_csv)
            ),
        )
        layout = pn.Column(self.dashboard_title, self.dashboard_desc, contents)
        a = pn.Column(layout)
        return a.servable(title=self.dashboard_title)
