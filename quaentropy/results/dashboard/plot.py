from typing import Union

from api.model import PlotGenerator


class PanelPlotGenerator(PlotGenerator):
    def __init__(self, data, type_or_action: Union[str]) -> None:
        super().__init__()
