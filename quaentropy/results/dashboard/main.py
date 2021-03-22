import os
import sys

import panel as pn

sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("../../.."))

from quaentropy.results.dashboard.panel_dashboard import Dashboard  # noqa: E402

pn.extension()

dashboard = Dashboard()
dashboard.servable()
# pn.serve(dashboard.servable())
