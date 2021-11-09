# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import plotly
from plotly import graph_objects as go
from plotly.subplots import make_subplots
from dash import dcc
from dash import html
import plotly.express as px
import pandas as pd

app = dash.Dash(__name__)


num = "price"
cat = "item"
title = "Pareto Chart"

df = pd.DataFrame(
    {
        "price": [4.0, 17.0, 7.0, 7.0, 2.0, 1.0, 1.0],
        "item": ["apple", "banana", "carrot", "plum", "orange", "date", "cherry"],
    }
)

df = df.sort_values(num, ascending=False)
df["cumulative_sum"] = df[num].cumsum()
df["cumulative_perc"] = 100 * df["cumulative_sum"] / df[num].sum()

df["demarcation"] = 80


trace1 = go.Bar(x=df[cat], y=df[num], name=num, marker=dict(color="rgb(34,163,192)"))
trace2 = go.Scatter(
    x=df[cat], y=df["cumulative_perc"], name="Cumulative Percentage", yaxis="y2"
)

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(trace1)
fig.add_trace(trace2, secondary_y=True)
fig["layout"].update(height=600, width=800, title=title, xaxis=dict(tickangle=-90))

app.layout = dcc.Graph(figure=fig)


if __name__ == "__main__":
    app.run_server(debug=True)
