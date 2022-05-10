import ast
import plotly.data
from dash import Dash, dash_table, html, dcc, callback_context
from dash.dependencies import Input, Output, State, ALL
from entropylab.api.in_process_param_store import InProcessParamStore
from paramstoreGUIUtils import paramStore_to_df, paramStore_commits_df, data_diff
from entropylab.api.param_store import ParamStore
import dash_bootstrap_components as dbc
from datetime import datetime
import pandas as pd

PATH = r'.\params.db'
params = InProcessParamStore(PATH)
params.load_temp()
app = Dash('paramStoreGUI', external_stylesheets=[dbc.themes.BOOTSTRAP])


@app.callback(
    [Output('data-table', 'data'),
     Output('commit-table', 'data'),
     Output('dirty-badge-collapse', 'is_open')],
    Input('data-table', 'data'),
    Input('commit-button', 'n_clicks'),
    Input('add-key-button', 'n_clicks'),
    Input('checkout-button', 'n_clicks'),
    Input('save-temp-button', 'n_clicks'),
    Input('load-temp-button', 'n_clicks'),
    Input('commit-table', 'selected_rows'),
    Input('commit-table', 'data'),

    # Input("interval", "n_intervals"),
    State('data-table', 'active_cell'),
    State('data-table', 'data_previous'),
    State('commit-label', 'value'),
    prevent_initial_call=True
)
def button_callback(data, commit_click, add_click, checkout_click, load_temp_click, save_temp_click, checkout_idx,
                    checkout_data, active_cell, data_prev, commit_label):
    prop_id = callback_context.triggered[0]["prop_id"].split('.')[0]

    if prop_id == 'commit-button':
        params.commit(commit_label)
    elif prop_id == 'checkout-button':
        commit_id = checkout_data[checkout_idx[0]]['commit_id']
        params.checkout(commit_id)
    elif prop_id == 'data-table':
        data_diff(params, data, data_prev)
    elif prop_id == 'add-key-button':
        params[f"newKey{len(data)}"] = None
    elif prop_id == 'save-temp-button':
        params.save_temp()
    elif prop_id == 'load-temp-button':
        params.load_temp()

    return paramStore_to_df(params).to_dict('records'), paramStore_commits_df(params).to_dict(
        'records'), params.is_dirty


paramGUI_layout = dbc.Container([html.H1('ParamStore Interface', className="text-center"), dbc.Row([
    dbc.Col([html.H2(['ParamStore', dbc.Collapse(
        dbc.Badge("Dirty state", color='white', text_color='danger', id='dirty-badge', className="border ms-1",
                  pill=True), is_open=False, id='dirty-badge-collapse')]),
             dbc.InputGroup(
                 [
                     dbc.Button('add-key', color='primary', id='add-key-button', className='me-1'),
                     dbc.Button('commit', color='secondary', id='commit-button'),
                     dbc.Input(placeholder='enter commit label', id='commit-label'),
                     dbc.Button('load temp', id="load-temp-button", className='me-1'),
                     dbc.Button('save_temp', id="save-temp-button", className='me-1')
                 ]),
             dash_table.DataTable(
                 columns=[
                     dict(name="key", id="key", type="text", editable=True),
                     dict(name="value", id="value", type="any", editable=True),
                     dict(name="tag", id="tag", type="text", editable=True),
                 ],
                 data=paramStore_to_df(params).to_dict('records'),
                 id='data-table',
                 sort_action='native',
                 row_deletable=True,
                 filter_action='native',
                 editable=True,
                 fixed_rows={'headers': True, 'data': 0}
             )],
            style={"maxHeight": "550px", "overflow": "scroll"}
            , width=6

            ),
    dbc.Col([
        html.H2('Commit Table'),
        dbc.Button('Checkout', id='checkout-button'),
        dash_table.DataTable(
            columns=[
                dict(name="id", id="commit_id", type="text"),
                dict(name="label", id="commit_label", type="text"),
                dict(name="time", id="commit_time", type="datetime"),
            ],
            data=paramStore_commits_df(params).to_dict('records'),
            sort_action='native',
            sort_mode='multi',
            id='commit-table',
            filter_action='native',
            row_selectable='single',
            sort_by=[{'column_id': 'commit_time', 'direction': 'desc'}],
            fixed_rows={'headers': True, 'data': 0},
            editable=True)],
        style={"maxHeight": "550px", "overflow": "scroll"}
        , width=6)
    # , dcc.Interval(id="interval", interval=10000, n_intervals=0)
])]
                                , fluid=True)

app.layout = paramGUI_layout

if __name__ == '__main__':
    app.run_server(debug=True)
