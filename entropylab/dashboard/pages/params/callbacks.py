from dash import callback_context
from dash.dependencies import Input, Output, State

from entropylab.pipeline.api.param_store import ParamStore
from entropylab.dashboard.pages.params.utils import (
    param_store_to_df,
    param_store_to_commits_df,
    data_diff,
)


def register_callbacks(app, param_store: ParamStore):
    @app.callback(
        [
            Output("data-table", "data"),
            Output("commit-table", "data"),
            Output("dirty-badge-collapse", "is_open"),
        ],
        Input("data-table", "data"),
        Input("commit-button", "n_clicks"),
        Input("add-key-button", "n_clicks"),
        Input("checkout-button", "n_clicks"),
        Input("save-temp-button", "n_clicks"),
        Input("load-temp-button", "n_clicks"),
        Input("commit-table", "selected_rows"),
        Input("commit-table", "data"),
        # Input("interval", "n_intervals"),
        State("data-table", "active_cell"),
        State("data-table", "data_previous"),
        State("commit-label", "value"),
        prevent_initial_call=True,
    )
    def button_callback(
        data,
        commit_click,
        add_click,
        checkout_click,
        save_temp_click,
        load_temp_click,
        commits_selected_rows,
        commits_data,
        # interval,
        active_cell,
        data_prev,
        commit_label,
    ):
        prop_id = callback_context.triggered[0]["prop_id"].split(".")[0]

        if prop_id == "commit-button":
            param_store.commit(commit_label)
        elif prop_id == "checkout-button":
            if commits_selected_rows and len(commits_selected_rows) > 0:
                commit = commits_data[commits_selected_rows[0]]
                param_store.checkout(commit["commit_id"])
        elif prop_id == "data-table":
            data_diff(param_store, data, data_prev)
        elif prop_id == "add-key-button":
            param_store[f"newKey{len(data)}"] = None
        elif prop_id == "save-temp-button":
            param_store.save_temp()
        elif prop_id == "load-temp-button":
            param_store.load_temp()

        return (
            param_store_to_df(param_store).to_dict("records"),
            param_store_to_commits_df(param_store).to_dict("records"),
            param_store.is_dirty,
        )
