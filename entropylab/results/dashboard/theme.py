import dash_bootstrap_components as dbc

theme_stylesheet = dbc.themes.DARKLY

# Based on https://bootswatch.com/darkly/
colors = [
    "#375a7f",
    "#3a3a3a",
    "#00bc8c",
    "#f39c12",
    "#e74c3c",
    "#3498db",
    "#ad05bd",
]
plot_font_color = "#fff"
plot_legend_font_color = "#fff"
plot_paper_bgcolor = "rgb(48, 48, 48)"
plot_plot_bgcolor = "rgb(173, 181, 189)"

dark_plot_layout = dict(
    font_color=plot_font_color,
    legend_font_color=plot_legend_font_color,
    paper_bgcolor=plot_paper_bgcolor,
    plot_bgcolor=plot_plot_bgcolor,
    margin=dict(b=40, t=40, l=40, r=40),
)
