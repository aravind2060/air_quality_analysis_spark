import dash, pandas as pd, plotly.express as px
from dash import dcc, html
from sqlalchemy import create_engine

ENG = create_engine("postgresql+psycopg2://airq:airq@localhost:5432/airq")

def load():
    return pd.read_sql("select * from air_quality_live order by event_time", ENG)

app = dash.Dash(__name__)
app.layout = html.Div([
    html.H2("Air-Quality Dashboard"),
    dcc.Interval(id="tick", interval=30_000, n_intervals=0),
    dcc.Graph(id="line"),  dcc.Graph(id="spikes"),
    dcc.Graph(id="aqi"),   dcc.Graph(id="scatter")
])

@app.callback(
    [dash.Output("line","figure"),
     dash.Output("spikes","figure"),
     dash.Output("aqi","figure"),
     dash.Output("scatter","figure")],
    dash.Input("tick","n_intervals"))
def refresh(_):
    df = load()
    fig1 = px.line(df, x="event_time", y=["pm25","pm25_pred"],
                   title="Actual vs Predicted PM2.5")
    fig2 = px.timeline(df[df.pm25>35], x_start="event_time", x_end="event_time",
                       y="location_id", title="PM2.5 Spikes (>35)")
    fig3 = px.pie(df, names="aqi_category", title="AQI Breakdown")
    fig4 = px.scatter(df, x="temperature", y="pm25",
                      color="humidity", title="PM2.5 vs Temperature")
    return fig1, fig2, fig3, fig4

if __name__ == "__main__":
    app.run_server(debug=True)
