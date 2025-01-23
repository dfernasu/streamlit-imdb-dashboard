# ///////////////////////////////////////////////////////////////////////
#
#                           UTILITIES GRAPHS
#   Functions to create Graphics using Plotly.
#
# ///////////////////////////////////////////////////////////////////////

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

# -----------------------------------------------------------------------
#                             GRAPHS
# -----------------------------------------------------------------------

def lines_graph(title: str, x_axis_title: str, y_axis_title: str, traces: list):

    fig = go.Figure()

    for trace in traces:
        list_x , list_y, trace_name = trace

        fig.add_trace(go.Scatter(x=list_x, y=list_y, mode='lines+markers', name=trace_name))

    fig.update_layout(
        title=title,
        xaxis_title=x_axis_title,
        yaxis_title=y_axis_title,
        template='plotly_dark'
    )

    return fig

def horizontal_bars_graph(title: str, x_axis_title: str, y_axis_title: str, color_title: str, data: pd.DataFrame):

    column0 = data.columns[0]
    column1 = data.columns[1]
    column2 = data.columns[2]

    labels = {column0: x_axis_title, column1: y_axis_title, column2: color_title}

    fig = px.bar(data, x=column0, y=column1, title=title, color=column2, labels=labels, color_continuous_scale='Peach')

    return fig

def pie_chart(title: str, names_title: str, values_title: str, data: pd.DataFrame, color_discrete_map = None):

    column0 = data.columns[0]
    column1 = data.columns[1]

    labels = {column0: names_title, column1: values_title}

    if color_discrete_map is None:
        fig = px.pie(data, title=title, names=column0, values=column1, labels=labels)
    else:
        fig = px.pie(data, title=title, names=column0, values=column1, labels=labels, color=column0, color_discrete_map=color_discrete_map)

    return fig

def bubble_chart(title: str, x_axis_title: str, y_axis_title: str, size_title: str, color_title: str, data: pd.DataFrame):

    column0 = data.columns[0]
    column1 = data.columns[1]
    column2 = data.columns[2]
    column3 = data.columns[3]

    labels = {column0: x_axis_title, column1: y_axis_title, column2: size_title, column3: color_title}

    fig = px.scatter(data, x=column0, y=column1, title=title, size=column2, color=column3, labels=labels)

    return fig
