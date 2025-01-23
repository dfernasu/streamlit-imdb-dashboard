from utilities_navigation import get_login_state, show_info_toast, move_to_login
from utilities_snow_data import get_initial_data, get_average_data_per_year, get_pie_count
from utilities_graphs import lines_graph, pie_chart
from pages.st_sidebar import get_filter_sidebar

from global_parameters import *
import streamlit as st
import plotly.express as px

# -----------------------------------------------------------------------
#                              MAIN
# -----------------------------------------------------------------------

show_info_toast()

# Login Check
if not get_login_state():
    move_to_login()

# Data initialization
get_initial_data()

# Sidebar initialization
get_filter_sidebar()

fact_table = st.session_state[FACT_TABLE_KEY]

# Lines Graphs
average_data_columns = ['YEAR_ID', 'USER_SCORE', 'CRITIC_SCORE', 'REVENUE', 'USER_VOTES']
avg_data = get_average_data_per_year(st.session_state[DIM_YEARS_KEY], fact_table[average_data_columns])
years = list(avg_data['YEAR'])

user_score_traces = years, list(avg_data['USER_SCORE'].astype(int)), 'User Score'
critic_score_traces = years, list(avg_data['CRITIC_SCORE'].astype(int)), 'Critic Score'

revenue_traces = years, list(avg_data['REVENUE']), 'Revenue'
user_votes_traces = years, list(avg_data['USER_VOTES']), 'User Votes'

scores_graph = lines_graph('Average User and Critic Score per Year', 'Year', 'Average Score (0-100)', [user_score_traces, critic_score_traces])
revenue_graph = lines_graph('Average Film Revenue per Year', 'Year', 'Average Revenue (million $)', [revenue_traces])
user_votes_graph = lines_graph('Average User Votes per Year', 'Year', 'Average User Votes', [user_votes_traces])

# Pie Graphs
pie_columns = ['USER_SCORE_CATEGORY', 'CRITIC_SCORE_CATEGORY', 'USER_VOTES_CATEGORY', 'REVENUE_CATEGORY']
user_score_count, critic_score_count, user_votes_count, revenue_category_count = get_pie_count(fact_table[pie_columns])

colors = px.colors.qualitative.Plotly
color_discrete_map = {category: color for category, color in zip(SCORE_OPTIONS, colors[:len(SCORE_OPTIONS)])}

user_score_pie_chart = pie_chart('User Scores', 'Count', 'Score', user_score_count, color_discrete_map)
critic_score_pie_chart = pie_chart('Critic Scores', 'Count', 'Score', critic_score_count, color_discrete_map)
user_votes_pie_chart = pie_chart('User Votes', 'Count', 'Score', user_votes_count)
revenue_pie_chart = pie_chart('Revenue Category', 'Count', 'Category', revenue_category_count)

# Page elements

st.title("Film Industry State by Year")
st.subheader("This dashboard shows the state of the film industry over the years, measured by film grades, votes, and revenue.")

col1, col2 = st.columns([3, 1])

with col1:
    st.plotly_chart(revenue_graph)
    st.plotly_chart(user_votes_graph)
    st.plotly_chart(scores_graph)

with col2:
    st.plotly_chart(revenue_pie_chart)
    st.plotly_chart(user_votes_pie_chart)
    st.plotly_chart(user_score_pie_chart)
    st.plotly_chart(critic_score_pie_chart)

