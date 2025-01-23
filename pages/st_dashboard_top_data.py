from utilities_navigation import get_login_state, show_info_toast, move_to_login
from utilities_snow_data import get_initial_data, get_top_directors, get_top_genres, get_top_actors, get_merged_genres, get_merged_actors, get_merged_directors, get_merged_years, get_average_score
from utilities_graphs import horizontal_bars_graph, bubble_chart
from pages.st_sidebar import get_filter_sidebar

from global_parameters import *
import streamlit as st

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


# Graphs
fact_table = st.session_state[FACT_TABLE_KEY]

# Methods for horizontal graphs
top_directors_columns = ['FILM_RANK', 'DIRECTOR_ID', 'REVENUE', 'USER_SCORE']
top_directors_data = get_top_directors(st.session_state[DIM_DIRECTORS_KEY], fact_table[top_directors_columns])

top_genres_columns = ['FILM_RANK', 'BRIDGE_GENRE_ID', 'REVENUE', 'USER_SCORE']
top_genres_data = get_top_genres(st.session_state[DIM_GENRES_KEY], st.session_state[BRIDGE_GENRES_KEY], fact_table[top_genres_columns])

top_actors_columns = ['FILM_RANK', 'BRIDGE_ACTOR_ID', 'REVENUE', 'USER_SCORE']
top_actors_data = get_top_actors(st.session_state[DIM_ACTORS_KEY], st.session_state[BRIDGE_ACTORS_KEY], fact_table[top_actors_columns])

# Methods for bubble graphs
bubble_chart_columns = ['FILM_RANK', 'BRIDGE_GENRE_ID', 'BRIDGE_ACTOR_ID', 'DIRECTOR_ID', 'YEAR_ID', 'USER_SCORE', 'CRITIC_SCORE', 'REVENUE']
fact_table_reduced = fact_table[bubble_chart_columns]

fact_table_top_data = fact_table[['FILM_RANK', 'BRIDGE_GENRE_ID', 'BRIDGE_ACTOR_ID', 'DIRECTOR_ID', 'REVENUE', 'USER_SCORE']]
top_genres = list(top_genres_data['GENRE_NAME'])
top_actors = list(top_actors_data['ACTOR_NAME'])
top_directors = list(top_directors_data['DIRECTOR_NAME'])

fact_table_reduced = get_average_score(fact_table_reduced)
fact_table_reduced = get_merged_years(st.session_state[DIM_YEARS_KEY], fact_table_reduced)
fact_table_reduced = get_merged_directors(st.session_state[DIM_DIRECTORS_KEY], fact_table_reduced)
fact_table_reduced = get_merged_genres(st.session_state[DIM_GENRES_KEY], st.session_state[BRIDGE_GENRES_KEY], fact_table_reduced)
fact_table_reduced = get_merged_actors(st.session_state[DIM_ACTORS_KEY], st.session_state[BRIDGE_ACTORS_KEY], fact_table_reduced)

bubble_chart_columns_ordered = ['REVENUE', 'YEAR', 'AVERAGE_SCORE']
bubble_chart_columns_genres = bubble_chart_columns_ordered + ['GENRE_NAME']
bubble_chart_columns_directors = bubble_chart_columns_ordered + ['DIRECTOR_NAME']
bubble_chart_columns_actors = bubble_chart_columns_ordered + ['ACTOR_NAME']

fact_table_genres = fact_table_reduced[bubble_chart_columns_genres]
fact_table_directors = fact_table_reduced[bubble_chart_columns_directors]
fact_table_actors = fact_table_reduced[bubble_chart_columns_actors]

fact_table_genres = fact_table_genres[fact_table_genres['GENRE_NAME'].isin(top_genres)]
fact_table_genres = fact_table_genres.drop_duplicates()
fact_table_directors = fact_table_directors[fact_table_directors['DIRECTOR_NAME'].isin(top_directors)]
fact_table_actors = fact_table_actors[fact_table_actors['ACTOR_NAME'].isin(top_actors)]


# Page Elements
st.title("Top Metrics")
st.subheader("This dashboard shows which directors, actors, and genres obtained the most revenue or user score, and how they distribute over the years")

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(horizontal_bars_graph('Top 15 Directors by Revenue', 'Revenue', 'Director', 'User Score', top_directors_data))
    st.plotly_chart(horizontal_bars_graph('Top 15 Actors by Revenue', 'Revenue', 'Actor', 'User Score', top_actors_data))
    st.plotly_chart(horizontal_bars_graph('Top 5 Genres by Revenue', 'Revenue', 'Genre', 'User Score', top_genres_data))

with col2:
    st.plotly_chart(bubble_chart('Director Distribution', 'Revenue', 'Year', 'Average Score', 'Director', fact_table_directors))
    st.plotly_chart(bubble_chart('Actor Distribution', 'Revenue', 'Year', 'Average Score', 'Actor', fact_table_actors))
    st.plotly_chart(bubble_chart('Genre Distribution', 'Revenue', 'Year', 'Average Score', 'Genre', fact_table_genres))
