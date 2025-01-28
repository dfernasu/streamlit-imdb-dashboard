# ///////////////////////////////////////////////////////////////////////
#
#                       UTILITIES SNOW DATA
#   Functions to retrieve and transform data from the IMDB_DWH schema,
#   stored in Snowflake.
#
# ///////////////////////////////////////////////////////////////////////

from snowflake.connector import SnowflakeConnection
from utilities_db_connections import create_snow_connection, close_snow_connection
from global_parameters import *
import pandas as pd
import streamlit as st

# -----------------------------------------------------------------------
#                          GENERAL FUNCTIONS
# -----------------------------------------------------------------------

def get_score_list(max_score_selected: str):
    if max_score_selected not in SCORE_OPTIONS:
        return None
    else:
        index = SCORE_OPTIONS.index(max_score_selected)

        return SCORE_OPTIONS[:(index + 1)]
    
def list_to_wherein(column: str, data: list, is_int=False):

    if not is_int:
        data_list_str = '(\'' + '\', \''.join(map(str, data)) + '\')'
    else:
        data_list_str = '(' + ', '.join(map(str, data)) + ')'

    return f'{column} IN {data_list_str}'

# -----------------------------------------------------------------------
#               QUERY FUNCTIONS (Snowflake connection needed)
# -----------------------------------------------------------------------

def get_max_revenue(conn: SnowflakeConnection):

    query = f"SELECT MAX(revenue) FROM IMDB_DWH.fact_table"

    cursor = conn.cursor()
    cursor.execute(query)

    return float(cursor.fetchone()[0])

def get_filtered_dimensions(conn: SnowflakeConnection, table_name: str, colum_to_filter: str = None, filter_list: list = None, is_int=False):

    query = f"SELECT * FROM IMDB_DWH.{table_name}"

    if colum_to_filter is not None and filter_list is not None:
        query = query + ' WHERE ' + list_to_wherein(colum_to_filter, filter_list, is_int)
  
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()

    print(f"[SUCCESS] {len(df)} elements retrieved from the table {table_name}")

    return df

def get_filtered_fact_table(conn: SnowflakeConnection, min_revenue: float = 0.0, max_revenue:float = None, year_id_list: list = None, bridge_genre_id_list: list = None, max_user_score: str = None, max_critic_score: str = None):
    
    year_column = 'year_id'
    genre_column = 'bridge_genre_id'
    user_score_column = 'user_score_category'
    critic_score_column = 'critic_score_category'
    user_score_list = get_score_list(max_user_score)
    critic_score_list = get_score_list(max_critic_score)

    query = f"SELECT * FROM IMDB_DWH.fact_table"
    
    if(max_revenue is None):
        query = query + f" WHERE revenue BETWEEN {min_revenue} AND {get_max_revenue(conn)}"
    else:
        query = query + f" WHERE revenue BETWEEN {min_revenue} AND {max_revenue}"

    if(year_id_list is not None):
        query = query + f" AND {list_to_wherein(year_column, year_id_list, is_int=True)}"
    if(bridge_genre_id_list is not None):
        query = query + f" AND {list_to_wherein(genre_column, bridge_genre_id_list, is_int=True)}"
    if(user_score_list is not None):
        query = query + f" AND {list_to_wherein(user_score_column, user_score_list)}"
    if(critic_score_list is not None):
        query = query + f" AND {list_to_wherein(critic_score_column, critic_score_list)}"

    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()

    print(f"[SUCCESS] {len(df)} elements retrieved from the fact table")

    return df

# -----------------------------------------------------------------------
#    LOCAL BD FUNCTIONS (Used to store and query data from PostgreSQL)
# -----------------------------------------------------------------------

def save_local_table(table_name: str, data: pd.DataFrame):
    return 0    # TODO: Not yet implemented


# -----------------------------------------------------------------------
#         FIRST CONNECTION FUNCTIONS (only executed the first time) 
# -----------------------------------------------------------------------

def validate_initial_data(cached_value):
    return DIM_YEARS_KEY in st.session_state and DIM_GENRES_KEY in st.session_state and DIM_DIRECTORS_KEY in st.session_state and DIM_ACTORS_KEY in st.session_state

@st.cache_resource(validate=validate_initial_data)
def get_initial_data():
    
    # Get data from the snowflake account
    conn = None
    try:
        conn = create_snow_connection()

        dim_years = get_filtered_dimensions(conn, 'dim_years')
        dim_genres = get_filtered_dimensions(conn, 'dim_genres')
        dim_directors = get_filtered_dimensions(conn, 'dim_directors') 
        dim_actors = get_filtered_dimensions(conn, 'dim_actors')

        bridge_genres = get_filtered_dimensions(conn, 'bridge_genres')
        bridge_actors = get_filtered_dimensions(conn, 'bridge_actors')

        max_revenue = get_max_revenue(conn)
        fact_table = get_filtered_fact_table(conn)
        
    finally:
        close_snow_connection(conn)

    # Save data into session
    st.session_state[DIM_YEARS_KEY] = dim_years
    st.session_state[DIM_GENRES_KEY] = dim_genres
    st.session_state[DIM_DIRECTORS_KEY] = dim_directors
    st.session_state[DIM_ACTORS_KEY] = dim_actors
    
    st.session_state[BRIDGE_ACTORS_KEY] = bridge_actors
    st.session_state[BRIDGE_GENRES_KEY] = bridge_genres

    st.session_state[FACT_TABLE_KEY] = fact_table

    st.session_state[COMPLETE_YEAR_LIST_KEY] = dim_years['YEAR']
    st.session_state[COMPLETE_YEAR_IDS_LIST_KEY] = dim_years['YEAR_ID']
    st.session_state[COMPLETE_GENRE_LIST_KEY] = dim_genres['GENRE_NAME']
    st.session_state[MAX_REVENUE_KEY] = max_revenue

    st.session_state[CHECKBOXES_YEAR_STATES_KEY] = [True] * len(st.session_state[COMPLETE_YEAR_IDS_LIST_KEY])

    st.session_state[INITIAL_DATA_KEY] = True

    # TODO: Save data into the local db

# -----------------------------------------------------------------------
#       GRAPH FUNCTIONS (used to create charts from the dataframes) 
# -----------------------------------------------------------------------

def get_average_data_per_year(dim_year: pd.DataFrame, fact_table_reduced: pd.DataFrame):
    
    average_data_per_year = fact_table_reduced.merge(dim_year, on='YEAR_ID')
    average_data_per_year = average_data_per_year.groupby('YEAR_ID').mean().reset_index()

    return average_data_per_year

def get_top_directors(dim_director: pd.DataFrame, fact_table_reduced: pd.DataFrame):

    average_data_per_director = fact_table_reduced.merge(dim_director, on='DIRECTOR_ID')
    
    average_data_per_director = average_data_per_director[['REVENUE', 'DIRECTOR_NAME', 'USER_SCORE']]
    average_data_per_director = average_data_per_director.groupby('DIRECTOR_NAME').mean().reset_index()

    top_directors_data = average_data_per_director.sort_values(by=['REVENUE'], ascending=False).head(15)
    return top_directors_data[['REVENUE', 'DIRECTOR_NAME', 'USER_SCORE']]

def get_top_genres(dim_genres: pd.DataFrame, bridge_genres: pd.DataFrame, fact_table_reduced: pd.DataFrame):

    average_data_per_genre = fact_table_reduced.merge(bridge_genres, on='FILM_RANK')
    average_data_per_genre = average_data_per_genre.merge(dim_genres, on='GENRE_ID')

    average_data_per_genre = average_data_per_genre[['REVENUE', 'GENRE_NAME', 'USER_SCORE']]
    average_data_per_genre = average_data_per_genre.groupby('GENRE_NAME').mean().reset_index()

    top_genres_data = average_data_per_genre.sort_values(by=['REVENUE'], ascending=False).head(5)
    return top_genres_data[['REVENUE', 'GENRE_NAME', 'USER_SCORE']]

def get_top_actors(dim_actors: pd.DataFrame, bridge_actors: pd.DataFrame, fact_table_reduced: pd.DataFrame):

    average_data_per_actor = fact_table_reduced.merge(bridge_actors, on='FILM_RANK')
    average_data_per_actor = average_data_per_actor.merge(dim_actors, on='ACTOR_ID')

    average_data_per_actor = average_data_per_actor[['REVENUE', 'ACTOR_NAME', 'USER_SCORE']]
    average_data_per_actor = average_data_per_actor.groupby('ACTOR_NAME').mean().reset_index()

    top_actors_data = average_data_per_actor.sort_values(by=['REVENUE'], ascending=False).head(15)
    return top_actors_data[['REVENUE', 'ACTOR_NAME', 'USER_SCORE']]

def get_pie_count(fact_table_reduced: pd.DataFrame):

    user_score_count = fact_table_reduced['USER_SCORE_CATEGORY'].value_counts()
    critic_score_count = fact_table_reduced['CRITIC_SCORE_CATEGORY'].value_counts()
    user_votes_count = fact_table_reduced['USER_VOTES_CATEGORY'].value_counts()
    revenue_category_count = fact_table_reduced['REVENUE_CATEGORY'].value_counts()

    user_score_count = pd.DataFrame({'USER_SCORE_CATEGORY':user_score_count.index, 'COUNT':user_score_count.values})
    critic_score_count = pd.DataFrame({'CRITIC_SCORE_CATEGORY':critic_score_count.index, 'COUNT':critic_score_count.values})
    user_votes_count = pd.DataFrame({'USER_VOTES_CATEGORY':user_votes_count.index, 'COUNT':user_votes_count.values})
    revenue_category_count = pd.DataFrame({'REVENUE_CATEGORY':revenue_category_count.index, 'COUNT':revenue_category_count.values})

    user_score_count = user_score_count.sort_values('USER_SCORE_CATEGORY', ascending=False)
    critic_score_count = critic_score_count.sort_values('CRITIC_SCORE_CATEGORY', ascending=False)

    return user_score_count, critic_score_count, user_votes_count, revenue_category_count

def get_merged_genres(dim_genres: pd.DataFrame, bridge_genres: pd.DataFrame, fact_table: pd.DataFrame):

    merged_fact_table = fact_table.merge(bridge_genres, on='FILM_RANK')
    merged_fact_table = merged_fact_table.merge(dim_genres, on='GENRE_ID')

    return merged_fact_table

def get_merged_actors(dim_actors: pd.DataFrame, bridge_actors: pd.DataFrame, fact_table: pd.DataFrame):

    merged_fact_table = fact_table.merge(bridge_actors, on='FILM_RANK')
    merged_fact_table = merged_fact_table.merge(dim_actors, on='ACTOR_ID')

    return merged_fact_table

def get_merged_directors(dim_director: pd.DataFrame, fact_table: pd.DataFrame):

    merged_fact_table = fact_table.merge(dim_director, on='DIRECTOR_ID')
    return merged_fact_table

def get_merged_years(dim_year: pd.DataFrame, fact_table: pd.DataFrame):

    merged_fact_table = fact_table.merge(dim_year, on='YEAR_ID')
    return merged_fact_table

def get_average_score(fact_table: pd.DataFrame):

    fact_table['AVERAGE_SCORE'] = fact_table[['USER_SCORE', 'CRITIC_SCORE']].mean(axis=1)
    return fact_table