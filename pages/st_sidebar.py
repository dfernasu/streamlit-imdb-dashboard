from utilities_snow_connections import create_connection, close_connection
from utilities_snow_data import get_filtered_dimensions, get_filtered_fact_table
from global_parameters import *
import streamlit as st

# -----------------------------------------------------------------------
#                               FILTERS
# -----------------------------------------------------------------------

def apply_filters():

    conn = None
    try:
        conn = create_connection()

        #Selected values initialization
        selected_years = st.session_state[COMPLETE_YEAR_IDS_LIST_KEY]
        selected_genres = st.session_state[COMPLETE_GENRE_LIST_KEY]

        min_revenue = 0.0
        max_revenue = st.session_state[MAX_REVENUE_KEY]

        max_user_score = SCORE_OPTIONS[-1]
        max_critic_score = SCORE_OPTIONS[-1]

        # YEARS
        #if(CHECKBOX_YEAR_CONTAINER_KEY in st.session_state):
        
        selected_years = []
        for id in st.session_state[COMPLETE_YEAR_IDS_LIST_KEY]:
            if(st.session_state[CHECKBOX_YEAR_ID_PREFIX + str(id)]):
                selected_years.append(id)

        #if len(selected_years) == len(year_ids):
        #    selected_years = None

        st.session_state[DIM_YEARS_KEY] = get_filtered_dimensions(conn, 'dim_years', 'YEAR_ID', selected_years, is_int=True)

        # GENRES
        if(CHECKBOX_GENRES_KEY in st.session_state):
            selected_genres = []
            if st.session_state[CHECKBOX_GENRES_KEY]:
                selected_genres = None
            else:
                selected_genres = st.session_state[MULTISELECT_GENRES_KEY]

            dim_genres = get_filtered_dimensions(conn, 'dim_genres', 'GENRE_NAME', selected_genres, is_int=False)
            st.session_state[DIM_GENRES_KEY] = dim_genres
            st.session_state[BRIDGE_GENRES_KEY] = get_filtered_dimensions(conn, 'bridge_genres', 'GENRE_ID', list(dim_genres['GENRE_ID']), is_int=True)

        # REVENUE
        if(RANGE_REVENUE_KEY in st.session_state):
            min_revenue, max_revenue = st.session_state[RANGE_REVENUE_KEY]
            #st.write(f'Revenue interval selected: {min_revenue} - {max_revenue}')

        # MAX SCORES
        if(SLIDER_USER_SCORE_KEY in st.session_state and SLIDER_CRITIC_SCORE_KEY in st.session_state):
            max_user_score = st.session_state[SLIDER_USER_SCORE_KEY]
            max_critic_score = st.session_state[SLIDER_CRITIC_SCORE_KEY]
            #st.write(f'Max scores selected: For users: {max_user_score}. For critics: {max_critic_score}')

        # FACT TABLE
        bridge_genres = st.session_state[BRIDGE_GENRES_KEY]
        selected_bridge_genres_ids = list(bridge_genres['BRIDGE_GENRE_ID'])
        
        st.session_state[FACT_TABLE_KEY] = get_filtered_fact_table(conn, min_revenue, max_revenue, selected_years, selected_bridge_genres_ids, max_user_score, max_critic_score)
        #st.write(f'Number of rows filtered in the fact table: {len(st.session_state[FACT_TABLE_KEY])}')

    finally:
        close_connection(conn)

# -----------------------------------------------------------------------
#                              WIDGETS
# -----------------------------------------------------------------------

def checkbox_container(title: str, container_key: str, data: list, data_ids: list):
    st.subheader(title)

    with st.container(key = container_key):
        col1, col2 = st.columns(2)

        for i in range(len(data)):
            label = str(data[i])
            id = CHECKBOX_YEAR_ID_PREFIX + str(data_ids[i])
            state = st.session_state[CHECKBOXES_YEAR_STATES_KEY][i]

            if i % 2 == 0:
                with col1:
                    st.checkbox(label, key=id, value=state, on_change=apply_filters)
            else:
                with col2:
                    st.checkbox(label, key=id, value=state, on_change=apply_filters)

def multiselect_container(title: str, label: str, data: list, multiselect_key: str, checkbox_key: str, max_selections: int, placeholder: str):
    st.subheader(title)

    multiselect_checkbox = st.checkbox("Select all", key=checkbox_key, value=True)
    
    format = lambda x: str.capitalize(x)
    st.multiselect(label, options=data, key=multiselect_key, format_func=format, max_selections=max_selections, placeholder=placeholder, on_change=apply_filters, disabled=multiselect_checkbox)

    if multiselect_checkbox:
        apply_filters()

def range_float_container(title: str, label: str, range_key: str, min_value: float, max_value: float):
    st.subheader(title)

    st.slider(label=label, min_value=min_value, max_value=max_value, value=(min_value, max_value) ,key=range_key, on_change=apply_filters)

def slider_container(title: str, label: str, slider_key: str, options: list, default_option: str):
    st.subheader(title)

    st.select_slider(label=label, key=slider_key, options=options, value=default_option, on_change=apply_filters)

# -----------------------------------------------------------------------
#                               SIDEBAR
# -----------------------------------------------------------------------

def get_filter_sidebar():

    sidebar = st.sidebar

    with sidebar:
        st.header('Filters')

        if(INITIAL_DATA_KEY in st.session_state):
            checkbox_container('Years', CHECKBOX_YEAR_CONTAINER_KEY, st.session_state[COMPLETE_YEAR_LIST_KEY], st.session_state[COMPLETE_YEAR_IDS_LIST_KEY])
            multiselect_container('Genres', 'Genres to filter', st.session_state[COMPLETE_GENRE_LIST_KEY], MULTISELECT_GENRES_KEY, CHECKBOX_GENRES_KEY, 5, 'Select 1 to 5 genres')
            range_float_container('Revenue', 'Revenue interval to select', RANGE_REVENUE_KEY, 0.0, st.session_state[MAX_REVENUE_KEY])
            slider_container('User score', 'Select the maximum user score', SLIDER_USER_SCORE_KEY, SCORE_OPTIONS, SCORE_OPTIONS[-1])
            slider_container('Critic score', 'Select the maximum critic score', SLIDER_CRITIC_SCORE_KEY, SCORE_OPTIONS, SCORE_OPTIONS[-1])

    return sidebar