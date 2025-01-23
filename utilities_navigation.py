# ///////////////////////////////////////////////////////////////////////
#
#                           UTILITIES NAVIGATION
#   Functions to configure the navigation and login state of the Streamlit App.
#
# ///////////////////////////////////////////////////////////////////////

import streamlit as st

# -----------------------------------------------------------------------
#                          GLOBAL PARAMETERS
# -----------------------------------------------------------------------

SNOWFLAKE_USER_KEY = 'SNOWFLAKE_USER'
SNOWFLAKE_PASS_KEY = 'SNOWFLAKE_PASS'
LOGIN_STATE_KEY = 'LOGIN_STATE_KEY'

PAGE_LOGIN = st.Page("pages/st_login.py", title="Log Into Snowflake", icon="📥")
PAGE_DASHBOARD_YEAR_STATE = st.Page("pages/st_dashboard_year_state.py", title="Dashboard: Metrics by Year", icon='📈')
PAGE_DASHBOARD_TOP_DATA = st.Page("pages/st_dashboard_top_data.py", title="Dashboard: Top Metrics", icon='📈')

INFO_TOAST_KEY = 'info_toast'

# -----------------------------------------------------------------------
#                      FUNCTIONS FOR LOGIN
# -----------------------------------------------------------------------

def set_credentials(username, password):
    st.session_state[SNOWFLAKE_USER_KEY] = username
    st.session_state[SNOWFLAKE_PASS_KEY] = password
    st.session_state[LOGIN_STATE_KEY] = True

def get_login_state():
    if LOGIN_STATE_KEY in st.session_state:
        return st.session_state[LOGIN_STATE_KEY]
    else:
        return False

def get_credentials():
    if get_login_state():
        return st.session_state[SNOWFLAKE_USER_KEY], st.session_state[SNOWFLAKE_PASS_KEY]
    else:
        return None, None
    
# -----------------------------------------------------------------------
#                      FUNCTIONS FOR NAVIGATION
# -----------------------------------------------------------------------

def save_info_toast(msg, icon):
    info_toast = {
        'msg': msg,
        'icon': icon
    }

    st.session_state[INFO_TOAST_KEY] = info_toast

def show_info_toast():
    if INFO_TOAST_KEY in st.session_state:
        if st.session_state[INFO_TOAST_KEY] is not None:
            info_toast = st.session_state[INFO_TOAST_KEY]
            st.session_state[INFO_TOAST_KEY] = None
            st.toast(info_toast['msg'], icon=info_toast['icon'])

def logout():
    st.session_state.clear()
    save_info_toast('You have logged out successfully', icon='🔴')
    st.switch_page(PAGE_LOGIN)

def move_to_dashboard():
    save_info_toast('You have logged in successfully', icon='🟢')
    st.switch_page(PAGE_DASHBOARD_YEAR_STATE)

def move_to_login():
    save_info_toast('You have to be logged to view this page.', icon='⚠️')
    st.switch_page(PAGE_LOGIN)

def initial_navigation():
    pg = st.navigation([PAGE_LOGIN, PAGE_DASHBOARD_YEAR_STATE, PAGE_DASHBOARD_TOP_DATA, st.Page(logout, title='Log Out', icon='📤')])
    pg.run()