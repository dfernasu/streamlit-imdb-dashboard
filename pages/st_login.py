import streamlit as st
from utilities_snow_connections import validate_credentials
from utilities_navigation import set_credentials, show_info_toast, move_to_dashboard

# -----------------------------------------------------------------------
#                              FUNCTIONS
# -----------------------------------------------------------------------

def login_form():
    st.title("Login")
    st.text("Use your snowflake credentials to access the Dashboard")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    login_button = st.button("Login")

    if login_button:
        if validate_credentials(username, password):
            set_credentials(username, password)
            move_to_dashboard()
        else:
            st.error("Invalid credentials")

# -----------------------------------------------------------------------
#                                 MAIN
# -----------------------------------------------------------------------

show_info_toast()
login_form()