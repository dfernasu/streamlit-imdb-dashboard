import streamlit as st
from utilities_db_connections import validate_credentials
from utilities_data import get_initial_data
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
            st.success(f"Valid credentials")
            set_credentials(username, password)
            
            get_initial_data_ok = get_initial_data()

            if(get_initial_data_ok is None):
                st.error("Sorry, this user is not allowed to make queries on the database. Try another account.")
            else:
                if get_initial_data_ok:
                    move_to_dashboard()
                else:
                    st.error("An error has occurred while downloading the data")
        else:
            st.error("Invalid credentials")

# -----------------------------------------------------------------------
#                                 MAIN
# -----------------------------------------------------------------------

show_info_toast()
login_form()