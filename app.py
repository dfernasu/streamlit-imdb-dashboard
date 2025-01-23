import streamlit as st
from utilities_navigation import initial_navigation

st.set_page_config(
    page_title="IMDB Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state='expanded')

initial_navigation()