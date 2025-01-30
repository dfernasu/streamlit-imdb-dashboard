import streamlit as st
from utilities_navigation import initial_navigation
import logging as log
import os

LOG_PATH = 'app.log'

@st.cache_resource(show_spinner=False)
def truncate_log_file(max_lines=800, lines_to_leave=600):

    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if(len(lines) > max_lines):
            with open(LOG_PATH, 'w', encoding='utf-8') as file:
                file.writelines(lines[-lines_to_leave:])

@st.cache_resource(show_spinner=False)
def conf_log():
    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        handlers=[
            log.FileHandler(LOG_PATH, mode='a', encoding='utf-8'),
            log.StreamHandler()
        ]
    )

st.set_page_config(
    page_title="IMDB Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state='expanded')

truncate_log_file()
conf_log()

initial_navigation()