import streamlit as st
from utilities_navigation import initial_navigation
import logging as log

log.basicConfig(
    level=log.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        log.FileHandler('app.log', mode='w', encoding='utf-8'),
        log.StreamHandler()
    ]
)

st.set_page_config(
    page_title="IMDB Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state='expanded')

initial_navigation()