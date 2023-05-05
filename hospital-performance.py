import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import avg, sum, col,lit

st.set_page_config(
     page_title="Hospital Performance Measures",
     page_icon="🩺",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "Powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace, this app allows California citizens to easily lookup hospital performance metrics"
     }
)

# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()
st.success("Connected to Snowflake!")

# Load data table
@st.cache_data
def load_data():
    ## Read in data table
    table = session.table("HOSPITAL_DB.PUBLIC.STATE_CASE_TREND")
    
    ## Collect the results. This will run the query and download the data
    table = table.collect()
    return table

st.header("Hospital Performance Measures")
## Display data table

df = load_data()

st.bar_chart(df, x="RPT_YEAR")

with st.expander("See Table"):
    st.dataframe(df)


st.caption("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

