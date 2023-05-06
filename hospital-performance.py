import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import plotly.express as px

st.set_page_config(
     page_title="Hospital Performance Measures",
     page_icon="🩺",
     layout="centered",
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
#st.success("Connected to Snowflake!")

st.header("Hospital Performance Measures")
st.write("Using Inpatient Mortality Indicators (IMI) across the most prevelant disease conditions to compare >300 California Hospitals' Performance")

tab1, tab2, tab3, tab4 = st.tabs(["🌎 Overview", "🏆 Top N","📈 Procedure Trend", "🏥 Hospital Scorecard"])

# Load data table
@st.cache_data
def load_data(tbl):
    #snow_df1_map = session.table().group_by("hospital","county","longitude","latitude",).sum("num_cases")
    df = session.table(tbl)
    return df.to_pandas()


    ## Display data table


    # highest and lowest IMI rating by procedure
    # lookup ratings by hospital for all procedures with a indicator
    # plotly map with bubble for cases, procedure selector, rating
    # https://plotly.com/python/bubble-maps/
    # https://12ft.io/proxy?&q=https%3A%2F%2Ftowardsdatascience.com%2F3-easy-ways-to-include-interactive-maps-in-a-streamlit-app-b49f6a22a636
    # county



with tab1:
    pd_df1_map = load_data("HOSPITAL_DB.PUBLIC.VW_HOSP_MAP")
    st.map(pd_df1_map)
    fig = px.scatter_geo(pd_df1_map,lat=pd_df1_map.LATITUDE,lon=pd_df1_map.LONGITUDE, hover_name="HOSPITAL", size="NUM_CASES", scope="usa", fitbounds='locations',locationmode='ISO-3')
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("See Table"):
        st.dataframe(pd_df1_map)

with tab2:
    pd_df2_top = load_data("HOSPITAL_DB.PUBLIC.VW_HOSP_RATING")
    proc_list = pd_df2_top.sort_values(by='PROCEDURE').PROCEDURE.unique()
    proc_to_filter = st.selectbox('Select a Procedure:', proc_list, index=2)

    st.subheader('Best performing hositals: Lowest Risk Adjusted Mortality')
    top_hosp = pd_df2_top[pd_df2_top.PROCEDURE.eq(proc_to_filter) & pd_df2_top.HOSPITAL_RATING.eq('Better')]
    st.dataframe(top_hosp[["HOSPITAL","COUNTY","RISK_ADJ_MORTALITY"]],use_container_width=True)

    st.subheader('Worst performing hositals: Highest Risk Adjusted Mortality')
    bottom_hosp = pd_df2_top[pd_df2_top.PROCEDURE.eq(proc_to_filter) & pd_df2_top.HOSPITAL_RATING.eq('Worse')].sort_values(by="RISK_ADJ_MORTALITY", ascending=False)
    st.dataframe(bottom_hosp[["HOSPITAL","COUNTY","RISK_ADJ_MORTALITY"]],use_container_width=True)

with tab3:
    pd_df3 = load_data("HOSPITAL_DB.PUBLIC.VW_PROC_TREND")

    st.subheader("Acute Stroke")
    pd_df3_stroke = pd_df3[pd_df3.PROCEDURE.eq("Acute Stroke")]
    st.line_chart(data=pd_df3_stroke, x="RPT_YEAR", y="AVG_IMI")
    with st.expander("See Table"):
        st.dataframe(pd_df3_stroke)

    st.subheader("Acute Myocardial Infarction (AMI)")
    pd_df3_ami= pd_df3[pd_df3.PROCEDURE.eq("AMI")]
    st.line_chart(data=pd_df3_ami, x="RPT_YEAR", y="AVG_IMI")
    with st.expander("See Table"):
        st.dataframe(pd_df3_ami)

    st.subheader("Heart Failure")
    pd_df3_hf= pd_df3[pd_df3.PROCEDURE.eq("Heart Failure")]
    st.line_chart(data=pd_df3_hf, x="RPT_YEAR", y="AVG_IMI")

    st.subheader("Hip Fracture")
    pd_df3_hip= pd_df3[pd_df3.PROCEDURE.eq("Hip Fracture")]
    st.line_chart(data=pd_df3_hip, x="RPT_YEAR", y="AVG_IMI")

    st.subheader("PCI")
    pd_df3_pci= pd_df3[pd_df3.PROCEDURE.eq("PCI")]
    st.line_chart(data=pd_df3_pci, x="RPT_YEAR", y="AVG_IMI")

    st.subheader("Pneumonia")
    pd_df3_pn= pd_df3[pd_df3.PROCEDURE.eq("Pneumonia")]
    st.line_chart(data=pd_df3_pn, x="RPT_YEAR", y="AVG_IMI")

with tab4:
    pd_df4 = load_data("HOSPITAL_DB.PUBLIC.VW_HOSP_DTL")
    hosp_list = pd_df4.sort_values(by="HOSPITAL").HOSPITAL.unique()
    hosp_to_filter = st.selectbox('Select a Hospital:', hosp_list)
    st.write("County: " + pd_df4.iloc[1][1])

    st.subheader("Acute Stroke")
    pd_df4_hs= pd_df4[pd_df4.HOSPITAL.eq(hosp_to_filter) & pd_df4.PROCEDURE.eq("Acute Stroke")]
    df_hs = pd_df4_hs[["RPT_YEAR","RATING_INDICATOR","PROCEDURE"]]
    st.write(df_hs.pivot(index='PROCEDURE', columns='RPT_YEAR', values='RATING_INDICATOR'))

    st.subheader("Acute Myocardial Infarction (AMI)")
    pd_df4_hs= pd_df4[pd_df4.HOSPITAL.eq(hosp_to_filter) & pd_df4.PROCEDURE.eq("AMI")]
    df_hs = pd_df4_hs[["RPT_YEAR","RATING_INDICATOR","PROCEDURE"]]
    st.write(df_hs.pivot(index='PROCEDURE', columns='RPT_YEAR', values='RATING_INDICATOR'))

    st.subheader("Heart Failure")
    pd_df4_hs= pd_df4[pd_df4.HOSPITAL.eq(hosp_to_filter) & pd_df4.PROCEDURE.eq("Heart Failure")]
    df_hs = pd_df4_hs[["RPT_YEAR","RATING_INDICATOR","PROCEDURE"]]
    st.write(df_hs.pivot(index='PROCEDURE', columns='RPT_YEAR', values='RATING_INDICATOR'))

    st.subheader("Hip Fracture")
    pd_df4_hs= pd_df4[pd_df4.HOSPITAL.eq(hosp_to_filter) & pd_df4.PROCEDURE.eq("Hip Fracture")]
    df_hs = pd_df4_hs[["RPT_YEAR","RATING_INDICATOR","PROCEDURE"]]
    st.write(df_hs.pivot(index='PROCEDURE', columns='RPT_YEAR', values='RATING_INDICATOR'))

    st.subheader("Percutaneous Coronary Intervention (PCI)")
    pd_df4_hs= pd_df4[pd_df4.HOSPITAL.eq(hosp_to_filter) & pd_df4.PROCEDURE.eq("PCI")]
    df_hs = pd_df4_hs[["RPT_YEAR","RATING_INDICATOR","PROCEDURE"]]
    st.write(df_hs.pivot(index='PROCEDURE', columns='RPT_YEAR', values='RATING_INDICATOR'))

    st.subheader("Pneumonia")
    pd_df4_hs= pd_df4[pd_df4.HOSPITAL.eq(hosp_to_filter) & pd_df4.PROCEDURE.eq("Pneumonia")]
    df_hs = pd_df4_hs[["RPT_YEAR","RATING_INDICATOR","PROCEDURE"]]
    st.write(df_hs.pivot(index='PROCEDURE', columns='RPT_YEAR', values='RATING_INDICATOR'))

st.caption("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")
