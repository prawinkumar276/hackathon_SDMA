import streamlit as st
from time import sleep
from snowflake.snowpark.session import Session

st.set_page_config(initial_sidebar_state="collapsed")

connection_parameters = {
    "account": "QGONQBX-JU39310",
    "user": "PRAVEEN",
    "password": "Prawin@276",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN",
    "database": "MANAGE_DB",
    "schema": "EXTERNAL_STAGES"
}
if "session_data" in st.session_state:
    st.switch_page("ingest_data.py")

session = Session.builder.configs(connection_parameters).create()
st.header("Simple Data Management Application")
st.write("Please login to continue")    

username = st.text_input("Username", label_visibility = "visible")
password = st.text_input("Password", type="password")

db_username = ''
db_password = ''

col1, col2 = st.columns(2)
if col1.button("Log in",use_container_width=True):
    try:
        df = session.sql(f"SELECT * FROM sdm_users where username='{username.lower()}'").collect()
    except:
        st.error("Error connecting to the Database")
    else:
        if df:
            db_username=df[0][0]
            db_password=df[0][1]
            role=df[0][2]
            if username==db_username and password==db_password:
                st.session_state.logged_in = True
                st.session_state.session_data = session
                st.session_state.username = username
                st.success("Logged in successfully!")
                sleep(0.5)
                st.switch_page("pages/ingest_data.py")
            else:
                st.error("Incorrect username or password")
        else:
            st.error("Username does not exist")
        
if col2.button("Register",use_container_width=True):
    st.switch_page("pages/register.py")

