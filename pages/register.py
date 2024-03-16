import streamlit as st
from snowflake.snowpark.session import Session

st.set_page_config(page_title="Simple Data Management Application",
     page_icon="shamrock",
     initial_sidebar_state="collapsed")

connection_parameters = {
    "account": "QGONQBX-JU39310",
    "user": "PRAVEEN",
    "password": "Prawin@276",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN",
    "database": "MANAGE_DB",
    "schema": "EXTERNAL_STAGES"
}

session = Session.builder.configs(connection_parameters).create()

with st.form("register"):
   username=st.text_input("Username")
   password=st.text_input("Password", type="password")
   confirm_password=st.text_input("Confirm Password", type="password")
   role=st.selectbox("Select the role",("Admin", "Reviewer", "Restricted User"),index=None,placeholder="Choose an option")

   submitted = st.form_submit_button("Submit")
   if submitted:
    if password==confirm_password and username:
          create_db = session.sql(f"CREATE TABLE IF NOT EXISTS sdm_users (username varchar(255),password varchar(255), role varchar(255))").collect()
          if create_db:
            username_check = session.sql(f"select username from sdm_users where username='{username.lower()}'").collect()
            if username_check:
                st.error("Username already exist please choose a different one")
            else:
                session.sql(f"INSERT INTO sdm_users (username,password,role) VALUES('{username.lower()}','{password}','{role}')").collect()
                st.info("Registration Completed")
    else:
        st.warning("Your password does not match or  username is empty")

if st.button("<- Back to login"):
    st.switch_page("myapp.py")
        