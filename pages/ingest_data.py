# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.session import Session
import pandas as pd
from datetime import datetime
from datetime import date
import re
from soda.scan import Scan
import os.path
import humanize
from st_aggrid import AgGrid, JsCode, ColumnsAutoSizeMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from streamlit_pandas_profiling import st_profile_report
from snowflake.snowpark.functions import when_matched, when_not_matched, lit


st.set_page_config(
     page_title="Simple Data Management Application",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
 )

no_sidebar_style = """
    <style>
        div[data-testid="stSidebarNav"] {display: none;}
    </style>
"""
st.markdown(no_sidebar_style, unsafe_allow_html=True)

# Get the current credentials
if "session_data" in st.session_state:
    session = st.session_state.session_data
    username = st.session_state.username
else:
     st.switch_page("myapp.py")

def show_file(name):
    st.session_state['file_name'] = name
    table_name_split = name.split(".")[0]
    st.session_state['table_name'] = re.sub(r"[^a-zA-Z0-9 ]", "_", table_name_split)


# st.write(session.sql('select current_account(), current_warehouse(), current_database(), current_schema()').collect())
stages = session.sql("show stages in MANAGE_DB.EXTERNAL_STAGES").collect()
stages_list = [x["name"] for x in stages if x["type"] != 'INTERNAL']

with st.sidebar:
    st.title("Simple Data Management Application")
    st.write(" ")
    st.write(" ")
    st.write(" ")
    stage_name = st.selectbox("Please choose the stage", stages_list, index=None, placeholder="Please choose the stage")
    if stage_name:
        files = session.sql(f"LIST @MANAGE_DB.EXTERNAL_STAGES.{stage_name}").collect()
        if files:
            files_df = pd.DataFrame(files)
            files_df['full_name'] = files_df['name']
            files_df = files_df[['name', 'full_name', 'size', 'last_modified']]
            files_df['name'] = files_df['name'].apply(lambda x: x.split('/')[-1])

            st.subheader("Files:")
            for i in range(len(files_df)):
                file = files_df.iloc[i]
                with st.expander(file['name']):
                    file_size = humanize.naturalsize(file['size'])
                    f_in = file['full_name'].rindex(file['name'])
                    st.caption(f"Path: :blue[{file['full_name'][:f_in]}]")
                    st.caption(f"Size: :blue[{file_size}]")
                    st.caption(f"Modified on: :blue[{file['last_modified']}]")
                    st.button("Show content", key=i, on_click=show_file, args=[file['name']])

    else:
        st.session_state['file_name'] = ""
        
    for i in range(10):
        st.write(" ")
    
    if st.button("Logout",use_container_width=True):
        del st.session_state.session_data
        st.switch_page("myapp.py")

col1, col2, col3, col4, col5, col6, col7= st.columns(7)
col7.button('Refresh Data')
        
@st.cache_data(ttl=30,show_spinner=False)
def get_file_data(filename):
    file_df = pd.DataFrame(session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv(f"@{stage_name}/{filename}").collect())
    file_df.columns = file_df.columns.str.strip()
    file_df.insert(0, "ID", range(1, len(file_df)+1))
    return file_df

if st.session_state.get('file_name'):
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Data Ingestion", "Data Audit", "Data Analysis","Update Table","Ingestion Graph","Data Quality Checks"])
    
if st.session_state.get('file_name'):
    with tab1:                                       ############# Data Ingestion ##############
        with st.spinner("Getting data..."):
            filename = st.session_state.get('file_name')
            st.subheader(filename)
            df_file = get_file_data(filename)
            gd = GridOptionsBuilder.from_dataframe(df_file)
            gd.configure_default_column(editable=True)
            gd.configure_column("ID", editable=False)
            gd.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=30)
            gd.configure_selection(selection_mode='multiple', use_checkbox=True)
            gd.configure_grid_options(pre_selected_rows=[])
            gd.configure_column(df_file.columns[0], headerCheckboxSelection = True)
            gridoptions = gd.build()

            return_value = AgGrid(
                df_file, 
                gridOptions = gridoptions, 
                allow_unsafe_jscode = True,
                theme = 'balham',
                # height = 200,
                fit_columns_on_grid_load = False
            )

        selected_rows = return_value["selected_rows"]
        dis_ingest_btn = True
        if selected_rows:
            dis_ingest_btn = False
        if st.button("Ingest Data", key="ingestdata", use_container_width=True, disabled=dis_ingest_btn):
            table_name = st.session_state.get('table_name')
            selected_data = session.create_dataframe(selected_rows)
            selected_data = selected_data.drop(selected_data.columns[0])
            # selected_data.write.mode("append").save_as_table(table_name)
            df_col = selected_data.dtypes

            col_str = in_cond_str = up_cond_str = value_str = audit_up_cols = audit_in_cols = audit_in_val_cols = ""
            i = 0
            for col, dtype in df_col:
                col_org = col
                col = col.strip('"')
                if i == 0:
                    col_str += f"{col_org} {dtype}"
                    in_cond_str += f"t['{col}'] == selected_data['{col}']"
                    value_str += f"'{col}': selected_data['{col}']"
                else:
                    col_str += f", {col_org} {dtype}"
                    if i == 1:
                        up_cond_str += f"((t['{col}'] != selected_data['{col}'])"
                    else:
                        up_cond_str += f" | (t['{col}'] != selected_data['{col}'])"
                    value_str += f", '{col}': selected_data['{col}']"
                i += 1

            if len(df_col) > 1:
                up_cond_str += ")"
            curr_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            selected_data = selected_data.with_column("Created Date", lit(curr_date))
            selected_data = selected_data.with_column("Modified Date", lit(None))
            selected_data = selected_data.with_column("SDM User", lit(username))
            meta_cols = ["Created Date", "Modified Date", "SDM User"]
            for col in meta_cols:
                value_str += f", '{col}': selected_data['{col}']"
                col_str += f', "{col}" String'
            value_str = "{" + value_str + "}"
            audit_col_str = col_str + f', METADATA$ACTION String' + f', METADATA$UPDATE String' + f', METADATA$ROW_ID String'

            # Create Table
            tbl_res = session.sql(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_str})").collect()
            if tbl_res:
                #Create Stream Table
                stream_tbl_res = session.sql(f"CREATE STREAM IF NOT EXISTS {table_name}_stream ON TABLE {table_name}").collect()

                #Audit Table Creation
                audit_tbl_res = session.sql(f"CREATE TABLE IF NOT EXISTS {table_name}_audit ({audit_col_str})").collect()

                #Audit Table Creation
                audit_tbl_res = session.sql(f'CREATE TASK IF NOT EXISTS {table_name}_streamtask\
                    WAREHOUSE = COMPUTE_WH\
                    SCHEDULE = \'1 minute\'\
                    WHEN\
                    SYSTEM$STREAM_HAS_DATA(\'{table_name.upper()}_STREAM\')\
                    AS\
                    INSERT INTO {table_name.upper()}_AUDIT SELECT * FROM {table_name.upper()}_STREAM').collect()
                t = session.table(table_name)
                result = t.merge(selected_data, (eval(in_cond_str)),
                [when_matched(eval(up_cond_str)).update(eval(value_str)),
                when_not_matched().insert(eval(value_str))])

                st.info(f"Selected records ingested to the {table_name.upper()} table: {result}")
                    
    with tab2:                                       ############# Data Audit ##############
        audit_table = st.session_state.get('table_name') + "_audit"
        try: 
            audit_df = session.sql(f"SELECT * FROM MANAGE_DB.EXTERNAL_STAGES.{audit_table} WHERE METADATA$ACTION != 'DELETE'").collect()
            audit_df = pd.DataFrame(audit_df)
            block_cols = ["ID", "Created Date", "Modified Date", "METADATA$ACTION", "METADATA$UPDATE", "METADATA$ROW_ID"]
            audit_df_cols = [i for i in audit_df.columns if i not in block_cols]
            audit_df["Action"] = audit_df[["METADATA$ACTION", "METADATA$UPDATE"]].apply(lambda x: "Update" if x["METADATA$ACTION"] == "INSERT" and x["METADATA$UPDATE"] == "true" else "Insert", axis=1)
            audit_df["Date and Time"] = audit_df[["Created Date", "Modified Date"]].apply(lambda x: x["Modified Date"] if x["Modified Date"] else x["Created Date"], axis=1)
            audit_df["New Value"] = audit_df[audit_df_cols].apply(lambda x: x.to_json(), axis=1)
            audit_df['Old Value'] = audit_df.groupby(['ID'])['New Value'].shift(1)
            audit_df = audit_df.sort_values("Date and Time", ascending=False)

            # st.dataframe(audit_df[["Date and Time", "Old Value", "New Value", "Action", "SDM User"]], hide_index=True, use_container_width=True)
            audit_df = audit_df[["Date and Time", "Old Value", "New Value", "Action", "SDM User"]]
            gd = GridOptionsBuilder.from_dataframe(audit_df)
            gd.configure_default_column(editable=False, wrap_text=True)
            gd.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=30)
            gridoptions = gd.build()

            st.markdown('<div style="text-align: right;">Note: <span style="color:#33D1FF">Audit data is refreshed every 1 minute, Please click the \'Refresh Data\' button</span></div>', unsafe_allow_html=True)
            
            AgGrid(
                audit_df, 
                gridOptions = gridoptions,
                columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
                wrapText=  True,
                theme = 'alpine',
                fit_columns_on_grid_load=False
            )
        except:
            st.write("No audit log data!")
            
        
    with tab3:                                       ############# Data Analysis ##############    
        with st.expander("Data Analysis"):
            col1, col2 = st.columns(2)
            with col1:
                options = st.multiselect(
                    label="Please choose the columns..",
                    options=df_file.columns.values,
                    on_change = None,
                    placeholder = "Please choose the columns..", 
                    label_visibility="collapsed")
            dis_download_btn = True
            export = ""
            if options:
                profie_data = df_file.copy()
                profie_data = profie_data[options]
                pr = profie_data.profile_report()
                st_profile_report(pr)
                export=pr.to_html()
                dis_download_btn = False
            with col2:
                st.download_button(label="Download Full Report", data=export, file_name='report.html', disabled=dis_download_btn)
    with tab4:                                       ############# Data Table ##############
        table_name = st.session_state.get('table_name')
        try:
            users = session.sql(f"SELECT * FROM MANAGE_DB.EXTERNAL_STAGES.{table_name}").collect()
        except:
            st.info("No data has been ingested yet!")
        else:
            users_df = pd.DataFrame(users)
            gd = GridOptionsBuilder.from_dataframe(users_df)
            gd.configure_default_column(editable=True)
            gd.configure_column("ID", editable=False)
            gd.configure_column("Created Date", editable=False)
            gd.configure_column("Modified Date", editable=False)
            gd.configure_column("SDM User", editable=False)
            gd.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=30)
            gd.configure_selection(selection_mode='multiple', use_checkbox=True)
            gd.configure_grid_options(pre_selected_rows=[])
            # gd.configure_column("INDEX", headerCheckboxSelection = True)
            gridoptions = gd.build()

            return_value = AgGrid(
                users_df, 
                gridOptions = gridoptions, 
                allow_unsafe_jscode = True,
                theme = 'balham',
                # height = 200,
                fit_columns_on_grid_load = False
            )
            selected_rows = return_value["selected_rows"]
            dis_update_btn = True
            if selected_rows:
                dis_update_btn = False
            if st.button("Update Data", key="updatedata", use_container_width=True, disabled=dis_update_btn):
                selected_data = session.create_dataframe(selected_rows)
                t = session.table(table_name)
                selected_data = selected_data.drop(selected_data.columns[0])
                
                df_col = selected_data.columns
                in_cond_str = up_cond_str = value_str = ""
                i = 0
                for col in df_col:
                    col = col.strip('"')
                    if i == 0:
                        in_cond_str += f"t['{col}'] == selected_data['{col}']"
                        value_str += f"'{col}': selected_data['{col}']"
                    else:
                        if i == 1:
                            up_cond_str += f"((t['{col}'] != selected_data['{col}'])"
                        else:
                            up_cond_str += f" | (t['{col}'] != selected_data['{col}'])"
                        value_str += f", '{col}': selected_data['{col}']"
                    i += 1
                if len(df_col) > 1:
                    up_cond_str += ")"
                curr_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                value_str += f", 'Modified Date': '{curr_date}'"
                value_str = "{" + value_str + "}"


                result = t.merge(selected_data, (eval(in_cond_str)), [when_matched(eval(up_cond_str)).update(eval(value_str))])
                st.info(result)
    
    with tab5:
        default_date = date(2024, 1, 1)
        col1, col2 = st.columns(2)
        chart_start_date=col1.date_input("Select Start Date",value=default_date)
        chart_end_date=col2.date_input("Select End Date")
        gk=session.sql(f'select CAST("Created Date" AS DATE) AS DATE,count(CAST("Created Date" AS DATE)) AS INSERTED, count(CAST("Modified Date" AS DATE)) AS UPDATED from {table_name}\
                         where CAST("Created Date" AS DATE) between \'{chart_start_date}\' AND \'{chart_end_date}\' group by CAST("Created Date" AS DATE)').collect()
        expander = st.expander("Ingestion History")
        expander.table(gk)
        if chart_start_date and chart_end_date:
            st.line_chart(gk, x="DATE", use_container_width=True)
    
    with tab6:
        table_name = st.session_state.get('table_name')
        st.write(table_name)
        try:
            users = session.sql(f"SELECT * FROM MANAGE_DB.EXTERNAL_STAGES.{table_name}").collect()
        except:
            st.info("No data has been ingested yet!")
        else:
            if not os.path.isfile(f"./soda_sip/{table_name}_checks.yml"):
                st.info("No data checks has been configure yet!")
            else:
                scan = Scan()
                scan.set_data_source_name("hackathon")
                scan.set_scan_definition_name(table_name)

                scan.add_configuration_yaml_file(file_path="./soda_sip/configuration.yml")

                scan.add_sodacl_yaml_file(f"./soda_sip/{table_name}_checks.yml")

                # Execute the scan
                exit_code = scan.execute()

                # Set logs to verbose mode, equivalent to CLI -V option
                scan.set_verbose(True)

                # Print results of scan
                scan_result = scan.get_scan_results()
                scan_result_df = pd.DataFrame(scan_result['checks'])
                scan_result_df = scan_result_df[['name', 'definition', 'dataSource', 'table', 'column', 'outcome']]
                scan_result_df.rename(columns=str.title, inplace=True)
                
                gd1 = GridOptionsBuilder.from_dataframe(scan_result_df)
                gd1.configure_default_column(editable=False, wrap_text=True)
                # gd1.configure_pagination(enabled=False, paginationAutoPageSize=False, paginationPageSize=30)
                gridoptions1 = gd1.build()

                AgGrid(
                    scan_result_df, 
                    gridOptions = gridoptions1,
                    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
                    wrapText=  True,
                    theme = 'alpine'
                )
    
    