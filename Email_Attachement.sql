call SEND_EMAIL();

CREATE OR REPLACE PROCEDURE SEND_EMAIL()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10' -- Specify a supported Python version
PACKAGES = ('snowflake-snowpark-python', 'pandas') -- Required package for Snowpark access
HANDLER = 'SEND_EMAIL'
AS $$
from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
import pandas as pd
import datetime

 
def SEND_EMAIL(session):
    # Get the current active session automatically
    
    
    # 1. Read the specific columns from the view
    view_name = 'nyu_ref_data_prod.nyu_ref_data_ssd.DATA_SUMMARY_RPT'
    
    # We use .limit(200) to ensure the email doesn't get too big and fail silently
    df = session.table(view_name).select(
        "DB_NAME", 
        "SCHEMA_NAME", 
        "COLUMN_NAME", 
        "distinct_matching_cnt", 
        "total_cnt", 
        "matching_patern_cnt"
    ).limit(200).to_pandas()
    
    # 2. Convert Dataframe to HTML Table string
    html_table = df.to_html(index=False, border=1, justify='center', classes='report-table')
    
    # 3. Create the HTML email body
    email_body = f"""
    <html>
        <head>
            <style>
                .report-table {{
                    border-collapse: collapse;
                    width: 100%;
                    font-family: Arial, sans-serif;
                    font-size: 12px;
                }}
                .report-table th {{
                    background-color: #29b5e8;
                    color: white;
                    padding: 8px;
                }}
                .report-table td {{
                    padding: 8px;
                    text-align: left;
                    border: 1px solid #ddd;
                }}
                .report-table tr:nth-child(even) {{background-color: #f2f2f2;}}
            </style>
        </head>
        <body>
            <p>Hello,</p>
          #  <p>Please find the requested <b>Data Summary Report</b> for {datetime.date.today()}:</p>
            {html_table}
            <p>Regards,<br>Snowflake Automated Reporter</p>
        </body>
    </html>
    """
    
    to_email = 'hk69@nyu.edu'
    email_subject = 'Snowflake Automated Report - {}'.format(datetime.datetime.utcnow().strftime('%Y-%m-%d'))
    
    # 4. Send the email using bind variables (?) for maximum reliability
    # This avoids the error if your HTML body is very large
    try:
        session.sql("CALL SYSTEM$SEND_EMAIL(?, ?, ?, ?, ?)", 
                    params=['EMAIL_INT', to_email, email_subject, email_body, "text/html"]).collect()
        return("Email Sent Successfully!")
    except Exception as e:
        return(f"Failed to send email: {e}")
$$;


CREATE or replace NOTIFICATION INTEGRATION EMAIL_INT
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ( 'hk69@nyu.edu' )
  DEFAULT_SUBJECT = 'Email Notification';

CREATE or replace DATABASE FILES_DB;
USE DATABASE FILES_DB;
CREATE SCHEMA CSV_FILES;
CREATE or replace STAGE PUBLIC_FILES ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
--stage_name = "@FILES_DB.CSV_FILES.PUBLIC_FILES"


CREATE DATABASE TEST;
create schema TEST_SCHEMA;

create or replace view TEST.TEST_SCHEMA.EMP as select * from data_db.sch.customers; 
select * from TEST.TEST_SCHEMA.emp;

select $1 , $2 from @FILES_DB.CSV_FILES.PUBLIC_FILES;
select GET_PRESIGNED_URL(@FILES_DB.CSV_FILES.PUBLIC_FILES,'employee_details_geit0k_f.csv');
select GET_PRESIGNED_URL(@FILES_DB.CSV_FILES.PUBLIC_FILES, 'employee_details_geit0k_f.csv',8600) as signed_url;
select GET_PRESIGNED_URL(@FILES_DB.CSV_FILES.PUBLIC_FILES, 'employee_details_lug4g0j0.csv',8600) as signed_url;
SELECT GET_STAGE_LOCATION(@FILES_DB.CSV_FILES.PUBLIC_FILES);
 
select GET_PRESIGNED_URL(@FILES_DB.CSV_FILES.PUBLIC_FILES, 'employee_details_zqcgpv29.csv',8600) as signed_url;
employee_details_geit0k_f.csv