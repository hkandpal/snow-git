import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import tempfile
import pandas as pd
import datetime



def main(session: snowpark.Session): 

    # Print a sample of the dataframe to standard output.
    file_name, file_url = send_full_reports(session)
    email_body = f'please find attached report <a href="{file_url}">{file_name}</a>'
    to_email = 'hk69@nyu.edu'
    email_subject = 'Snowflake Automated Report - {}'.format(datetime.datetime.utcnow().strftime('%Y-%m-%d'))
    
    session.sql("CALL SYSTEM$SEND_EMAIL('EMAIL_INT', '{}', '{}', '{}', '{}');".format(to_email,
                                                                                      email_subject,
                                                                                      email_body,
                                                                                      "text/html")).collect()
    return 'Email sent successfully.'

def send_full_reports(session):
    try:
        view_name = 'TEST.TEST_SCHEMA.EMP' #Provide Fully Qualified Name of the View or Table.
        df =  session.table(view_name).toPandas()
        stage_name = "@FILES_DB.CSV_FILES.PUBLIC_FILES"
        file_name = f'employee_details_' #Change the FileName Here.
        with tempfile.NamedTemporaryFile(mode="w+t", prefix=file_name, suffix=".csv", delete=False) as t:
            df.to_csv(t.name, index=None)
            session.file.put(t.name, stage_name,auto_compress=False)
            exported_file_name = t.name.split("/")[-1]
            file_sql = f"select GET_PRESIGNED_URL(@FILES_DB.CSV_FILES.PUBLIC_FILES, '{exported_file_name}',8600) as signed_url;"
            print(file_sql)
            signed_url = session.sql(file_sql).collect()[0]['SIGNED_URL']
            return exported_file_name, signed_url
    except Exception as e:
        print(str(e))
        