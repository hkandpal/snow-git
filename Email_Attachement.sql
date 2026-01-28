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