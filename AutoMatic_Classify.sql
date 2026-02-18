
--test
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS RAM_governance_db;
CREATE SCHEMA IF NOT EXISTS RAM_governance_db.sch;
CREATE WAREHOUSE IF NOT EXISTS tutorial_wh;
CREATE DATABASE IF NOT EXISTS RAM_data_db;
CREATE SCHEMA IF NOT EXISTS RAM_data_db.sch;
CREATE or replace TABLE RAM_data_db.sch.customers (
 account_number NUMBER(38,0),
 first_name VARCHAR(16777216),
 last_name VARCHAR(16777216),
 email VARCHAR(16777216),
 per_num VARCHAR(16777216),
 Social_Security_Num VARCHAR(16777216),
 nine_char VARCHAR(16777216),
 BIRTHDATE_date date,
 BIRTHDATE_varchar varchar(20),
 BIRTHDATE_time_stamp timestamp,
 BIRTHDATE  TIMESTAMP_NTZ(0),
 entered_date date,
 date_of_birth  TIMESTAMP_NTZ
);
INSERT INTO RAM_data_db.sch.customers (account_number, first_name, last_name, email, per_num,Social_Security_Num, nine_char,
 BIRTHDATE_date , BIRTHDATE_varchar,  BIRTHDATE_time_stamp, BIRTHDATE, entered_date ,date_of_birth   )
 VALUES
 (1589420, 'john', 'doe', 'john.doe@example.com', '232-76-1119', '232-76-1119','434689191', to_date('1988-02-23','YYYY-MM-DD'), '1988-02-23',TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date(), '1988-07-25 00:00:00.000')  ,
     (2834123, 'jane', 'doe', 'jane.doe@example.com', '478-76-1119', NULL, '223788145', to_date('1998-02-23','YYYY-MM-DD'), '1998-02-23',TO_TIMESTAMP('1998-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1998-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3') , current_date(),'2000-07-25 00:00:00.000'),
     (4829381, 'jim', 'doe', 'jim.doe@example.com', '981-76-1119', NULL, '678788235', to_date('1958-02-23','YYYY-MM-DD'), '1958-02-23',TO_TIMESTAMP('1958-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1958-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date(), '2001-07-25 00:00:00.000'),
     (9821802, 'susan', 'smith', 'susan.smith@example.com','123-73-3171', NULL,'789788235',null,null,null, null, current_date() , null),
     (8028387, 'bart', 'simpson', 'bart.barber@example.com','589-78-8239', NULL,'568788235', to_date('1988-02-23','YYYY-MM-DD'), '1988-02-23',TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date() , '2003-07-25 00:00:00.000'),
     (8028367, 'Berry', 'Jane', 'ninecontinous@example.com','819-78-8276', NULL,'348788255',null,null,null, null, current_date(), null),
(4345381, 'Joseph', 'Billy', 'Joseph.doe@example.com', '245-56-4569', NULL, '678788235', to_date('2000-01-31','YYYY-MM-DD') , '2000-01-31',TO_TIMESTAMP('2000-01-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('2000-01-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date() , '2003-09-29 00:00:00.000'),
     (9866802, 'Carol', 'Washington', 'Carol.smith@example.com','269-73-4571', NULL,'789788235', to_date('2000-01-31','YYYY-MM-DD'), '2000-01-31',TO_TIMESTAMP('2000-01-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3') ,TO_TIMESTAMP('2000-01-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date(), '2001-09-29 00:00:00.000'),
     (8045387, 'Jason', 'Jane', 'Jason.barber@example.com','459 78 3439', NULL,'456788235', to_date('1988-07-31','YYYY-MM-DD'), '1988-07-31',TO_TIMESTAMP('1988-07-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1988-07-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date(), '2003-05-29 00:00:00.000'),
     (8689067, 'James', 'simpson', 'James@example.com','456788235', NULL,'345788255', to_date('1999-05-31','YYYY-MM-DD'), '1999-05-31',TO_TIMESTAMP('1999-05-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1999-05-31 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date() , '2001-09-30 00:00:00.000');

select * FROM RAM_data_db.sch.customers;
SELECT SYSTEM$CLUSTERING_INFORMATION('customers', 'ACCOUNT_NUMBER');
    SELECT PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('customers')):"total_partition_count";

CALL SYSTEM$CLASSIFY('RAM_data_db.sch.customers',null);
SELECT SYSTEM$GET_CLASSIFICATION_RESULT('RAM_data_db.sch.customers');
sHOW PARAMETERS LIKE '%timestamp%';


select * from RAM_data_db.sch.customers where per_num  REGEXP('^\\d{3}[-\\s]?\\d{2}[-\\s]?\\d{4}$');
select * from RAM_data_db.sch.customers where nine_char  REGEXP('^\\d{3}[-\\s]?\\d{2}[-\\s]?\\d{4}$');


use role accountadmin;

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER SSN_String();
 
CALL SSN_String!ADD_REGEX(
  semantic_category =>'NATIONAL_IDENTIFIER_REGEX',
  privacy_category => 'IDENTIFIER',
  value_regex =>   '^\\d{3}-?\\d{2}-?\\d{4}$' ,
  threshold => .50
  ); 
 

SELECT SSN_String!LIST();

create or replace tag RAM_data_db.sch.SSN;  -- creating a tag

CREATE OR REPLACE MASKING POLICY ssn_mask AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
    ELSE '*********'
END; -- creating a masking policy to mask column of string data type, displaying actual values to accountadmin, but ********** to other roles

alter tag ssn set masking policy ssn_mask; -- assign the masking policy to the tag we just created


CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE SSN_classification_profile(
  {
    'minimum_object_age_for_classification_days':0,
    'auto_tag':true,
    'custom_classifiers': {
      'SSN_nos': SSN_String!LIST()
    },
    'tag_map': {
     'column_tag_map': 
     [
      {
      'tag_name':'RAM_data_db.sch.SSN',
      'tag_value':'Highly Confidential',
      'semantic_categories':['NATIONAL_IDENTIFIER_REGEX']
       }
      ]
      }
  }
);

--describe the SSN Classification Profile

SELECT SSN_classification_profile!DESCRIBE();



CALL SYSTEM$CLASSIFY('RAM_data_db.sch.customers', 'SSN_classification_profile');

--CALL SYSTEM$CLASSIFY('RAM_GOVERNANCE_DB.SCH.BATCH_DATE_CARD_BCR','SSN_classification_profile') ;
--call  ram_governance_db.sch.RUN_CLASSIFIER_FOR_ALL_DB();
--CALL SYSTEM$CLASSIFY('RAM_GOVERNANCE_DB.SCH.BATCH_DATE_CARD_BCR','SSN_classification_profile') 
select current_role();
-- if you want at Schema level then use the following
--ALTER schema RAM_data_db.sch set CLASSIFICATION_PROFILE = 'RAM_data_db.sch.SSN_classification_profile';;
ALTER schema RAM_data_db.sch unset CLASSIFICATION_PROFILE 
--SELECT SYSTEM$GET_CLASSIFICATION_RESULT('RAM_data_db.sch.customers');

-- 1)  Procedure to   run CLASSIFICATION_PROFILE at a account level, this will scan all the DB's and the Schemas.
-- 2) Priority run 1 Go thorugh all the DB,Schema, and tables and run SYSTEM$CLASSIFY,  Store into a  table with DB name, schema name, table name and Jsaon. ( Batch number, run_date)
2.5) Create a logging table which will have batch_id, DB_name, Schema_name, table_name, classif_start_time, classify_end_time,
-- 3  procedrue or View   to Parse Jason, get the list of tables, column, privacy_category, "semantic_category, confidence,tag_name,tag_value, classifier_name, date_ran,
select database_name , database_owner from information_schema.databases where database_name not in ('SNOWFLAKE_SAMPLE_DATA') order by database_name;
select catalog_name as database, schema_name from information_schema.schemata where schema_name not in ('INFORMATION_SCHEMA', 'PUBLIC');
SELECT * FROM information_schema.tables where table_type = 'BASE TABLE';
select catalog_name as database, schema_name from information_schema.schemata where schema_name not in ('INFORMATION_SCHEMA', 'PUBLIC')
and catalog_name;

-- create a tst table with No SSN

create  table RAM_data_db.sch.customers_NO_SSN as select account_number, first_name, last_name, email from RAM_data_db.sch.customers;
alter table RAM_data_db.sch.customers_NO_SSN add (per_num VARCHAR(16777216), Social_Security_Num VARCHAR(16777216), nine_char VARCHAR(16777216));
CALL SYSTEM$CLASSIFY('RAM_data_db.sch.customers_NO_SSN', 'SSN_classification_profile');
select * from RAM_data_db.sch.customers_NO_SSN;

SELECT SYSTEM$GET_CLASSIFICATION_RESULT('RAM_data_db.sch.customers_NEW');
SELECT SYSTEM$GET_CLASSIFICATION_RESULT('RAM_data_db.sch.customers');
SELECT SYSTEM$GET_CLASSIFICATION_RESULT('RAM_data_db.sch.customers_NO_SSN');



 ---  HK 9/15/25   After this not required
 
-- check for the tables that are matching the NATIONAL_IDENTIFIER_REGEX whci is the custom semantic_category defined
select TABLE_NAME, CLASSIFICATION_RESULT,
PARSE_JSON(CLASSIFICATION_RESULT) AS CR_VARIANT,
PARSE_JSON(JSON_EXTRACT_PATH_TEXT(CR_VARIANT, 'classification_result' )) as data_chk ,
chk_col.value:EMAIL,  chk_col.value, chk_col.key, chk_col.value
FROM RAM_data_db.sch.CLASSIFICATION_RESULTS,
LATERAL FLATTEN(input => CLASSIFICATION_RESULT) chk_col
WHERE chk_col.value like '%NATIONAL_IDENTIFIER_REGEX%' ;

--Hk added on 7/2/2025   RECURSIVE=>False - No rows

select res.table_name, f.key as column_name,f.value --,  f.*
from RAM_data_db.sch.CLASSIFICATION_RESULTS res,
LATERAL FLATTEN(CLASSIFICATION_RESULT, RECURSIVE=>True) F
WHERE f.key not in ('classification_profile_config','classification_profile_name','classification_result','alternates','recommendation'
,'confidence','semantic_category','tags','tag_applied','tag_value','valid_value_ratio','tag_name','details','privacy_category',
'classifier_name','coverage')
and f.value like '%NATIONAL_IDENTIFIER_REGEX%';

select * from RAM_data_db.sch.CLASSIFICATION_RESULTS 
SELECT * FROM TABLE(information_schema.tag_references_all_columns('RAM_data_db.sch.customers','table'));

---HK added on 7/1/25
select TABLE_NAME, CLASSIFICATION_RESULT,
PARSE_JSON(CLASSIFICATION_RESULT) AS CR_VARIANT,
PARSE_JSON(JSON_EXTRACT_PATH_TEXT(CR_VARIANT, 'classification_result' )) as data_chk,  
 f.index,
  f.value AS "Current Level Value",
  f.this AS "Above Level Value"
FROM RAM_data_db.sch.CLASSIFICATION_RESULTS t,
LATERAL FLATTEN(t.CLASSIFICATION_RESULT, recursive=>true) f
WHERE CLASSIFICATION_RESULT like '%NATIONAL_IDENTIFIER_REGEX%' ;



