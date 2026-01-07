CREATE OR REPLACE API INTEGRATION api_integration_name
   API_PROVIDER = git_https_api
   API_ALLOWED_PREFIXES = ('https://github.com/')
   API_USER_AUTHENTICATION = (
      TYPE = snowflake_github_app
   )
   ENABLED = TRUE;


/*The Snowflake table has the ENABLE_SCHEMA_EVOLUTION parameter set to TRUE.
The COPY INTO <table> statement uses the MATCH_BY_COLUMN_NAME option.
The role used to load the data has the EVOLVE SCHEMA or OWNERSHIP privilege on the table.
*/


--per_num - This stores SSN without  a dash.
--Social_Security_Num - This has some rows which have SSN with dash
-- Nine_char - This stores SSN as a 9 character string without dash or space or dot.


-- 1 create the table 

USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS governance_db;
CREATE SCHEMA IF NOT EXISTS governance_db.sch;
CREATE WAREHOUSE IF NOT EXISTS tutorial_wh;
CREATE DATABASE IF NOT EXISTS data_db;
CREATE SCHEMA IF NOT EXISTS data_db.sch;

CREATE or replace TABLE data_db.sch.customers (
 account_number NUMBER(38,0),
 first_name VARCHAR(16777216),
 last_name VARCHAR(16777216),
 email VARCHAR(16777216),
 per_num VARCHAR(16777216),
 Social_Security_Num VARCHAR(16777216),
 nine_char VARCHAR(16777216)
);
INSERT INTO data_db.sch.customers (account_number, first_name, last_name, email, per_num,Social_Security_Num, nine_char)
 VALUES
 (1589420, 'john', 'doe', 'john.doe@example.com', '232-76-1119', '232-76-1119','434689191'),
     (2834123, 'jane', 'doe', 'jane.doe@example.com', '478-76-1119', NULL, '223788145'),
     (4829381, 'jim', 'doe', 'jim.doe@example.com', '981-76-1119', NULL, '678788235'),
     (9821802, 'susan', 'smith', 'susan.smith@example.com','123-73-3171', NULL,'789788235'),
     (8028387, 'bart', 'simpson', 'bart.barber@example.com','589-78-8239', NULL,'568788235'),
     (8028367, 'Berry', 'Jane', 'ninecontinous@example.com','819-78-8276', NULL,'348788255'),
     (4345381, 'Joseph', 'Billy', 'Joseph.doe@example.com', '245-56-4569', NULL, '678788235'),
     (9866802, 'Carol', 'Washington', 'Carol.smith@example.com','269-73-4571', NULL,'789788235'),
     (8045387, 'Jason', 'Jane', 'Jason.barber@example.com','459 78 3439', NULL,'456788235'),
     (8689067, 'James', 'simpson', 'James@example.com','456788235', NULL,'345788255');



select null as tst, * exclude(social_security_num) from data_db.sch.customers;

-- 2 create role

use role accountadmin;
use database data_db;
use schema sch;
create  or replace role analyst;
grant role analyst to user hkandpal;
grant usage on database data_db to analyst;
grant usage on schema sch to analyst;
grant select on table data_db.sch.customers to analyst;
grant usage on warehouse tutorial_wh to analyst;

use schema data_db.sch;
use warehouse tutorial_wh;

--If we select we will see the data is unmasked when using the role
--analyst as the policy has not been applied.
use role analyst;
select * from data_db.sch.customers;



use role accountadmin;

-- 3 call the Snowflake classifier
CALL SYSTEM$CLASSIFY('data_db.sch.customers',null);

-- 4 some columns are not classified
select SYSTEM$GET_CLASSIFICATION_RESULT('data_db.sch.customers');

-- 5  Create a custom classifier

use role accountadmin;

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER SSN_String();

-- sometimes need to execute this
--CALL SSN_String!DELETE_CATEGORY('NATIONAL_IDENTIFIER_REGEX');


CALL SSN_String!ADD_REGEX(
  semantic_category =>'NATIONAL_IDENTIFIER_REGEX',
  privacy_category => 'IDENTIFIER',
  value_regex =>   '^\\d{3}-?\\d{2}-?\\d{4}$' ,
  threshold => .50
  ); 
 

SELECT SSN_String!LIST();

/*
ALTER TAG  SSN UNSET MASKING POLICY ssn_mask;
drop tag data_db.sch.SSN;
drop MASKING POLICY ssn_mask();
*/


-- 5 create tags and masking policy

create or replace tag data_db.sch.SSN;  -- creating a tag

-- creating a masking policy to mask column of string data type, 
-- displaying actual values to accountadmin, but ********** to other roles

CREATE OR REPLACE MASKING POLICY ssn_mask AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
    ELSE '*********'
END; 


-- 6 assign the masking policy to the tag we just created
alter tag  data_db.sch.SSN set masking policy ssn_mask; 

/* syntax for 
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
   classification_profile_name(
      {
        'minimum_object_age_for_classification_days': 0,
        'maximum_classification_validity_days': 30,
        'auto_tag': true
        'classify_views': true
      });
*/


--  Remove custom classifiers from the SSN_classification_profile profile:
CALL SSN_classification_profile_new!UNSET_CUSTOM_CLASSIFIERS();
sHOW SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE;
DROP SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE SSN_classification_profile_new;

--- classification profile with custom classifier
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE SSN_classification_profile_new(
  {
    'minimum_object_age_for_classification_days':0,
    'maximum_classification_validity_days': 1,   -- added 1 for testing make it 30
    'auto_tag':true,
    'custom_classifiers': {
      'SSN_nos': SSN_String!LIST()
    },
    'tag_map': {
     'column_tag_map': 
     [
      {
      'tag_name':'data_db.sch.SSN',
      'tag_value':'Highly Confidential',
      'semantic_categories':['NATIONAL_IDENTIFIER_REGEX']
       }
      ]
      }
  }
);


 


-- 7 Run with a custom classifier
CALL SYSTEM$CLASSIFY('data_db.sch.customers', 'SSN_classification_profile_new');

select SYSTEM$GET_CLASSIFICATION_RESULT('data_db.sch.customers');

-- 8 If we select we will see the data is masked when using the role
--analyst as the policy has  been applied.
use role analyst;
select * from data_db.sch.customers;


-- 9 classify the schema and check  after 30 minutes as depending upon the table size it may take some time for the masking policy to be applied.

ALTER schema data_db.sch set CLASSIFICATION_PROFILE = 'data_db.sch.SSN_classification_profile_new';
-- for the database
ALTER DATABASE data_db   SET CLASSIFICATION_PROFILE = 'data_db.sch.SSN_classification_profile_new';;

-- 9.5
use role accountadmin;
CALL  SSN_classification_profile_new!UNSET_CUSTOM_CLASSIFIERS();
SELECT SYSTEM$SHOW_SENSITIVE_DATA_MONITORED_ENTITIES();
--
use role accountadmin;
select * from data_db.sch.customers;


--10 
-- classifier without custom classifier
-- this will hide all the Identifier , instead of only SSN

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE default_classification_profile(
  {
    'minimum_object_age_for_classification_days':0,
    'maximum_classification_validity_days': 1,
    'auto_tag':true,
    
    'tag_map': {
     'column_tag_map': 
     [
      {
      'tag_name':'data_db.sch.SSN',
      'tag_value':'Highly Confidential'
      --,   'semantic_categories':['NATIONAL_IDENTIFIER_REGEX']
       }
      ]
      }
  }
);

CALL SYSTEM$CLASSIFY('data_db.sch.customers', 'default_classification_profile');
use role analyst;
select * from data_db.sch.customers;


/*
Excluding objects from automatic sensitive data classification
By default, Snowflake automatically classifies all sensitive data in a database that has a classification profile set on it. 
You can configure Snowflake to exclude schemas, tables, or columns from automatic classification so that they are skipped during the classification process.
Apply the SNOWFLAKE.CORE.SKIP_SENSITIVE_DATA_CLASSIFICATION tag to every object that you want excluded from automatic sensitive data classification.
ALTER SCHEMA schema-name SET TAG SNOWFLAKE.CORE.SKIP_SENSITIVE_DATA_CLASSIFICATION = 'TRUE';
ALTER TABLE my_table SET TAG SNOWFLAKE.CORE.SKIP_SENSITIVE_DATA_CLASSIFICATION = 'TRUE';
ALTER TABLE my_table ALTER COLUMN employee_id   SET TAG SNOWFLAKE.CORE.SKIP_SENSITIVE_DATA_CLASSIFICATION = 'TRUE';
*/

-- grants to a role if not using Accountadmin
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE analyst;
GRANT CREATE DATA_PRIVACY CUSTOM_CLASSIFIER ON ACCOUNT TO ROLE analyst;
  
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE analyst;
GRANT  IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE analyst;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER ON SCHEMA sch TO ROLE analyst;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE on schema sch TO ROLE analyst;

-- Grants for create tag
grant  CREATE TAG on schema data_db.sch to role analyst;
-- grants for create masking policy
GRANT CREATE MASKING POLICY ON SCHEMA data_db.sch TO ROLE  analyst;
grant apply tag on account to role analyst;
grant apply masking policy on account to role analyst;


-- Allow role to create classification profiles in the schema
-- at minimum
GRANT USAGE ON DATABASE data_db TO ROLE analyst;
GRANT USAGE ON SCHEMA data_db.sch TO ROLE analyst;

-- end grants

-- us zip code
-- like  10538-2026 , 91604-3921, 91604-3921

-- test this on Monday 10/16 for the new table about classification profile
select SYSTEM$GET_CLASSIFICATION_RESULT('data_db.sch.customers');
-- new table created on 10/9/25
create or replace table data_db.sch.customers_new as select * from data_db.sch.customers;

ALTER schema data_db.sch  SET CLASSIFICATION_PROFILE = 'data_db.sch.SSN_classification_profile';
--on thursday it was null
select SYSTEM$GET_CLASSIFICATION_RESULT('data_db.sch.customers_new');
-- ran the on friday and  the value was hidden for role analyst
select * from data_db.sch.customers_new;
-- end Monday test