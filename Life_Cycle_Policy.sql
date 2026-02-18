--test
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS Life_cycle_DB;
CREATE SCHEMA IF NOT EXISTS LC_SCHEMA;
 

CREATE or replace TABLE Life_cycle_DB.LC_SCHEMA.customers (
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
INSERT INTO Life_cycle_DB.LC_SCHEMA.customers (account_number, first_name, last_name, email, per_num,Social_Security_Num, nine_char,
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


-- old rows to be entered to archive the data 60 days old
INSERT INTO Life_cycle_DB.LC_SCHEMA.customers (account_number, first_name, last_name, email, per_num,Social_Security_Num, nine_char,
 BIRTHDATE_date , BIRTHDATE_varchar,  BIRTHDATE_time_stamp, BIRTHDATE, entered_date ,date_of_birth   )
 VALUES
 (2345679, 'Nick', 'doe', 'john.doe@example.com', '232-76-1119', '232-76-1119','434689191', to_date('1988-02-23','YYYY-MM-DD'), '1988-02-23',TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date() - 90, '1988-07-25 00:00:00.000') ,
 (4445458, 'Nickeloden', 'Roth', 'john.doe@example.com', '232-76-1119', '232-76-1119','434689191', to_date('1988-02-23','YYYY-MM-DD'), '1988-02-23',TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'),TO_TIMESTAMP('1988-02-23 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3'), current_date() - 85, '1988-07-25 00:00:00.000') ;
 

select * FROM  Life_cycle_DB.LC_SCHEMA.customers;

Create or replace table Life_cycle_DB.LC_SCHEMA.account_delete (account_number NUMBER(38,0));
insert into Life_cycle_DB.LC_SCHEMA.account_delete (account_number) values (1589420);

-- Expiration
--ARCHIVE_TIER = { COOL | COLD }
--If you don’t specify this parameter, the policy is an expiration policy that deletes rows without archiving them.

Create or replace storage lifecycle policy expire_account
AS (i_account_number NUMBER) returns BOOLEAN ->
EXISTS (SELECT 1 from account_delete where account_number = i_account_number );

ALTER table   Life_cycle_DB.LC_SCHEMA.customers add storage lifecycle policy  expire_account on (account_number);

select * FROM  Life_cycle_DB.LC_SCHEMA.customers where account_number = 1589420;

CREATE TABLE Life_cycle_DB.LC_SCHEMA.customers_archive FROM ARCHIVE OF Life_cycle_DB.LC_SCHEMA.customers
where account_number in (SELECT account_number from account_delete);

--- Create a new table to test COOL tier
create or replace table Life_cycle_DB.LC_SCHEMA.customer_detail as select * from Life_cycle_DB.LC_SCHEMA.customers;

 select * from Life_cycle_DB.LC_SCHEMA.customer_detail where entered_date < current_date - 60;
 
 CREATE TABLE Life_cycle_DB.LC_SCHEMA.customer_detail_archive FROM ARCHIVE OF Life_cycle_DB.LC_SCHEMA.customer_detail
 WHERE   entered_date < current_date - 60;
 select * from Life_cycle_DB.LC_SCHEMA.customer_detail_archive;

 -- Create the policy (The "Quick-Tidying Helper")
CREATE OR REPLACE STORAGE LIFECYCLE POLICY Life_cycle_DB.LC_SCHEMA.archive_after_60_days
  AS (entered_date DATE)
  RETURNS BOOLEAN  ->
    entered_date < DATEADD('day', -60, CURRENT_DATE())  
    ARCHIVE_TIER = COOL
    ARCHIVE_FOR_DAYS = 180; 

 -- Assign the 60-day organizer to the customers table
ALTER TABLE Life_cycle_DB.LC_SCHEMA.customer_detail 
  add STORAGE LIFECYCLE POLICY Life_cycle_DB.LC_SCHEMA.archive_after_60_days 
  ON (entered_date);

 
SELECT p.policy_name, p.* FROM TABLE(information_schema.policy_references( policy_name => 'Life_cycle_DB.LC_SCHEMA.expire_account'))p  UNION
SELECT p.policy_name, p.* FROM TABLE(information_schema.policy_references( policy_name => 'Life_cycle_DB.LC_SCHEMA.archive_after_60_days')) p;

SELECT * FROM   TABLE (INFORMATION_SCHEMA.STORAGE_LIFECYCLE_POLICY_HISTORY(REF_ENTITY_NAME => 'Life_cycle_DB.LC_SCHEMA.customers', REF_ENTITY_DOMAIN => 'Table'));
SELECT * FROM   TABLE (INFORMATION_SCHEMA.STORAGE_LIFECYCLE_POLICY_HISTORY(REF_ENTITY_NAME => 'Life_cycle_DB.LC_SCHEMA.customer_detail', REF_ENTITY_DOMAIN => 'Table'));

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY;

SHOW STORAGE LIFECYCLE POLICIES  ;
SELECT * FROM TABLE(Life_cycle_DB.INFORMATION_SCHEMA.POLICY_REFERENCES(
    REF_ENTITY_NAME => 'Life_cycle_DB.LC_SCHEMA.customer_detail', 
    REF_ENTITY_DOMAIN => 'TABLE'
));


