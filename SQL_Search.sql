  select * from data_db.sch.customers;

  select * from  data_db.sch.customers where  (SEARCH((*), '232-76-1119'));
  select * from  ram_data_db.sch.CUSTOMERS_TEST where  (SEARCH((*), '232-76-1119'));
  
call SEARCH_DATA_IN_ALL_DB ('232-76-1119');
select * from BATCH_SEARCH_DATA;
select * from SEARCH_DATA_TABLE_COUNT;

select *  from CLUSTERING_EXP.TPCDS_CLUSTERING_TEST.CATALOG_SALES WHERE  (SEARCH((*), '232-76-1119')); 

--- testing end


create or replace TABLE BATCH_SEARCH_DATA (
	BATCH_ID NUMBER(38,0) NOT NULL autoincrement start 1001 increment 1 order,
	START_DATE TIMESTAMP_NTZ(9) NOT NULL,
	END_DATE TIMESTAMP_NTZ(9)
);

CREATE or replace  TABLE  SEARCH_DATA_TABLE_COUNT (
        BATCH_ID NUMBER,
        DB_NAME VARCHAR, 
        SCHEMA_NAME VARCHAR, 
        TABLE_NAME VARCHAR, 
        ROW_COUNT NUMBER, 
        SQL_USED   VARCHAR
    );
    
CREATE or replace  TABLE  SEARCH_DATA_COLUMN_COUNT (
        BATCH_ID NUMBER,
        BATCH_RUN_ID  NUMBER(38,0) ,
        DB_NAME VARCHAR, 
        SCHEMA_NAME VARCHAR, 
        TABLE_NAME VARCHAR, 
        COLUMN_NAME VARCHAR, 
        ROW_COUNT NUMBER, 
        SQL_USED   VARCHAR
    );
 
 

 
-- call SEARCH_DATA_IN_ALL_DB  '232-76-1119'
CREATE OR REPLACE PROCEDURE SEARCH_DATA_IN_ALL_DB(strSearch_Value varchar)
RETURNS VARIANT
LANGUAGE SQL
--EXECUTE AS OWNER
EXECUTE AS caller
AS 

--Declare all variables
DECLARE
    v_batch_id integer;
    res_1 RESULTSET;
    res_2 RESULTSET;
    res_3 RESULTSET;
    v_row_count     integer;
    v_job_name      VARCHAR;
    v_current_timestamp VARCHAR;
    sql_stmt VARCHAR;
    sql_stmt2 VARCHAR;
    sql_stmt3 VARCHAR;
    vjob_proc VARCHAR;
    vCNT INTEGER;
    vTable_Name VARCHAR;
    v_err_stmt      varchar;   
    v_sqlerrm       varchar;
    vDB_NAME varchar;
    vSchema_NAME varchar;
    vPROC_SQL VARCHAR;
    vjob_proc2 VARCHAR;
    v_CLASSIFICATION_RESULT variant;


BEGIN

    v_job_name  := 'SEARCH_DATA_IN_ALL_DB';
    select to_char(current_timestamp(),'YYYY-MM-DD HH24:MI:SS') INTO :v_current_timestamp;
    
    v_err_stmt := 'START PROC';

    INSERT INTO BATCH_SEARCH_DATA (  START_DATE ) VALUES (  current_timestamp() );

    v_err_stmt := 'Before the UDW_DATA_CHECK_GET_CURRENT_RUNNING_BATCH_ID';
    SELECT NVL(MAX(BATCH_ID),0) INTO :v_batch_id  FROM  BATCH_SEARCH_DATA;
      
    sql_stmt := 'select database_name from information_schema.databases where database_name not in (''SNOWFLAKE_SAMPLE_DATA'',''SNOWFLAKE'')
    order by database_name';
    --' and database_name in (select DB_NAME from DB_CHK where PII_CHECK = ''1'') 
    v_err_stmt := sql_stmt;
    res_1 := (EXECUTE IMMEDIATE :sql_stmt);
    LET cur1 CURSOR FOR res_1;
    v_err_stmt := 'Start Main Loop';
    FOR row_variable IN cur1 DO
        vDB_NAME := row_variable.database_name;
        sql_stmt2 := 'select catalog_name as database, schema_name from ' || :vDB_NAME || '.information_schema.schemata where  schema_name 
        not in (''INFORMATION_SCHEMA'', ''PUBLIC'') and catalog_name = ' || '''' || :vDB_NAME ||  '''' --;
        || ' and schema_name not in (''EXCLUDE'') ' ;
        v_err_stmt := sql_stmt2;
        res_2 := (EXECUTE IMMEDIATE :sql_stmt2);
        LET cur2 CURSOR FOR res_2;
                               INSERT INTO SEARCH_DATA_TABLE_COUNT ( BATCH_ID ,DB_NAME , SCHEMA_NAME , TABLE_NAME , ROW_COUNT , SQL_USED )
                     values(:v_batch_id, :vDB_NAME, :vSchema_NAME,:vTable_Name,:v_row_count, :v_err_stmt  );
        v_err_stmt := 'Start Inner Loop';
        FOR row_variable2 IN cur2 DO
            v_err_stmt := 'Start 1';
            vSchema_NAME := row_variable2.schema_name;
            v_err_stmt := 'Start 2';
            sql_stmt3 := 'SELECT table_name FROM ' ||  :vDB_NAME || '.information_schema.tables where table_type = ''BASE TABLE''' ||
                ' and table_catalog = ' || '''' || :vDB_NAME ||  ''''   || ' and table_schema = ' || '''' || :vSchema_NAME ||  ''''  ;
            v_err_stmt := sql_stmt3;
            res_3 := (EXECUTE IMMEDIATE :sql_stmt3); 
            LET cur3 CURSOR FOR res_3;
            v_err_stmt := 'Inside the table loop';
            FOR row_variable3 IN cur3 DO
                vTable_Name := row_variable3.table_name;
                v_err_stmt := 'In Loop for table ' ||  :vTable_Name;
                    --  select count(*) from  data_db.sch.customers where  (SEARCH((*), '232-76-1119'));
                    --vPROC_SQL := 'select count(*) into :v_row_count 
                    --from  ' || :vDB_NAME || '.' || :vSchema_NAME || '.' || :vTable_Name || ' WHERE ' || ' (SEARCH((*), '  || ''' || strSearch_Value  --|| ''';
                 vPROC_SQL := 'select count(*) into :v_row_count from ' || :vDB_NAME || '.' || :vSchema_NAME || '.' || :vTable_Name || ' WHERE  (SEARCH((*), ' || '''' || strSearch_Value || '''))';
  v_err_stmt := vPROC_SQL;
                    EXECUTE IMMEDIATE vPROC_SQL ;
                   -- if ( :v_row_count > 0 ) then
                     INSERT INTO SEARCH_DATA_TABLE_COUNT ( BATCH_ID ,DB_NAME , SCHEMA_NAME , TABLE_NAME , ROW_COUNT , SQL_USED )
                     values(:v_batch_id, :vDB_NAME, :vSchema_NAME,:vTable_Name,:v_row_count, :vPROC_SQL  );
                    --end if;
                    
                    
                    select to_char(current_timestamp(),'YYYY-MM-DD HH24:MI:SS') INTO :v_current_timestamp;
                    --if the count is > 0 then capture the db, schame, and table name and  check for the column.
                  END FOR; -- Table Loop
             END FOR; -- Schema Loop     
    END FOR; --DB Loop

  UPDATE  BATCH_SEARCH_DATA SET END_DATE = CURRENT_TIMESTAMP() WHERE BATCH_ID = :v_batch_id;    
  
RETURN 'SUCCESS';

    exception
        when other then
        return object_construct('Error type', 'STATEMENT_ERROR',
                                'Error_string', v_err_stmt,
                                'SQLCODE', sqlcode,
                                'SQLERRM', sqlerrm,
                                'SQLSTATE', sqlstate
                                ); 

END;   
