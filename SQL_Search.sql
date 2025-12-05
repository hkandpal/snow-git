  select * from data_db.sch.customers;
  select count(*) from  DATA_DB.SCH.CUSTOMERS WHERE (SEARCH((*), '232-76-1119'));
  select count(*) from  data_db.sch.customers where  (SEARCH((*), '232-76-1119'));
  select * from  ram_data_db.sch.CUSTOMERS_TEST where  (SEARCH((*), '232-76-1119'));
  
call SEARCH_DATA_IN_ALL_DB ('232-76-1119');
select * from BATCH_SEARCH_DATA;
select *  from SEARCH_DATA_TABLE_COUNT;
select *  from  SEARCH_DATA_ERROR_LOG;
delete from  SEARCH_DATA_ERROR_LOG;


select *  from CLUSTERING_EXP.TPCDS_CLUSTERING_TEST.CATALOG_SALES WHERE  (SEARCH((CS_PROMO_SK), '232-76-1119')); 
select count(*)   from RAM_DATA_DB.SCH.CUSTOMERS_NO_SSN WHERE  (SEARCH((*), '232-76-1119'))

--- testing end

CREATE OR REPLACE SEQUENCE seq_BATCH_RUN_ID START = 1001 INCREMENT = 1 ORDER ;    

create table SEARCH_DATA_ERROR_LOG (
BATCH_ID NUMBER, DB_NAME VARCHAR(200), 
SCHEMA_NAME VARCHAR(200), TABLE_NAME VARCHAR(200), 
ERROR_SQL VARCHAR, ERROR_MSG VARCHAR);

create or replace TABLE BATCH_SEARCH_DATA (
	BATCH_ID NUMBER(38,0) NOT NULL autoincrement start 1001 increment 1 order,
	START_DATE TIMESTAMP_NTZ(9) NOT NULL,
	END_DATE TIMESTAMP_NTZ(9)
);

CREATE or replace  TABLE  SEARCH_DATA_TABLE_COUNT (
        BATCH_ID NUMBER,
        DB_NAME VARCHAR(200), 
        SCHEMA_NAME VARCHAR(200), 
        TABLE_NAME VARCHAR(200), 
        ROW_COUNT number, 
        STR_SEARCHED VARCHAR(200),
        SQL_USED   VARCHAR 
    );
    
CREATE or replace  TABLE  SEARCH_DATA_COLUMN_COUNT (
        BATCH_ID NUMBER,
        BATCH_RUN_ID  NUMBER(38,0) ,
        DB_NAME VARCHAR(200), 
        SCHEMA_NAME VARCHAR(200), 
        TABLE_NAME VARCHAR(200), 
        COLUMN_NAME VARCHAR(200), 
        ROW_COUNT NUMBER, 
        SQL_USED   VARCHAR
    );
 
 
CREATE OR REPLACE PROCEDURE CAPTURE_STR_COUNT(p_batch_id number)
RETURNS VARIANT
LANGUAGE SQL
--EXECUTE AS OWNER
EXECUTE AS caller  
					   
AS DECLARE
    sql_stmt VARCHAR;
    v_job_name VARCHAR;
    v_current_timestamp VARCHAR;
    v_batch_running VARCHAR;
    v_batch_id NUMBER ; 
    vTable_Name VARCHAR;
    res1 RESULTSET;
    v_err_stmt VARCHAR;
    v_sqlerrm VARCHAR;
    vDB_NAME VARCHAR;
    vSchema_NAME VARCHAR;
    vColumn_Name VARCHAR; -- Used for the column name from the view
    vPROC_SQL VARCHAR; -- Holds the final dynamic SQL to execute
    vjob_proc2 VARCHAR;
    v_CLASSIFICATION_RESULT VARIANT;
    v_full_table_name VARCHAR; -- Fully qualified table name
    v_pattern_check string DEFAULT 'SSN_PATTERN';
	
    v_seq_BATCH_RUN_ID  NUMBER;
    		   																																		   
BEGIN

    v_job_name := 'CAPTURE_STR_COUNT';
    v_batch_id  := :p_batch_id;
    v_err_stmt := 'START PROC';
    --Join this with the information schema and columns only of    Qualifying columns are those that have TEXT,VARIANT,ARRAY, OBJECT
    sql_stmt := ' SELECT db_name,schema_name,table_name,column_name FROM SEARCH_DATA_TABLE_COUNT
          WHERE batch_id = ' || :p_batch_id || '  order by db_name, schema_name ,table_name ';
    v_err_stmt := sql_stmt;     

     SELECT seq_BATCH_RUN_ID.NEXTVAL into v_seq_BATCH_RUN_ID;
     res1 := (EXECUTE IMMEDIATE :sql_stmt);
     LET cur1 CURSOR FOR res1;
     

     --FOR record_row IN c_tables DO
    FOR record_row IN cur1 DO    
        -- Assign cursor values to local variables
        vDB_NAME     := record_row.db_name;
        vSchema_NAME := record_row.schema_name;
        vTable_Name  := record_row.table_name;
        vColumn_Name := record_row.column_name;

        -- Construct the fully qualified table name (quoted for safety)
		v_full_table_name := '"' || vDB_NAME || '"."' || vSchema_NAME || '"."' || vTable_Name || '"';
        v_err_stmt := 'In the BEGIN THE CURSOR LOOP';
        
        -- 3. CONSTRUCT THE DYNAMIC SQL (Replicating your exact desired output SQL)
        v_err_stmt := 'Before the insert statement ';
        vPROC_SQL := 
            'INSERT INTO SEARCH_DATA_COLUMN_COUNT 
             SELECT 
                 ? AS BATCH_ID,
                 ? as BATCH_RUN_ID,
                 ''' || vDB_NAME || ''' AS DB_NAME, 
                 ''' || vSchema_NAME || ''' AS SCHEMA_NAME, 
                 ''' || vTable_Name || ''' AS TABLE_NAME, 
                 ''' || vColumn_Name || ''' AS COLUMN_NAME,
                 tot.total_cnt, 
                 tot.MATCHING_PATERN_CNT, 
                 (tot.MATCHING_PATERN_CNT / tot.total_cnt) * 100 AS MATCHING_PERCENTAGE, 
                 tot.DISTINCT_MATCHING_CNT,
                 CURRENT_TIMESTAMP(),
                 ? as SQL_USED,
                 '''''' || v_pattern_check || '''''' as CLASSIFICATION_TYPE
             FROM 
                 (
                    SELECT 
                        COUNT(*) OVER () AS total_cnt,
                        FLT.MATCHING_PATERN_CNT,
                        FLT.DISTINCT_MATCHING_CNT
                    FROM ' || v_full_table_name || ' tot
                    JOIN 
                        (
                            SELECT 
                                COUNT(*) AS MATCHING_PATERN_CNT, 
                                COUNT(DISTINCT ' || vColumn_Name || ') AS DISTINCT_MATCHING_CNT 
                            FROM ' || v_full_table_name || ' flt 
                            WHERE ' || vColumn_Name || ' REGEXP ''' || v_ssn_regex || '''
                        ) FLT 
                    LIMIT 1
                 ) tot;';
        v_err_stmt := vPROC_SQL;
									 
									 
										   
								  

        EXECUTE IMMEDIATE :vPROC_SQL USING (v_batch_id,v_seq_BATCH_RUN_ID, vPROC_SQL); 
        vPROC_SQL := 'Before the end loop';
    END FOR; -- End of Cursor Loop
    return 'SUCCESS';
    -- 5. Return success and the results from the temporary table
 

     exception
        when other then
         INSERT INTO SEARCH_DATA_ERROR_LOG (BATCH_ID, DB_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_SQL, ERROR_MSG)
                        VALUES (:v_batch_id, :vDB_NAME, :vSchema_NAME, :vTable_Name, :vPROC_SQL, :SQLERRM);
        return object_construct('Error type', 'STATEMENT_ERROR',
                                'Error_string', v_err_stmt,
                                'SQLCODE', sqlcode,
                                'SQLERRM', sqlerrm,
                                'SQLSTATE', sqlstate
                                ); 

END;

  
 
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
                -- >>> START OF NESTED ERROR HANDLING BLOCK <<<
                BEGIN
                v_err_stmt := 'In Loop for table ' ||  :vTable_Name;
                   --EXECUTE IMMEDIATE vPROC_SQL ;
                    vPROC_SQL := 'select count(*) as cnt from ' || :vDB_NAME || '.' || :vSchema_NAME || '.' || :vTable_Name || 
                    ' WHERE (SEARCH((*), ' || '''' || strSearch_Value || '''))';
                    v_err_stmt := vPROC_SQL;
                    v_row_count := 0; -- Reset count before execution    
                    res_3 := (EXECUTE IMMEDIATE :vPROC_SQL);
                    LET temp_cur CURSOR FOR res_3;
                    FOR temp_row IN temp_cur DO
                        v_row_count := temp_row.cnt; -- Assign the value by the alias 'C'
                    END FOR;
                                    
                    if (v_row_count > 0 ) then
                     INSERT INTO SEARCH_DATA_TABLE_COUNT ( BATCH_ID ,DB_NAME , SCHEMA_NAME , TABLE_NAME , ROW_COUNT ,STR_SEARCHED, SQL_USED )
                     values(:v_batch_id, :vDB_NAME, :vSchema_NAME,:vTable_Name,:v_row_count,:strSearch_Value, :vPROC_SQL  );
                   end if;
                  EXCEPTION
                    -- Catch any error during the table search (e.g., "Expected non-empty set of columns...")
                        WHEN OTHER THEN
                        -- Log the error and continue to the next table
                         INSERT INTO SEARCH_DATA_ERROR_LOG (BATCH_ID, DB_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_SQL, ERROR_MSG)
                        VALUES (:v_batch_id, :vDB_NAME, :vSchema_NAME, :vTable_Name, :vPROC_SQL, :SQLERRM);
                            CONTINUE; 
                    END;  
                    
                    select to_char(current_timestamp(),'YYYY-MM-DD HH24:MI:SS') INTO :v_current_timestamp;
                    --if the count is > 0 then capture the db, schame, and table name and  check for the column.
                  END FOR; -- Table Loop
             END FOR; -- Schema Loop     
    END FOR; --DB Loop

  UPDATE  BATCH_SEARCH_DATA SET END_DATE = CURRENT_TIMESTAMP() WHERE BATCH_ID = :v_batch_id;    
  
RETURN 'SUCCESS';

    exception
        when other then
         INSERT INTO SEARCH_DATA_ERROR_LOG (BATCH_ID, DB_NAME, SCHEMA_NAME, TABLE_NAME, ERROR_SQL, ERROR_MSG)
                        VALUES (:v_batch_id, :vDB_NAME, :vSchema_NAME, :vTable_Name, :vPROC_SQL, :SQLERRM);
        return object_construct('Error type', 'STATEMENT_ERROR',
                                'Error_string', v_err_stmt,
                                'SQLCODE', sqlcode,
                                'SQLERRM', sqlerrm,
                                'SQLSTATE', sqlstate
                                ); 

END;   





