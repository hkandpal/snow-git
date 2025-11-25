 CREATE TEMPORARY FUNCTION ADD_NUMBERS(num1 INT, num2 INT)
RETURNS INT
AS
$$
  num1 + num2
$$;

SELECT ADD_NUMBERS(12, 4);
SELECT CURRENT_SESSION();
select * from INFORMATION_SCHEMA.FUNCTIONS;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    END_TIME_RANGE_START => CURRENT_TIMESTAMP() - INTERVAL '6 DAY',
    END_TIME_RANGE_END => CURRENT_TIMESTAMP()
)) where QUERY_TEXT like '%CREATE TEMPORARY FUNCTION%';

select * from snowflake.account_usage.query_history where QUERY_TEXT like '%CREATE TEMPORARY FUNCTION%';

SELECT *   FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) ORDER BY start_time;
  
select ai_complete('claude-haiku-4-5', 'Why is the Snowflake plattform awesome? Max 200 words.')::string;

select * from snowflake.account_usage.PASSWORD_POLICIES;
select name from snowflake.account_usage.users where has_password = 'TRUE';
SELECT name FROM snowflake.account_usage.users WHERE 
deleted_on IS NULL AND name != 'SNOWFLAKE' and has_password = 'TRUE';
SELECT  to_Date(DATEADD(Month, seq4(), '2025-01-31')) AS MONTH_END_date
            FROM  TABLE(GENERATOR(rowcount => 365 * 24));

 


select * from snowflake.account_usage.query_history;
select * from snowflake.account_usage.warehouse_load_history;

 select query_tag,q.*  FROM snowflake.account_usage.query_history q;
SELECT query_tag,
       SUM(credits_used_cloud_services + credits_used_compute) AS total_credits
FROM snowflake.account_usage.query_history  
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP)
GROUP BY query_tag
ORDER BY total_credits DESC;

select *     from snowflake.account_usage.access_history;
/*
How are costs incurred
The total cost of using Snowflake is the aggregate of the cost of using data transfer, storage, and compute resources.
Compute Resources
    Virtual Warehouse Compute: Virtual warehouses are user-managed compute resources that consume credits when loading data, executing queries, and performing other DML operations.
    Serverless Compute: There are Snowflake features such as Search Optimization and Snowpipe that use Snowflake-managed compute resources rather than virtual warehouses. 
    Cloud Services Compute: The cloud services layer of the Snowflake architecture consumes credits as it performs behind-the-scenes tasks such as authentication, metadata management, and access control. This is only charged if it exceeds 10% of the daily WH usage.
Storage Resources: The monthly cost for storing data in Snowflake is based on a flat rate per terabyte (TB). 

Data Transfer Resources:
Snowflake does not charge data ingress fees to bring data into your account, but does charge for data egress.

*/
/*The WAREHOUSE_METERING_HISTORY view in Snowflake's ACCOUNT_USAGE schema provides hourly credit usage data for each virtual warehouse within your account. */
-- Ideal time of warehouse
SELECT warehouse_name, SUM(credits_used_compute) as  SUM_credits_used_compute,
 SUM(credits_attributed_compute_queries) as sum_credits_attributed_compute_queries,
( SUM(credits_used_compute) - SUM(credits_attributed_compute_queries) ) AS idle_cost
     FROM     SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE --start_time >= DATEADD('days', -10, CURRENT_DATE()) -- Adjust the time range as needed
    --AND 
    end_time < CURRENT_DATE() GROUP BY     warehouse_name order by idle_cost desc;

    
/*  WAREHOUSE_LOAD_HISTORY view  Load history is shown in 5-minute intervals.
*/
select * from snowflake.account_usage.warehouse_load_history order by start_time desc;

SELECT warehouse_name, start_time, end_time,  DATE(start_time) AS usage_date,
    HOUR(start_time) AS hour_of_day,  avg_running, avg_queued_load 
FROM snowflake.account_usage.warehouse_load_history
WHERE start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP()) ORDER BY warehouse_name, start_time;

/* we know the amount of work that was performed during the time period (via WAREHOUSE_LOAD_HISTORY) 
and the cost per time period (via WAREHOUSE_METERING_HISTORY),  we can perform a simple efficiency 
ratio calculation for a particular warehouse.
LInk https://www.snowflake.com/en/blog/understanding-snowflake-utilization-warehouse-profiling/
*/
with cte as (  select date_trunc('hour', start_time) as start_time, end_time, warehouse_name, credits_used
    from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    --where warehouse_name = 'UDW_FIN_PROD_BI_VW'
    )
select date_trunc('hour', a.start_time) as start_hour, avg(AVG_RUNNING) avg_running , avg(credits_used) avg_used_credit,    
avg(AVG_RUNNING) / avg(credits_used) * 100
from snowflake.account_usage.warehouse_load_history a
join cte b on a.start_time = date_trunc('hour', a.start_time)
--where a.warehouse_name = 'UDW_FIN_PROD_BI_VW'
group by start_hour     order by start_hour desc;

-- end
select * from snowflake.account_usage.warehouse_metering_history; --hourly credit usage
select * from snowflake.account_usage.warehouse_load_history;--Load hist is shown in 5-minute intveral
select * from snowflake.account_usage.warehouse_events_history;-- warehouse events
select * from snowflake.account_usage.query_history;
 
--https://blog.greybeam.ai/snowflake-cost-per-query/
/* 1) Gather warehouse suspend events
2) Enrich query data with execution times and idle periods
3) Create a timeline of all events (queries and idle periods)
4) Join with WAREHOUSE_METERING_HISTORY to attribute costs
*/

SET startDate = DATEADD('DAY', -15, current_date);
WITH warehouse_list AS (
    SELECT DISTINCT warehouse_name, warehouse_id
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE warehouse_name IS NOT NULL  AND start_time >= $startDate
),

warehouse_events AS (
    SELECT weh.warehouse_id, weh.timestamp 
    FROM snowflake.account_usage.warehouse_events_history as weh
    WHERE event_name = 'SUSPEND_WAREHOUSE'        
),

queries_filtered AS (
    SELECT q.query_id , q.warehouse_id , q.warehouse_name , q.warehouse_size , q.role_name
        , q.user_name , q.query_text , q.query_hash , q.queued_overload_time , q.compilation_time
        , q.queued_provisioning_time , q.queued_repair_time , q.list_external_files_time
        , q.start_time , TIMEADD( 'millisecond', q.queued_overload_time + q.compilation_time +
                q.queued_provisioning_time + q.queued_repair_time + q.list_external_files_time,
                q.start_time ) AS execution_start_time
        , q.end_time::timestamp AS end_time , w.timestamp AS suspended_at
        , MAX(q.end_time) OVER (PARTITION BY q.warehouse_id, w.timestamp
        ORDER BY execution_start_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as end_time_max
     , LEAD(execution_start_time) OVER (PARTITION BY q.warehouse_id ORDER BY execution_start_time ASC) as next_query_at
    FROM snowflake.account_usage.query_history AS q
    ASOF JOIN warehouse_events AS w MATCH_CONDITION (q.end_time::timestamp <= w.timestamp) ON q.warehouse_id = w.warehouse_id
    WHERE q.warehouse_size IS NOT NULL AND q.execution_status = 'SUCCESS'
        AND start_time >= $startDate   AND EXISTS ( SELECT 1 FROM warehouse_list AS wl
            WHERE q.warehouse_id = wl.warehouse_id
            ) 
            ) 
 --       SELECT * FROM queries_filtered;  
        ,
queries_enriched AS (
    SELECT q.query_id , q.warehouse_id , q.execution_start_time , q.end_time::timestamp AS end_time
        , q.end_time_max AS end_time_running , q.next_query_at , q.suspended_at
        , (CASE WHEN q.next_query_at > q.suspended_at THEN q.end_time_max
            WHEN q.next_query_at > q.end_time_max THEN q.end_time_max
            WHEN q.next_query_at < q.end_time_max THEN NULL
            WHEN q.next_query_at IS NULL THEN q.end_time
            END)::timestamp AS idle_start_at
        ,IFF(idle_start_at IS NOT NULL, LEAST(COALESCE(next_query_at, '3000-01-01'),
            q.suspended_at), NULL)::timestamp  AS idle_end_at
        , HOUR(execution_start_time::timestamp) = HOUR(q.end_time::timestamp) AS is_same_hour_query
        , HOUR(idle_start_at) = HOUR(idle_end_at) AS is_same_hour_idle
        , DATE_TRUNC('HOUR', execution_start_time) AS query_start_hour
        , DATE_TRUNC('HOUR', idle_start_at) as idle_start_hour
        , DATEDIFF('HOUR', execution_start_time, q.end_time) AS hours_span_query
        , DATEDIFF('HOUR', idle_start_at, idle_end_at) AS hours_span_idle
    FROM queries_filtered AS q
)
--select * from queries_enriched;
,

numgen AS ( 
    SELECT  0 AS num     UNION ALL 
    SELECT ROW_NUMBER() OVER (ORDER BY NULL)
    FROM table(generator(ROWCOUNT=>24)) -- assuming no one has idle or queries running more than 24 hours
),

mega_timeline AS (
    SELECT q.query_id , q.warehouse_id , 'query' AS type
        , q.execution_start_time AS event_start_at , q.end_time AS event_end_at
        , DATEDIFF('MILLISECOND', event_start_at, event_end_at)*0.001 AS event_time_secs
        , q.query_start_hour AS meter_start_hour , NULL AS meter_end_hour
        , q.execution_start_time AS meter_start_at , q.end_time AS meter_end_at
        , DATEDIFF('MILLISECOND', meter_start_at, meter_end_at)*0.001 AS meter_time_secs
    FROM queries_enriched AS q
    WHERE q.is_same_hour_query = TRUE
    
    UNION ALL
    
    SELECT 'idle_' || q.query_id , q.warehouse_id , 'idle' AS type 
        , q.idle_start_at AS event_start_at , q.idle_end_at AS event_end_at
        , DATEDIFF('MILLISECOND', event_start_at, event_end_at)*0.001 AS event_time_secs
        , q.idle_start_hour AS meter_start_hour , NULL AS meter_end_hour
        , q.idle_start_at AS meter_start_at , q.idle_end_at AS meter_end_at
        , DATEDIFF('MILLISECOND', meter_start_at, meter_end_at)*0.001 AS meter_time_secs
    FROM queries_enriched AS q WHERE q.is_same_hour_idle = TRUE

    UNION ALL

    SELECT 'idle_' || q.query_id, q.warehouse_id , 'idle' , q.idle_start_at AS event_start_at
        , q.idle_end_at AS event_end_at
        , DATEDIFF('MILLISECOND', event_start_at, event_end_at)*0.001 AS event_time_secs
        , DATEADD('HOUR', n.num, DATE_TRUNC('HOUR', q.idle_start_at)) AS meter_start_hour
        , DATEADD('HOUR', n.num + 1, DATE_TRUNC('HOUR', q.idle_start_at)) AS meter_end_hour
        , GREATEST(meter_start_hour, q.idle_start_at) as meter_start_at
        , LEAST(meter_end_hour, q.idle_end_at) as meter_end_at
        , DATEDIFF('MILLISECOND', meter_start_at, meter_end_at)*0.001 AS meter_time_secs
    FROM queries_enriched AS q
    LEFT JOIN numgen AS n ON q.hours_span_idle >= n.num
    WHERE q.is_same_hour_idle = FALSE
    
    UNION ALL
    
    SELECT q.query_id , q.warehouse_id , 'query' , q.execution_start_time AS event_start_at
        , q.end_time AS event_end_at 
        , DATEDIFF('MILLISECOND', event_start_at, event_end_at)*0.001 AS event_time_secs
        , DATEADD('HOUR', n.num, DATE_TRUNC('HOUR', q.execution_start_time)) AS meter_start_hour
        , DATEADD('HOUR', n.num + 1, DATE_TRUNC('HOUR', q.execution_start_time)) AS meter_end_hour
        , GREATEST(meter_start_hour, q.execution_start_time) as meter_start_at
        , LEAST(meter_end_hour, q.end_time) as meter_end_at
        , DATEDIFF('MILLISECOND', meter_start_at, meter_end_at)*0.001 AS meter_time_secs
    FROM queries_enriched AS q
    LEFT JOIN numgen AS n  ON q.hours_span_query >= n.num
    WHERE q.is_same_hour_query = FALSE
    )
--SELECT * FROM mega_timeline;  -- this is where we get the idle or query time line
    ,
metered AS ( SELECT m.query_id , REPLACE(m.query_id, 'idle_', '') as original_query_id
        , m.warehouse_id , m.type , m.event_start_at , m.event_end_at , m.event_time_secs
        , m.meter_start_hour , m.meter_start_at , m.meter_end_at , m.meter_time_secs
        , SUM(m.meter_time_secs) OVER (PARTITION BY m.warehouse_id, m.meter_start_hour) AS total_meter_time_secs
        , (m.meter_time_secs / total_meter_time_secs) * w.credits_used_compute AS credits_used
    FROM mega_timeline AS m
    JOIN snowflake.account_usage.warehouse_metering_history AS w
        ON m.warehouse_id = w.warehouse_id AND m.meter_start_hour = w.start_time
),

final AS (
    SELECT
        m.* EXCLUDE total_meter_time_secs, meter_end_at, original_query_id
        , q.query_text
        , q.query_hash
        , q.warehouse_size
        , q.warehouse_name
        , q.role_name
        , q.user_name
    FROM metered AS m
    JOIN queries_filtered AS q
        ON m.original_query_id = q.query_id
)
SELECT     * FROM final;



-- new
  SELECT DISTINCT warehouse_name
    FROM UTILIZATION -- Querying the target table
    WHERE warehouse_name IS NOT NULL
    ORDER BY warehouse_name

-- -- Himanshu 10/9 identify oversize warehouse 
SELECT   warehouse_name,warehouse_size,MAX(query_load_percent) AS max_query_load,
  AVG(query_load_percent) AS avg_query_load,COUNT(*) AS query_count
FROM   snowflake.account_usage.query_history
WHERE start_time >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())   AND query_load_percent IS NOT NULL
GROUP BY  warehouse_name,warehouse_size  HAVING max_query_load < 100 OR avg_query_load < 50
  ORDER BY max_query_load, avg_query_load;    