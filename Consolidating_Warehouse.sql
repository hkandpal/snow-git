--by hk in workspace
--edited in Git by HK on 11/24

SELECT warehouse_name, SUM(credits_used_compute) as  SUM_credits_used_compute,
SUM(credits_attributed_compute_queries) as sum_credits_attributed_compute_queries,
(SUM(credits_used_compute) - SUM(credits_attributed_compute_queries)) AS idle_cost
 FROM snowflake.account_usage.warehouse_metering_history 
WHERE start_time >= DATEADD('days', -30, CURRENT_DATE()) -- Adjust the time range as needed
    and end_time < CURRENT_DATE() 
GROUP BY warehouse_name 
order by idle_cost desc;

set interval_start = current_date()-5;
set interval_end =   current_date();
set Vwarehouse_name = 'COMPUTE_WH';
select DATEDIFF(second, $interval_end ,$interval_start   ) seconds  ;  -- 432000

-- The query to get the seconds
select DATEADD(second,r_num, $interval_start) AS seconds , r_num 
    FROM ( (select current_date()) cd
    JOIN (select row_number() over(order by null) r_num from table(generator(rowcount => 432000))  ) r
);

with  query_sec as (
    select DATEADD(second,r_num, $interval_start) AS seconds , r_num FROM ( (select current_date()) cd
    JOIN (select row_number() over(order by null) r_num from table(generator(rowcount => 432000))  ) r
        )  ) 
    select distinct query_sec.seconds, warehouse_name,nvl(cluster_number,0)cluster_number,
    DATE_TRUNC('second', start_time) start_time,DATE_TRUNC('second', end_time) end_time
    FROM snowflake.account_usage.query_history qh
    join query_sec on query_sec.seconds between DATEADD(second, -1, qh.start_time) and qh.end_time   
    where warehouse_name= $Vwarehouse_name 
    and start_time  > $interval_start
    order by start_time,end_time,seconds;

     --  this is the query joining the warehouse-status and seconds
with 
whse_res  as (
    select warehouse_name, nvl(CLUSTER_NUMBER,0) CLUSTER_NBR,timestamp as whse_start  ,event_state,event_name,
     rank() OVER (PARTITION BY warehouse_name,CLUSTER_NBR,event_name ORDER BY timestamp) as series
    FROM  snowflake.account_usage.warehouse_events_history
     WHERE event_name in ('RESUME_CLUSTER','RESUME_WAREHOUSE','WAREHOUSE_AUTORESUME') AND warehouse_name=$Vwarehouse_name
     )
,whse_sus  as (
    select warehouse_name, nvl(CLUSTER_NUMBER,0) CLUSTER_NBR,timestamp as whse_end  ,event_state,event_name,
     rank() OVER (PARTITION BY warehouse_name,CLUSTER_NBR,event_name ORDER BY timestamp) as series
    FROM  snowflake.account_usage.warehouse_events_history
     WHERE event_name in ('SUSPEND_CLUSTER','SUSPEND_WAREHOUSE','WAREHOUSE_AUTOSUSPEND') AND warehouse_name= $Vwarehouse_name
     )
  , sec as (
    select DATEADD(second,r_num, $interval_start) AS seconds , r_num FROM ( (select current_date()) cd
    JOIN (select row_number() over(order by null) r_num from table(generator(rowcount => 432000))  ) r
        )  ) 
  select   res.warehouse_name,  res.CLUSTER_NBR, --res.series,
    res.event_name, sec.seconds, datediff('seconds', whse_start, sec.seconds) as running_sec,res.whse_start,sus.whse_end, sus.event_name,
    datediff('seconds', whse_start, whse_end)  as seconds_active
    from whse_res res
    asof join whse_sus sus MATCH_CONDITION (res.whse_start < sus.whse_end)
    on res.warehouse_name= sus.warehouse_name and res.CLUSTER_NBR=sus.CLUSTER_NBR
    join sec on sec.seconds between res.whse_start and sus.whse_end
    where whse_start > current_date() -5
    order by res.whse_start ,running_sec ;

---final query

with 
whse_res  as (
    select warehouse_name, nvl(CLUSTER_NUMBER,0) CLUSTER_NBR,timestamp as whse_start  ,event_state,event_name,
     rank() OVER (PARTITION BY warehouse_name,CLUSTER_NBR,event_name ORDER BY timestamp) as series
    FROM  snowflake.account_usage.warehouse_events_history
     WHERE event_name in ('RESUME_CLUSTER','RESUME_WAREHOUSE-xx','WAREHOUSE_AUTORESUME') AND warehouse_name=$Vwarehouse_name
     )
,whse_sus  as (
    select warehouse_name, nvl(CLUSTER_NUMBER,0) CLUSTER_NBR,timestamp as whse_end  ,event_state,event_name,
     rank() OVER (PARTITION BY warehouse_name,CLUSTER_NBR,event_name ORDER BY timestamp) as series
    FROM  snowflake.account_usage.warehouse_events_history
     WHERE event_name in ('SUSPEND_CLUSTER','SUSPEND_WAREHOUSE-xx','WAREHOUSE_AUTOSUSPEND') AND warehouse_name= $Vwarehouse_name
     )
  , sec as (
    select DATEADD(second,r_num, $interval_start) AS seconds , r_num FROM ( (select current_date()) cd
    JOIN (select row_number() over(order by null) r_num from table(generator(rowcount => 432000))  ) r
        )  ) 
   , query_sec as (
    select DATEADD(second,r_num, $interval_start) AS seconds , r_num FROM ( (select current_date()) cd
    JOIN (select row_number() over(order by null) r_num from table(generator(rowcount => 432000))  ) r
        )  ) 
 , query_exec_time as (
    select distinct query_sec.seconds, warehouse_name,nvl(cluster_number,0)cluster_number,
    DATE_TRUNC('second', start_time) start_time,DATE_TRUNC('second', end_time) end_time
    FROM snowflake.account_usage.query_history qh
     --   join sec on sec.seconds between qh.start_time and qh.end_time 
    join query_sec on query_sec.seconds between DATEADD(second, -1, qh.start_time) and qh.end_time   
    where warehouse_name= $Vwarehouse_name 
    --and start_time >= current_date() -1 and end_time>=current_date()-2
    -- and start_time >= $interval_start and end_time>=$interval_end
    and start_time  > $interval_start
     )
  select  sec.seconds as whse_up_seconds, qs.seconds as sec_no_qry_running, decode(sec_no_qry_running,null,1,0) qry_running, 
    sum(qry_running) over (partition by DATE_TRUNC('HOUR', sec.seconds)) total_sec_no_qry_running_in_an_hour,
    sum(qry_running) over (partition by DATE_TRUNC('DAY', sec.seconds)) total_sec_no_qry_running_in_an_day,
    sum(decode(sec_no_qry_running,null,1,1)) over (partition by DATE_TRUNC('DAY', sec.seconds)) total_sec_whse_running_in_an_day,
    res.warehouse_name,  res.CLUSTER_NBR, --res.series,
    res.event_name,  datediff('seconds', whse_start, sec.seconds) as running_sec,
    res.whse_start,sus.whse_end, sus.event_name,  datediff('seconds', whse_start, whse_end)  as seconds_active
    from whse_res res
    asof join whse_sus sus MATCH_CONDITION (res.whse_start < sus.whse_end)
    on res.warehouse_name= sus.warehouse_name and res.CLUSTER_NBR=sus.CLUSTER_NBR
    join sec on sec.seconds between res.whse_start and sus.whse_end
    left outer join query_exec_time  qs on qs.warehouse_name = res.warehouse_name and qs.cluster_number = res.CLUSTER_NBR
                and qs.seconds = sec.seconds
    --where whse_start > current_date() -2
   qualify ROW_NUMBER()    OVER  (partition by DATE_TRUNC('HOUR', sec.seconds) order by sec.seconds) = 1
   --qualify ROW_NUMBER()    OVER  (partition by DATE_TRUNC('DAY', sec.seconds) order by sec.seconds) = 1
    order by res.whse_start ,running_sec ;
