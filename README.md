Welcome to your new dbt project!

### Using the starter project

Try running the following command:
- dbt build


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Hands On / Demo Instructions
### CI Job
1. **Get your database login**
   1. Click on your name
   2. Click on Your profile
   3. Click on “Credentials”
   4. Click on “Analytics”
   5. Copy your <Username>
   6. Copy your <Schema_name>

4. **Create a CI Job**
   1. Navigate to Deploy → Jobs
   2. Click "Create job" → "Continuous integration job"
   3. Job name: `CI Job`
   4. Environment: `Production`
   5. Run Timeout: `3600`

5. NEED instruction to get API key

6. Open up your terminal window
   1. execute the curl statement
   2. curl --request POST <your ci job url> \
      --header 'Content-Type: application/json' \
      --header 'Authorization: Token <your token>' \
      --data '{"cause": "Triggered via API"}'
7. Go check to see that your CI job is running


### Hands On / Demo Instructions
### Query Tagging

Reference: https://docs.getdbt.com/reference/resource-configs/snowflake-configs#query-tags

1) Create file under mnacros called "set_query_tag.sql"
2) Paste in code:


{%- macro set_query_tag() -%}

  {# These are built in dbt Cloud environment variables you can leverage to better understand your runs usage data #}
  {%- set dbt_job_id = env_var('DBT_CLOUD_JOB_ID', 'not set') -%}
  {%- set dbt_run_id = env_var('DBT_CLOUD_RUN_ID', 'not set') -%}
  {%- set dbt_run_reason = env_var('DBT_CLOUD_RUN_REASON', 'development_and_testing') -%}

  {# These are built in to dbt Core #}
  {%- set dbt_project_name = project_name -%}
  {%- set dbt_user_name = target.user -%}
  {%- set dbt_model_name = model.name -%}
  {%- set dbt_materialization_type = model.config.materialized -%}
  {%- set dbt_environment_name = target.name -%}

  {%- if dbt_model_name -%}
    
    {%- set new_query_tag = '{"dbt_environment_name": "%s", "dbt_job_id": "%s", "dbt_run_id": "%s", "dbt_run_reason": "%s", "dbt_project_name": "%s", "dbt_user_name": "%s", "dbt_model_name": "%s", "dbt_materialization_type": "%s"}'
      | format(
                dbt_environment_name,
                dbt_job_id,
                dbt_run_id, 
                dbt_run_reason,
                dbt_project_name,
                dbt_user_name,
                dbt_model_name,
                dbt_materialization_type
    ) -%}
    {%- set original_query_tag = get_current_query_tag() -%}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {%- do run_query("alter session set query_tag = '{}'".format(new_query_tag)) -%}
    {{ return(original_query_tag)}}
  
  {%- endif -%}
  
  {{ return(none) }}

{%- endmacro -%};

3) go to snowflake and run a query against the query history table

--query history view of most recent queries by a given user
SELECT
    qh.query_id,
    qh.query_tag,
    qh.user_name,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.warehouse_type,
    qh.database_name,
    qh.schema_name,
    qh.start_time,
    qh.execution_time
FROM snowflake.account_usage.query_history qh
WHERE
    qh.execution_status = 'SUCCESS'
    AND CONVERT_TIMEZONE('UTC', qh.start_time)::date > '2024-10-01'
    AND user_name ='DBTLABS_KLEWEN'
ORDER BY start_time DESC ;

--query history view of most recent queries by a given user with the query tag broken down
SELECT
    qh.query_id,
    qh.query_tag,
    qh.user_name,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.warehouse_type,
    qh.database_name,
    qh.schema_name,
    qh.start_time,
    qh.execution_time,
    CONVERT_TIMEZONE('UTC', qh.start_time)::date AS query_date,
    try_parse_json(qh.query_tag):dbt_environment_name::string as dbt_environment_name,
    try_parse_json(qh.query_tag):dbt_job_id::string as dbt_job_id,
    try_parse_json(qh.query_tag):dbt_run_id::string as dbt_run_id,
    try_parse_json(qh.query_tag):dbt_run_reason::string as dbt_run_reason,
    try_parse_json(qh.query_tag):dbt_user_name::string as dbt_user_name,
    try_parse_json(qh.query_tag):dbt_project_name::string as dbt_project_name,
    try_parse_json(qh.query_tag):dbt_model_name::string as dbt_model_name,
    try_parse_json(qh.query_tag):dbt_materialization_type::string as dbt_materialization_type,
    try_parse_json(qh.query_tag):dbt_incremental_full_refresh::string as dbt_incremental_full_refresh
FROM snowflake.account_usage.query_history qh
WHERE
    qh.execution_status = 'SUCCESS'
    AND CONVERT_TIMEZONE('UTC', qh.start_time)::date > '2024-10-01'
    AND user_name ='DBTLABS_KLEWEN'
ORDER BY start_time DESC ;



### Hands On / Demo Instructions
## Warehouse Size & Costing

Reference: https://docs.getdbt.com/reference/resource-configs/snowflake-configs#configuring-virtual-warehouses

1) Modify the  dbt_project to add variables under the vars: section

  wh_xs: 'DBT_FUNDAMENTALS'
  wh_m: 'ZERO_DBT'
  wh_l: 'TRANSFORMER_COALESCE'
  wh_xl: 'PACKAGES'

2) add to the model fct_page_views.sql.  In the config section paste the following:

        snowflake_warehouse = var('wh_xs')

3) Go to the database and run queries.


--query history view of most recent queries by a given user to find the cost used on the queries
with flat as (
    SELECT
        qh.query_id,
        qh.query_tag,
        qh.user_name,
        qh.warehouse_name,
        qh.warehouse_size,
        qh.warehouse_type,
        qh.database_name,
        qh.schema_name,
        qh.execution_time,
        CONVERT_TIMEZONE('UTC', qh.start_time)::date AS query_date,
        coalesce(qh.credits_used_cloud_services, 0) as credits_used_cloud_services,
        coalesce(qah.credits_attributed_compute, 0) as credits_attributed_compute,
        coalesce(qah.credits_used_query_acceleration, 0) as credits_used_query_acceleration,
        try_parse_json(qh.query_tag):dbt_environment_name::string as dbt_environment_name,
        try_parse_json(qh.query_tag):dbt_job_id::string as dbt_job_id,
        try_parse_json(qh.query_tag):dbt_run_id::string as dbt_run_id,
        try_parse_json(qh.query_tag):dbt_run_reason::string as dbt_run_reason,
        try_parse_json(qh.query_tag):dbt_user_name::string as dbt_user_name,
        try_parse_json(qh.query_tag):dbt_project_name::string as dbt_project_name,
        try_parse_json(qh.query_tag):dbt_model_name::string as dbt_model_name,
        try_parse_json(qh.query_tag):dbt_materialization_type::string as dbt_materialization_type,
        try_parse_json(qh.query_tag):dbt_incremental_full_refresh::string as dbt_incremental_full_refresh
    FROM snowflake.account_usage.query_history qh
    LEFT JOIN snowflake.account_usage.query_attribution_history qah
        ON qh.query_id = qah.query_id
    WHERE
        qh.execution_status = 'SUCCESS'
        AND qh.query_tag LIKE '%dbt_environment_name%'
        AND CONVERT_TIMEZONE('UTC', qh.start_time)::date > '2024-10-01'

), credits as (
    SELECT
        flat.query_date,
        flat.user_name,
        flat.dbt_project_name,
        flat.dbt_model_name as dbt_model_name,
        flat.dbt_environment_name,
        coalesce(sum(flat.credits_used_cloud_services + flat.credits_attributed_compute + flat.credits_used_query_acceleration), 0) as credits_used,
        COUNT(*) AS query_count,
        coalesce(avg(execution_time), 0) as avg_execution_time,
        coalesce(median(execution_time), 0) as median_execution_time,
        coalesce(min(execution_time), 0) as min_execution_time,
        coalesce(max(execution_time), 0) as max_execution_time,
    FROM flat
    WHERE flat.dbt_project_name ='jaffle_shop'
    GROUP BY 1, 2, 3, 4, 5
)
select *
from credits

;