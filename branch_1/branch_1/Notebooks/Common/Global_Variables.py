# Databricks notebook source
dbutils.widgets.dropdown(
    "working_environment",
    "DEV",
    ["DEV", "SIT", "PROD"]
)

# COMMAND ----------

working_environment = dbutils.widgets.get("working_environment")

# COMMAND ----------

catalog = "dynata"
logs_schema = "mr_db_staging"
error_logs_tablename = "error_logs"
event_logs_tablename = "event_logs"
config_database_schema = "mr_db_staging"
database_schema = "mr_db_staging"

# COMMAND ----------

# Update container path and schema

source_aligned_container_path ={
    "DEV": 'DEV Bronze Path',
    "SIT": 'SIT Bronze Path',
    "PROD": 'PROD Bronze Path'
}

aggregate_domain_container_path ={
    "DEV": 'DEV Silver Path',
    "SIT": 'SIT Silver Path',
    "PROD": 'PROD Silver Path'
}

consumer_aligned_container_path ={
    "DEV": 'DEV Gold Path',
    "SIT": 'SIT Gold Path',
    "PROD": 'PROD Gold Path'
}


source_aligned_schema ={
    "DEV": 'mr_db_staging',
    "SIT": 'SIT Bronze schema',
    "PROD": 'PROD Bronze schema'
}

aggregate_domain_schema ={
    "DEV": 'mr_db_staging',
    "SIT": 'SIT Silver schema',
    "PROD": 'PROD Silver schema'
}

consumer_aligned_schema ={
    "DEV": 'mr_db_staging',
    "SIT": 'SIT Gold schema',
    "PROD": 'PROD Gold schema'
}

# COMMAND ----------

# Access the values using the working_enviorment variable
current_source_aligned_container_path = source_aligned_container_path[working_environment]
current_aggregate_domain_container_path = aggregate_domain_container_path[working_environment]
current_consumer_aligned_container_path = consumer_aligned_container_path[working_environment]

current_source_aligned_schema = source_aligned_schema[working_environment]
current_aggregate_domain_schema = aggregate_domain_schema[working_environment]
current_consumer_aligned_schema = consumer_aligned_schema[working_environment]

# COMMAND ----------

source_aligned_domain_table_names = {
    "dv_team" : "dv_teams",
    "sp_team" : "sp_teams",
    "dp_team" : "dp_teams",
    "pm_team" : "pm_teams"
}

# COMMAND ----------

database={
    "client_base": ["mr_db_DEV","mr_db_SIT","mr_db_PROD"]
    }

# COMMAND ----------

