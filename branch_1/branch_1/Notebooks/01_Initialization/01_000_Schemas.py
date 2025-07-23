# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

error_log_schema = StructType([
        StructField("error_id", StringType(), True),
        StructField("job_run_id", StringType(), True),
        StructField("error_timestamp", TimestampType(), True),
        StructField("error_message", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("working_environment", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("error_description", StringType(), True),
        StructField("error_category", StringType(), True),
        StructField("user_id", StringType(), True)
    ])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

event_log_schema = StructType([
        StructField("job_run_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_message", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("working_environment", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_description", StringType(), True),
        StructField("event_category", StringType(), True),
        StructField("user_id", StringType(), True)
    ])