# Databricks notebook source
# retrieve the jobRunId parameter
dbutils.widgets.text("job_run_id","")
job_run_id = dbutils.widgets.get("job_run_id")

# COMMAND ----------

# MAGIC %run "./Global_Variables"

# COMMAND ----------

def get_max_error_id(error_log_table):

    #read error_log table
    error_log_df = spark.table(error_log_table)

    # get the count of records in the table
    record_count = error_log_df.count()

    # get the max error id if the table is not empty
    if record_count > 0:
        max_error_id = error_log_df.agg({"error_id": "max"}).collect()[0][0]
    else:
        max_error_id = 0

    return max_error_id


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

def log_error(error_log_table, error_message, notebook_path, working_environment, error_type, error_description, 
              error_category, user_id):

    error_data = [(error_message, notebook_path, working_environment, error_type, error_description, error_category, user_id)]
    error_df = spark.createDataFrame(error_data, schema = StructType([
        StructField("error_message", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("working_environment", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("error_description", StringType(), True),
        StructField("error_category", StringType(), True),
        StructField("user_id", StringType(), True)
    ]))

    error_df = error_df.withColumn("error_timestamp",current_timestamp())
    max_error_id = get_max_error_id(error_log_table)
    error_df = error_df.withColumn("error_id",lit(max_error_id+1))
    error_df = error_df.withColumn("job_run_id",lit(job_run_id))

    # write the error details to error_log table
    error_df.write.format("delta").mode("append").saveAsTable(error_log_table)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

def log_event(event_log_table, event_message, notebook_path, working_environment, event_type, event_description, event_category, user_id):

    event_data = [(event_message, notebook_path, working_environment, event_type, event_description, event_category, user_id)]

    event_df = spark.createDataFrame(event_data, schema = StructType([
    StructField("event_message", StringType(), True),
    StructField("notebook_path", StringType(), True),
    StructField("working_environment", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_description", StringType(), True),
    StructField("event_category", StringType(), True),
    StructField("user_id", StringType(), True)]))
    
    event_df = event_df.withColumn("event_timestamp",current_timestamp())
    event_df = event_df.withColumn("job_run_id", lit(job_run_id))

    # write the event details to event_log table
    event_df.write.format("delta").mode("append").saveAsTable(event_log_table)

# COMMAND ----------

def get_table_list(schema_name):
    return database.get(schema_name,[])