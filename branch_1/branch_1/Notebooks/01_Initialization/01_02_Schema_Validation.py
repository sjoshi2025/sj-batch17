# Databricks notebook source
# MAGIC %run "../Common/Global_Variables"

# COMMAND ----------

# MAGIC %run "../Common/Shared_Utils"

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------

# MAGIC %run "./01_000_Schemas"

# COMMAND ----------

# MAGIC %run "../Common/Schema/01_000_Schemas_employee"

# COMMAND ----------

dbutils.widgets.dropdown("working_environment","DEV",["DEV","TEST","PROD"])
dbutils.widgets.dropdown("database_schema","db1",["db1","db2","db3"])
dbutils.widgets.dropdown("update_schema","True",["False","True"])
dbutils.widgets.dropdown("domain","aggregate",["source","aggregate","consumer"])

# COMMAND ----------

working_environment = dbutils.widgets.get("working_environment")
database_schema = dbutils.widgets.get("database_schema")    
update_schema = dbutils.widgets.get("update_schema")    
domain = dbutils.widgets.get("domain")

# COMMAND ----------

if domain=="source":
    database_name = source_aligned_schema[working_environment]
elif domain=="aggregate":
    database_name = aggregate_domain_schema[working_environment]
elif domain=="consumer":
    database_name = consumer_aligned_schema[working_environment]
  

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

def check_table_exists_and_schema(database_name, table_name, expected_schema,working_environment,catalog_name):
    try:
        table_df = spark.sql(f"select * from {catalog}.{database_name}.{table_name} LIMIT 1")

        actual_schema = table_df.schema
        if actual_schema != expected_schema:
            raise ValueError(f"Schema mismatch for table {database_name}.{table_name}. Expected: {expected_schema}, Actual: {actual_schema}")
        else:
            print(f"Table {database_name}.{table_name} exists and matches the expected schema.")
    except AnalysisException:
        raise ValueError(f"Table {database_name}.{table_name} does not exist.")



# COMMAND ----------

from pyspark.sql.types import StructType

def convert_schema_to_sql(schema):
    fields =[]
    for field in schema.fields:
        fields.append(f"{field.name} {field.dataType.simpleString()}")
    return ",".join(fields)

# COMMAND ----------

def schema_mismatch_changes(databricks_schema,table_name,expected_schema):

    table_df = spark.sql(f"select * from {catalog}.{database_name}.{table_name} LIMIT 1")

    actual_schema = table_df.schema
    actual_schema_dict ={field.name: {"dataType": field.dataType,"nullable": field.nullable} for field in actual_schema.fields}
    expected_schema_dict ={field.name: {"dataType": field.dataType,"nullable": field.nullable} for field in expected_schema.fields}
    
    newly_added_columns = {col:expected_schema_dict[col] for col in expected_schema_dict if col not in actual_schema_dict}
    
    type_nullable_change ={}

    for column in actual_schema_dict:
        if column in expected_schema_dict:
            if actual_schema_dict[column]["dataType"] != expected_schema_dict[column]["dataType"]:
                type_nullable_change[column] = {"dataType": expected_schema_dict[column]["dataType"],"nullable": expected_schema_dict[column]["nullable"]}
            elif actual_schema_dict[column]["nullable"] != expected_schema_dict[column]["nullable"]:
                type_nullable_change[column] = {"dataType": expected_schema_dict[column]["dataType"],"nullable": expected_schema_dict[column]["nullable"]}
    
    for column,columninfo in newly_added_columns.items():
        try:
            nullable_clause = "NOT NULL" if columninfo["nullable"]==False else ""
            spark.sql(f"ALTER TABLE {catalog}.{database_name}.{table_name} ADD COLUMN {column} {columninfo['dataType'].simpleString()} {nullable_clause}")

            log_event(event_log_table=f"{catalog}.{logs_schema}.{event_logs_tablename}",
                      event_message = f"{column} Column added in {table_name} table ",
                      notebook_path = notebook_path,
                      working_environment = working_environment,
                      event_type= "INFO",
                      event_description=" Schema Validation - Table schema updated. Table name "+table_name,
                      event_category= "Initization-Schema Validation",
                      user_id = user_id,
                      )
        except Exception as e:
            log_error(error_log_table=f"{catalog}.{logs_schema}.{error_logs_tablename}",
                      error_message= str(e),
                      notebook_path = notebook_path,
                      working_environment = working_environment,
                      error_type= "Schema Validation Error",
                      error_description="Column adding Error",
                      error_category= "Initization-Schema Validation",
                      user_id = user_id
                      )        


# COMMAND ----------

tables = get_table_list(database_schema)

if not tables:
    raise ValueError(f"Unknown database schema {database_schema}")

Type="INFO"

log_event(event_log_table=f"{catalog}.{logs_schema}.{event_logs_tablename}",
        event_message = f"Schema Validation for Sub Data product : "+database_schema+" is Started",
        notebook_path = notebook_path,
        working_environment = working_environment,
        event_type= "INFO",
        event_description=f"Schema Validation for Sub Data product: "+database_schema+" Tables. Table lists are :"+str(tables)+".",
        event_category= "Initization-Schema Validation",
        user_id = user_id,
    )
for table in tables:
    print(table)
    expected_schema = eval(f"{table}"+"_schema")
    try:
        check_table_exists_and_schema(database_name, table, expected_schema,working_environment,catalog)
    except ValueError as e:
        exception_message = str(e)
        not_exists_message = "does not exists"
        mismatched_message = "Schema mismatch"
        error_description = not_exists_message if not_exists_message in exception_message else mismatched_message
        print(exception_message)

        if not_exists_message in exception_message:
            log_error(error_log_table=f"{catalog}.{logs_schema}.{error_logs_tablename}",
                      error_message= exception_message,
                      notebook_path = notebook_path,
                      working_environment = working_environment,
                      error_type= "Schema Validation Error- Table does not Exist",
                      error_description="Table Does not Exist",
                      error_category= "Initization-Schema Validation",
                      user_id = user_id
                      )
            sql_schema = convert_schema_to_sql(expected_schema)

            print(f"Creating table {catalog}.{database_name}.{table} .......")
            spark.sql(f"CREATE TABLE {catalog}.{database_name}.{table} ({sql_schema})")

            log_event(event_log_table=f"Table {catalog}.{database_name}.{table}",
                event_message = "Schema Validation Table created under Sub Data product :"+database_schema+" Table details: "+table,
                notebook_path = notebook_path,
                working_environment = working_environment,
                event_type= "INFO",
                event_description="Schema Validation - Table created. Table name "+table,
                event_category= "Initization-Schema Validation",
                user_id = user_id,
            )
        elif(mismatched_message in exception_message) and (update_schema =="True"):
            log_error(error_log_table=f"{catalog}.{logs_schema}.{error_logs_tablename}",
                      error_message=exception_message,
                      notebook_path = notebook_path,
                      working_environment = working_environment,
                      error_type= "Schema Validation Error- Schema Mismatch",
                      error_description="Table Schema Mismatch",
                      error_category= "Initization-Schema Validation",
                      user_id = user_id
                )
        else:
            log_error(error_log_table=f"{catalog}.{logs_schema}.{error_logs_tablename}",
                      error_message=exception_message,
                      notebook_path = notebook_path,
                      working_environment = working_environment,
                      error_type= "Schema Validation Error- Schema Mismatch",
                      error_description=exception_message,
                      error_category= "Initization-Schema Validation",
                      user_id = user_id
                )
            log_event(event_log_table=f"{catalog}.{logs_schema}.{event_logs_tablename}",
                event_message = "Schema Validation Table Schema Mismatch under Sub Data product :"+database_schema+" is Failde.",
                notebook_path = notebook_path,
                working_environment = working_environment,
                event_type= "Failed",
                event_description="Schema Validation for Sub Data product :"+database_schema+" Tables. Table lists are :"+str(tables),
                event_category= "Initization-Schema Validation",
                user_id = user_id,
            )
            raise Exception
log_event(event_log_table=f"{catalog}.{logs_schema}.{event_logs_tablename}",
    event_message = "Schema Validation for Sub Data product :"+database_schema+" is Completed.",
    notebook_path = notebook_path,
    working_environment = working_environment,
    event_type= "SUCCESS",
    event_description="Schema Validation for Sub Data product :"+database_schema+" Tables. Table lists are :"+str(tables),
    event_category= "Initization-Schema Validation",
    user_id = user_id,
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

