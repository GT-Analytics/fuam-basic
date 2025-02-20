# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0fa52ded-4eb0-448b-87db-6ff6e31f9c6d",
# META       "default_lakehouse_name": "FUAM_Lakehouse",
# META       "default_lakehouse_workspace_id": "18ea641f-9de7-40c1-849c-22bdda18f679"
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Workspaces
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**REPLACE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_PBI_Workspaces_E2E**
# 
# ##### Source:
# 
# **Files** from FUAM_Lakehouse folder **bronze_file_location** variable
# 
# ##### Target:
# 
# **1 Delta table** in FUAM_Lakehouse 
# - **gold_table_name** variable value


# CELL ********************

import requests
from pyspark.sql.functions import col, lit, udf, explode, to_date, json_tuple, from_json, schema_of_json, get_json_object
from pyspark.sql.types import StringType, json
from pyspark.sql import SparkSession
import json
from delta.tables import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import datetime
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # needed for automatic schema evolution in merge 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## Parameters
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Variables
bronze_file_location = f"Files/raw/workspaces/"
silver_table_name = "FUAM_Staging_Lakehouse.workspaces_silver"
gold_table_name = "workspaces"
gold_table_name_with_prefix = f"Tables/{gold_table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Silver table, if exists
if spark.catalog.tableExists(silver_table_name):
    del_query = "DELETE FROM " + silver_table_name
    spark.sql(del_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Bronze data
bronze_df = spark.read.option("multiline", "true").json(bronze_file_location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
exploded_df = bronze_df.select(explode("value").alias("d"))

# Extract json objects to tabular form
extracted_df = exploded_df.select(col("d.*"))

# Convert key(s) to upper case
extracted_df = extracted_df.withColumn("id", f.upper(f.col("id")))
extracted_df = extracted_df.withColumn("capacityId", f.upper(f.col("capacityId")))

# Generate empty description column in case it is not available
if  not ("description" in extracted_df.columns):
    print("Create empty description column")
    extracted_df = extracted_df.withColumn("description", lit(""))

# Select all columns
silver_df = extracted_df.select(
    col("capacityId").alias("CapacityId"),
    col("id").alias("WorkspaceId"),
    col("capacityMigrationStatus").alias("CapacityMigrationStatus"),
    col("defaultDatasetStorageFormat").alias("DefaultDatasetStorageFormat"),
    col("description").alias("Description"),
    col("hasWorkspaceLevelSettings ").alias("HasWorkspaceLevelSettings"),
    col("isOnDedicatedCapacity").alias("IsOnDedicatedCapacity"),
    col("isReadOnly").alias("IsReadOnly"),
    col("name").alias("WorkspaceName"),
    col("state").alias("State"),
    col("type").alias("Type")
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared bronze_df to silver delta table
silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# This function maps and merges the silver data to gold dynamically
def write_silver_to_gold(silver_table_name, gold_table_name, ids):
    query = "SELECT *, current_timestamp() AS fuam_modified_at, False as fuam_deleted  FROM " + silver_table_name 
    silver_df = spark.sql(query)
    
    if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_table_name):
        # if exists -> MERGE to gold
        print("Gold table exists and will be merged.")
        gold_df = DeltaTable.forName(spark, gold_table_name)


        gold_columns = gold_df.toDF().columns
        silver_columns = silver_df.columns
        combined_columns = list(set(gold_columns) | set(silver_columns))
        id_cols = {}
        merge_id_stmt = ''
        for col in combined_columns:
            if col in ids:
                merge_id_stmt =  merge_id_stmt +  " t." + col + " = s." + col + " and"
                id_cols[col] = "s." + col

                
        # delete last and in merge id statement
        merge_id_stmt = merge_id_stmt[:-4]


        # Merge silver (s = source) to gold (t = target)
        try:
            merge = (gold_df.alias('t') \
            .merge(silver_df.alias('s'), merge_id_stmt )) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .whenNotMatchedBySourceUpdate( condition = "t.fuam_deleted == False or t.fuam_deleted IS NULL", set = {"fuam_deleted" : "True", "fuam_modified_at": "current_timestamp()"} )
            
            merge.execute()
        except:
        # In case the tables already exist, but the fuam column are not existent because of an old version do merge whenNotMatchedBySourceUpdate
            merge = (gold_df.alias('t') \
            .merge(silver_df.alias('s'), merge_id_stmt )) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
                        
            merge.execute()

    else:
        # else -> INSERT to gold
        print("Gold table will be created.")

        silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge semantic model refreshes to gold table
write_silver_to_gold(silver_table_name, gold_table_name, ['WorkspaceId'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# write history of bronze files

mssparkutils.fs.cp(bronze_file_location, bronze_file_location.replace("Files/raw/", "Files/history/") + datetime.datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
