# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a8a9061e-e116-4fb4-86bc-9daf95255b0e",
# META       "default_lakehouse_name": "FUAM_Lakehouse",
# META       "default_lakehouse_workspace_id": "ee9e7fa6-49a3-4561-afc8-41b001d5bf5b",
# META       "known_lakehouses": []
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Tenant Admin Settings
# 
# ##### Data ingestion strategy:
# <mark style="background: lightgreen;">**APPEND**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Tenant_Settings_E2E**
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

from datetime import datetime, timedelta
from pyspark.sql.functions import col, explode
from delta.tables import *

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
bronze_file_location = f"Files/raw/tenant_settings/"
silver_table_name = "FUAM_Staging_Lakehouse.tenant_settings_silver"
gold_table_name = "tenant_settings"
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

if display_data:
    display(bronze_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
# Error handling, because of API change which is not rolled out everywhere
try:
    exploded_df = bronze_df.select(explode("tenantSettings").alias("d"))
except:
    exploded_df = bronze_df.select(explode("value").alias("d"))

# Extract json objects to tabular form
silver_df = exploded_df.select(col("d.*"))

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

# Get Silver table data
query = """
SELECT 
     to_date(current_timestamp()) AS TransferDate
     ,current_timestamp() AS TransferDateTime
     ,*
FROM """ + silver_table_name


silver_df = spark.sql(query)

if display_data:
     display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge semantic model refreshes to gold table
silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write history of bronze files
mssparkutils.fs.cp(bronze_file_location, bronze_file_location.replace("Files/raw/", "Files/history/") + datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
