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

# #### Delegated Tenant Settings Overrides
# 
# ##### Data ingestion strategy:
# <mark style="background: lightgreen;">**APPEND**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Delegated_Tenant_Settings_Override_E2E**
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
from pyspark.sql.functions import col, explode, from_json
from delta.tables import *
import pandas as pd
import json

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
bronze_file_location = f"Files/raw/delegated_tenant_settings_overrides/"
silver_table_name = "FUAM_Staging_Lakehouse.delegated_tenant_settings_overrides_silver"
gold_table_name = "delegated_tenant_settings_overrides"
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
try:
    exploded_df = bronze_df.select(explode("Overrides").alias("d"))
except:
    exploded_df = bronze_df.select(explode("value").alias("d"))

if display_data:
    display(exploded_df)

# This prevents the notebook running into an error when no delegated tenant settings are existant in the tenant
if exploded_df.count() == 0 :
    notebookutils.notebook.exit("No Delegated Settings available")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract json objects to tabular form
tenantSettings_df = exploded_df.select(col("d.id").alias("OverrideId"), explode("d.tenantSettings").alias("ts"))

if display_data:
    display(tenantSettings_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Stop session in case no delegated settings are available, so no error is raised
if tenantSettings_df.count() == 0:
    print("Stop session to prevent error in case no delegated settings are existant")
    mssparkutils.session.stop()

else:

    # Select columns from df
    silver_df = tenantSettings_df.select(
        col("OverrideId"),
        col("OverrideId").alias("CapacityId"),
        col("ts.tenantSettingGroup"),
        col("ts.title"),
        col("ts.delegatedFrom"),
        col("ts.settingName"),
        col("ts.enabled"),
        col("ts.canSpecifySecurityGroups")
        )

    # Show data for debug
    if display_data:
        display(silver_df)

    # Write prepared bronze_df to silver delta table
    silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(silver_table_name)

    # Get Silver table data
    query = """
    SELECT 
        to_date(current_timestamp()) AS TransferDate
        ,current_timestamp() AS TransferDateTime
        ,*
    FROM """ + silver_table_name

    silver_df = spark.sql(query)

    # Show data for debug
    if display_data:
        display(silver_df)

    # Append Gold Lakehouse table
    silver_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

    # Write history of bronze files
    mssparkutils.fs.cp(bronze_file_location, bronze_file_location.replace("Files/raw/", "Files/history/") + datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
