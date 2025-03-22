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

# #### Activities (aggregated last 30 days)
# 
# ##### Data ingestion strategy:
# <mark style="background: #88D5FF;">**REPLACE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Activities_E2E**
# 
# ##### Source:
# 
# **1 Delta table**  from FUAM_Lakehouse
# - **activities**
# 
# ##### Target:
# 
# **1 Delta table** in FUAM_Lakehouse 
# - **aggr_table_name** variable value

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
aggr_table_name = "aggregated_activities_last_30days"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get aggregated Gold data
query = """
SELECT 
    CreationDate,
    CreationDateKey,
    Activity,
    Experience,
    Workload,
    UPPER(CapacityId) as CapacityId,
    UPPER(WorkspaceId) as WorkspaceId,
    ObjectType,
    UPPER(ItemId) as ItemId,
    IsSuccess,
    ResultStatus,
    COUNT(*) AS CountOfActivities
FROM FUAM_Lakehouse.activities
WHERE CreationDate >= date_sub(CAST(current_timestamp() as DATE), 30)
GROUP BY
    CreationDate,
    CreationDateKey,
    Activity,
    Experience,
    Workload,
    CapacityId,
    WorkspaceId,
    ObjectType,
    ItemId,
    ResultStatus,
    IsSuccess
ORDER BY CreationDate DESC
"""

# Query data
agg_df = spark.sql(query) 



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(agg_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Overwrite aggregated table
agg_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(aggr_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
