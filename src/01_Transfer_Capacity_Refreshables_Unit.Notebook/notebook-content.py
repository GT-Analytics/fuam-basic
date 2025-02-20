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

# #### Capacity Refreshables
# 
# ##### Data ingestion strategy:
# <mark style="background: #D69AFE;">**MERGE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Capacity_Refreshables_E2E**
# 
# ##### Source:
# 
# **Files** from FUAM_Lakehouse folder **bronze_file_location** variable
# 
# ##### Target:
# 
# **3 Delta tables** in FUAM_Lakehouse 
# - **gold_days_table_name** variable value
# - **gold_times_table_name** variable value
# - **gold_details_table_name** variable value


# CELL ********************

from pyspark.sql.functions import col, explode, to_date, date_format, lit, to_timestamp, unix_timestamp
from delta.tables import *
import pyspark.sql.functions as f
import datetime

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
bronze_file_location = f"Files/raw/refreshables/"

gold_main_table_name = "capacity_refreshables"
gold_main_table_name_with_prefix = f"Tables/{gold_main_table_name}"

gold_days_table_name = "capacity_refreshable_days"
gold_days_table_name_with_prefix = f"Tables/{gold_days_table_name}"

gold_times_table_name = "capacity_refreshable_times"
gold_times_table_name_with_prefix = f"Tables/{gold_times_table_name}"

gold_summary_table_name = "capacity_refreshable_summaries"
gold_summary_table_name_with_prefix = f"Tables/{gold_summary_table_name}"

gold_details_table_name = "capacity_refreshable_details"
gold_details_table_name_with_prefix = f"Tables/{gold_details_table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get bronze data

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

# This prevents the notebook running into an error when no capacity refreshables are existant in the tenant
if exploded_df.count() == 0 :
    notebookutils.notebook.exit("No Capacity Refreshables available")
 

# Extract json objects to tabular form
extracted_df = exploded_df.select(col("d.*")).alias("d")

#display(extracted_df)

# Convert key(s) to upper case
extracted_df = extracted_df.withColumn("CapacityId", f.upper(f.col("d.capacity.id")))
extracted_df = extracted_df.withColumn("WorkspaceId", f.upper(f.col("d.group.id")))
extracted_df = extracted_df.withColumn("ItemId", f.upper(f.col("id")))

# Drop unneccessary columns
extracted_df = extracted_df.drop("capacity")
extracted_df = extracted_df.drop("group")

# Drop duplicates
extracted_df = extracted_df.dropDuplicates()

if display_data:
    display(extracted_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract main attributes

# CELL ********************

# Select main columns
silver_main_df = extracted_df.select(
     col("CapacityId")    
    ,col("WorkspaceId")
    ,col("ItemId")
    ,col("d.name").alias("ItemName")
    ,col("d.refreshSchedule.enabled").alias("IsRefreshEnabled")
    ,col("d.refreshSchedule.localTimeZoneId").alias("LocalTimeZoneId")
    ,col("kind").alias("Kind")
    ).dropDuplicates()

if display_data:
    display(silver_main_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge main
# Check if gold table exists
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_main_table_name):
    # if exists -> MERGE to gold
    print("Gold table exists and will be merged.")

    gold_main_df = DeltaTable.forPath(spark, gold_main_table_name_with_prefix)
    # Merge silver (s = source) to gold (t = target)
    gold_main_df.alias('t') \
    .merge(
        silver_main_df.alias('s'),
        "s.WorkspaceId = t.WorkspaceId AND s.ItemId = t.ItemId"
    ) \
    .whenMatchedUpdate(set=
        {
            "CapacityId": "s.CapacityId",
            "ItemName": "s.ItemName",
            "IsRefreshEnabled": "s.IsRefreshEnabled",
            "LocalTimeZoneId": "s.LocalTimeZoneId",
            "Kind": "s.Kind"
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    #.whenNotMatchedBySourceDelete() \
    

else:
    # else -> INSERT to gold
    print("Gold table will be created.")

    silver_main_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_main_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract Days

# CELL ********************

# Extract days
silver_days_df = extracted_df.select(
     col("CapacityId")    
    ,col("WorkspaceId")
    ,col("ItemId")
    ,col("d.refreshSchedule.days").alias("Days")
    )

silver_days_df = silver_days_df.select(
      col("CapacityId")
     ,col("WorkspaceId")
     ,col("ItemId")
     ,explode('Days').alias('Day')
) \
.dropDuplicates()

if display_data:
    display(silver_days_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge days
# Check if gold table exists
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_days_table_name):
    # if exists -> MERGE to gold
    print("Gold table exists and will be merged.")

    gold_days_df = DeltaTable.forPath(spark, gold_days_table_name_with_prefix)
    # Merge silver (s = source) to gold (t = target)
    gold_days_df.alias('t') \
    .merge(
        silver_days_df.alias('s'),
        "s.WorkspaceId = t.WorkspaceId AND s.ItemId = t.ItemId AND s.Day = t.Day"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    # .whenNotMatchedBySourceDelete() \

else:
    # else -> INSERT to gold
    print("Gold table will be created.")

    silver_days_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_days_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract Times

# CELL ********************

# Extract times
silver_times_df = extracted_df \
.select(
     col("CapacityId")    
    ,col("WorkspaceId")
    ,col("ItemId")
    ,col("d.refreshSchedule.times").alias("Times")
    )

silver_times_df = silver_times_df.select(
      col("CapacityId")
     ,col("WorkspaceId")
     ,col("ItemId")
     ,explode('Times').alias('Time')
) \
.dropDuplicates()

if display_data:
    display(silver_times_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge Times
# Check if gold table exists
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_times_table_name):
    # if exists -> MERGE to gold
    print("Gold table exists and will be merged.")

    gold_times_df = DeltaTable.forPath(spark, gold_times_table_name_with_prefix)
    # Merge silver (s = source) to gold (t = target)
    gold_times_df.alias('t') \
    .merge(
        silver_times_df.alias('s'),
        "s.WorkspaceId = t.WorkspaceId AND s.ItemId = t.ItemId AND s.Time = t.Time"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # else -> INSERT to gold
    print("Gold table will be created.")

    silver_times_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_times_table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract refresh summary & details

# CELL ********************

# Select refresh summary
silver_summary_df = extracted_df.select(
     col("CapacityId")    
    ,col("WorkspaceId")
    ,col("ItemId")
    ,col("d.name").alias("ItemName")

    ,date_format("startTime","yyyyMMdd").alias("ConsideredStartDateKey")

    ,to_date(col("startTime").substr(1,10), "yyyy-MM-dd").alias("ConsideredStartDate")
    ,to_date(col("endTime").substr(1,10), "yyyy-MM-dd").alias("ConsideredEndDate")

    ,to_timestamp(col("startTime")).alias("ConsideredStartTime")
    ,to_timestamp(col("endTime")).alias("ConsideredEndTime")


    ,col("averageDuration").alias("RefreshAverageDuration")
    ,col("medianDuration").alias("RefreshMedianDuration")
    ,col("refreshCount").alias("RefreshCount")
    )

# Filter data
silver_summary_df = silver_summary_df.where(col("StartTime").isNotNull())

# Filter data with wrong end date (This can happen in case of new schedules and will be automatically fixed with future run)
silver_summary_df = silver_summary_df.where(col("ConsideredEndDate") != "0001-01-01")


# Calculate duration
silver_summary_df = silver_summary_df.withColumn('ConsiderationDurationSeconds', (unix_timestamp(col("ConsideredEndTime")) - unix_timestamp(col("ConsideredStartTime"))))


if display_data:
    display(silver_summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge summary
# Check if gold table exists
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_summary_table_name):
    # if exists -> MERGE to gold
    print("Gold table exists and will be merged.")

    gold_summary_df = DeltaTable.forPath(spark, gold_summary_table_name_with_prefix)
    # Merge silver (s = source) to gold (t = target)
    gold_summary_df.alias('t') \
    .merge(
        silver_summary_df.alias('s'),
        "s.WorkspaceId = t.WorkspaceId AND s.ItemId = t.ItemId AND s.ConsideredStartTime = t.ConsideredStartTime AND s.ConsideredEndTime = t.ConsideredEndTime"
    ) \
    .whenMatchedUpdate(set=
        {
            "CapacityId": "s.CapacityId",
            "RefreshAverageDuration": "s.RefreshAverageDuration",
            "RefreshMedianDuration": "s.RefreshMedianDuration",
            "RefreshCount": "s.RefreshCount",
            "ConsiderationDurationSeconds": "s.ConsiderationDurationSeconds"
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # else -> INSERT to gold
    print("Gold table will be created.")

    silver_summary_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_summary_table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select last refresh details
silver_details_df = extracted_df.select(
     col("CapacityId")    
    ,col("WorkspaceId")
    ,col("ItemId")
    ,col("d.name").alias("ItemName")

    ,col("d.lastRefresh.requestId").alias("RequestId")
    ,date_format("d.lastRefresh.startTime","yyyyMMdd").alias("LastRefreshStartDateKey")

    ,col("d.lastRefresh.status").alias("Status")
    ,col("d.lastRefresh.refreshType").alias("RefreshType")

    ,to_date(col("d.lastRefresh.startTime").substr(1,10), "yyyy-MM-dd").alias("LastRefreshStartDate")
    ,to_date(col("d.lastRefresh.endTime").substr(1,10), "yyyy-MM-dd").alias("LastRefreshEndDate")

    ,to_timestamp(col("d.lastRefresh.startTime")).alias("LastRefreshStartTime")
    ,to_timestamp(col("d.lastRefresh.endTime")).alias("LastRefreshEndTime")

    ,date_format("d.lastRefresh.startTime","H").alias("LastRefreshStartHour")
    ,date_format("d.lastRefresh.endTime","H").alias("LastRefreshEndHour")
    )

# Filter data
silver_details_df = silver_details_df.where(col("RequestId").isNotNull())

# Filter duplicates
silver_details_df = silver_details_df.dropDuplicates()

# Calculate duration
silver_details_df = silver_details_df.withColumn('DurationInSeconds', ( unix_timestamp(col("LastRefreshEndTime")) - unix_timestamp(col("LastRefreshStartTime")) )   )


if display_data:
    display(silver_details_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge details
# Check if gold table exists
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_details_table_name):
    # if exists -> MERGE to gold
    print("Gold table exists and will be merged.")

    gold_details_df = DeltaTable.forPath(spark, gold_details_table_name_with_prefix)
    # Merge silver (s = source) to gold (t = target)
    gold_details_df.alias('t') \
    .merge(
        silver_details_df.alias('s'),
        "s.WorkspaceId = t.WorkspaceId AND s.ItemId = t.ItemId AND s.RequestId = t.RequestId"
    ) \
    .whenMatchedUpdate(set=
        {
            "CapacityId": "s.CapacityId",
            "DurationInSeconds": "s.DurationInSeconds"
        }
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    # else -> INSERT to gold
    print("Gold table will be created.")

    silver_details_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_details_table_name)


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
