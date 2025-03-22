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

# #### Activities
# 
# ##### Data ingestion strategy:
# <mark style="background: lightgreen;">**APPEND**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Activities_E2E**
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

from pyspark.sql.functions import col, explode, to_date, date_format, lit, upper
import pyspark.sql.functions as f
from delta.tables import *
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
bronze_file_location = f"Files/raw/activities/*/"
silver_table_name = "FUAM_Staging_Lakehouse.activities_silver"
gold_table_name = "activities"
gold_table_name_with_prefix = f"Tables/{gold_table_name}"

last_activity_date = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This function converts all complex data types to StringType
def convert_columns_to_string(schema, parent = "", lvl = 0):
    """
    Input:
    - schema: Dataframe schema as StructType
    
    Output: List
    Returns a list of columns in the schema casting them to String to use in a selectExpr Spark function.
    """
    
    lst=[]
    
    for x in schema:
        # check if complex datatype has to be converted to string
        if str(x.dataType) in {"DateType()", "StringType()", "BooleanType()", "LongType()", "IntegerType()", "DoubleType()", "FloatType()"}:
            # no need to convert
            lst.append("{col}".format(col=x.name))
        else:
            # it has to be converted
            # print(str(x.dataType))
            lst.append("cast({col} as string) as {col}".format(col=x.name))

    return lst

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

if display_data:
    display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
exploded_df = bronze_df.select(explode("activityEventEntities").alias("d"))

del bronze_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select all columns (columns are dynamic)
silver_df = exploded_df.select(
    to_date(col("d.CreationTime").substr(1,10), "yyyy-MM-dd").alias("CreationDate"),
    date_format("d.CreationTime","yyyyMMdd").alias("CreationDateKey"),
    date_format("d.CreationTime","H").alias("CreationHour"),
    date_format("d.CreationTime","mm").alias("CreationMinute"),
    col("d.*")
    )
# Put selected ID columns to Upper Case
for co in silver_df.columns:
    if co in ['ActivityId','ArtifactId','CapacityId','DashboardId','DataflowId','DatasetId','DatasourceId','FolderObjectId','GatewayId','Id','ItemId','ReportId','UserId','WorkspaceId','','','','','','','','','',]:
        silver_df = silver_df.withColumn(co, f.upper(silver_df[co]))
del exploded_df

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

# Check if gold table exists 
table_exists = None
if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_table_name):
    table_exists = True
    print("Gold table exists.")
else:
    table_exists = False

# Get latest activity date from silver_df
silver_min_df = silver_df.select(col('CreationDate')).orderBy(col('CreationDate'), ascending=True).first()
silver_last_activity_date = silver_min_df['CreationDate']

# Calculate latest activity date
if table_exists:

    # Get latest activity date from gold table
    get_latest_date_sql = "SELECT CreationDate FROM FUAM_Lakehouse.activities ORDER BY CreationDate DESC LIMIT 1"
    gold_min_df = spark.sql(get_latest_date_sql)
    if gold_min_df.count() == 0:
        # in case there are no records in gold take silver_last_activity_date
        print("No existing records")
        gold_last_activity_date = silver_last_activity_date
    else:
        gold_last_activity_date = gold_min_df.first()['CreationDate']

    if silver_last_activity_date < gold_last_activity_date:
        print("From silver_df")
        last_activity_date = silver_last_activity_date
    else:
        print("From gold")
        last_activity_date = gold_last_activity_date

else:
    print("From silver_df")
    last_activity_date = silver_last_activity_date

print(last_activity_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean delta content from Gold table
if table_exists:
    del_query = f"DELETE FROM FUAM_Lakehouse.{gold_table_name} WHERE CreationDate >= TO_DATE('{last_activity_date}')"
    spark.sql(del_query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter silver_df data based on last activity date
silver_df = silver_df.filter(f.col("CreationDate") >= f.lit(last_activity_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(silver_df)
    # show converted table schema
    print(convert_columns_to_string(silver_df.schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert silver_df's complex data type columns to StringType columns
silver_df_converted = silver_df.selectExpr(convert_columns_to_string(silver_df.schema))
del silver_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared silver_df_converted to gold delta table
silver_df_converted.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write history of bronze files
path = bronze_file_location.replace("*/", '', )
mssparkutils.fs.cp(path, path.replace("Files/raw/", "Files/history/") + datetime.datetime.now().strftime('%Y/%m/%d') + "/", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
