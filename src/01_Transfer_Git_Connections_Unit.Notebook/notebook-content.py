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

# #### Git Connections
# 
# ##### Data ingestion strategy:
# <mark style="background: #D69AFE;">**MERGE**</mark>
# 
# ##### Related pipeline:
# 
# **Load_Git_Connections_E2E**
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

from pyspark.sql.functions import col, explode, to_date, date_format, lit
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
bronze_file_location = f"Files/raw/git_connections/"
silver_table_name = "FUAM_Staging_Lakehouse.git_connections_silver"
gold_table_name = "git_connections"
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

if display_data:
    display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode json subset structure
exploded_df = bronze_df.select(explode("value").alias("d"))

# To prevent error in execution this stops the notebook in case no git connection is available
if exploded_df.count() == 0 :
    notebookutils.notebook.exit("No Git Connections available")

# Select all columns (columns are dynamic)
silver_df = exploded_df.select(
    col("d.*")
    )

# Convert key(s) to upper case
silver_df = silver_df.withColumn("workspaceId", f.upper(f.col("workspaceId")))

# Extract columns
silver_df = silver_df.withColumn("repositoryName", f.col("gitProviderDetails.repositoryName"))
silver_df = silver_df.withColumn("gitProviderType", f.col("gitProviderDetails.gitProviderType"))

# Select needed columns
silver_df = silver_df.select("workspaceId", "repositoryName", "gitProviderType")

if display_data:
    display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# show converted table schema
if display_data:
    convert_columns_to_string(silver_df.schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert silver_df's complex data type columns to StringType columns
silver_df = silver_df.selectExpr(convert_columns_to_string(silver_df.schema)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write prepared bronze_df to silver delta table
silver_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(silver_table_name)

if display_data:
    display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_silver_to_gold(silver_table_name, gold_table_name, ids):
    query = "SELECT * FROM " + silver_table_name
    silver_df = spark.sql(query)
    
    if spark._jsparkSession.catalog().tableExists('FUAM_Lakehouse', gold_table_name):
        # if exists -> MERGE to gold
        print("Gold table exists and will be merged.")

        gold_df = DeltaTable.forPath(spark, gold_table_name_with_prefix)

        gold_columns = gold_df.toDF().columns
        silver_columns = silver_df.columns
        combined_columns = list(set(gold_columns) | set(silver_columns))
        id_cols = {}
        other_cols = {}
        merge_id_stmt = ''
        for col in combined_columns:
            if col in ids:
                merge_id_stmt =  merge_id_stmt +  " t." + col + " = s." + col + " and"
                id_cols[col] = "s." + col
            else:
                other_cols[col] = "s." + col
        # delete last and in merge id statement
        merge_id_stmt = merge_id_stmt[:-4]



        # Merge silver (s = source) to gold (t = target)
        gold_df.alias('t') \
        .merge(silver_df.alias('s'), merge_id_stmt ) \
        .whenMatchedUpdate(set = other_cols
        ) \
        .whenNotMatchedInsert(values = id_cols | other_cols
        ) \
        .execute()

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

# Merge data to gold table
write_silver_to_gold(silver_table_name, gold_table_name, ['workspaceId'])

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
