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
# META       "default_lakehouse_workspace_id": "ee9e7fa6-49a3-4561-afc8-41b001d5bf5b"
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import explode, sequence, to_date
from pyspark.sql.functions import col, year, month, dayofmonth, weekofyear, date_format, to_date, expr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameter
beginDate = '2024-01-01'
endDate = '2030-12-31'
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_df = df.select(
    date_format("date","yyyyMMdd").alias("DateKey"),
    date_format("date","yyyy-MM-dd").alias("Date"),
    col("date").alias("Date2Key"),
    year("date").alias("Year"),
    month("date").alias("Month"),
    dayofmonth("date").alias("Day"),
    weekofyear("date").alias("WeekOfYear"),
    date_format("date","yyyy-MM").alias("YearMonth"),
    date_format("date", "E").alias("DayOfWeek")
)

date_df = date_df.createOrReplaceTempView('calendar_temp')

query = """
    SELECT 
        *
        ,DAYOFWEEK(date) AS DayOfWeekNum
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE()) ) THEN 1 ELSE 0 END  AS IsCurrentYear
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE())-1 ) THEN 1 ELSE 0 END  AS IsPreviousYear
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE()) AND QUARTER(date) = QUARTER(CURRENT_DATE()) ) THEN 1 ELSE 0 END  AS IsCurrentQuarter
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE()) ) THEN 1 ELSE 0 END  AS IsCurrentMonth
        ,CASE WHEN ( DATE_FORMAT(date, 'yyyy-MM') = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM') ) THEN 1 ELSE 0 END  AS IsPreviousMonth
        ,CASE WHEN ( date BETWEEN DATE_ADD(CURRENT_DATE(), -14) AND CURRENT_DATE() ) THEN 1 ELSE 0 END  AS IsInLast14Days
        ,CASE WHEN ( date BETWEEN DATE_ADD(CURRENT_DATE(), -30) AND CURRENT_DATE() ) THEN 1 ELSE 0 END  AS IsInLast30Days
    FROM calendar_temp
"""

final_date_df = spark.sql(query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(final_date_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the DataFrame to the lakehouse
final_date_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
