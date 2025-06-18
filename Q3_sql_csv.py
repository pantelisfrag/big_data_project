from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q3 SQL CSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#αρχη καταγραφης
start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q3/Q3_SQL_csv_{job_id}"

#διαβασμα δεδομενων
trips_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
    header=True,
    inferSchema=True
).select(
    col("PULocationID").cast("integer").alias("PULocationID"),
    col("DOLocationID").cast("integer").alias("DOLocationID")
)

zones_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
).select(
    col("LocationID").cast("integer").alias("LocationID"),
    col("Borough")
)

trips_df.createOrReplaceTempView("trips")
zones_df.createOrReplaceTempView("zones")

#SQL Query:
#1 Join για να παρω Borough  για pick-up και drop-off.
#2 φιλτραρω οπου PU_Borough == DO_Borough.
#3 ομαδοποιηση ανα borough
query = """
WITH trips_with_boroughs AS (
    SELECT 
        t.PULocationID,
        t.DOLocationID,
        pu.Borough AS PU_Borough,
        do.Borough AS DO_Borough
    FROM trips t
    JOIN zones pu ON t.PULocationID = pu.LocationID
    JOIN zones do ON t.DOLocationID = do.LocationID
)
SELECT 
    PU_Borough AS Borough,
    COUNT(*) AS total_trips
FROM trips_with_boroughs
WHERE PU_Borough = DO_Borough
GROUP BY PU_Borough
ORDER BY total_trips DESC
"""

final_result = spark.sql(query)

#αποθκευση
final_result.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_dir)

#τελος καταγραφης χορνου εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

#couts
print("############################################")
print("\n=== Trips count inside the same Borough (SQL API) ===")
final_result.show(5, truncate=False)
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Full results saved to HDFS: {output_dir}")
print("############################################")

spark.stop()
