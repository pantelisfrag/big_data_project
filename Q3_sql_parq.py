from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q3 SQL PARQ") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q3/Q3_SQL_parq_{job_id}"

#δεδομενα ταξιδιων
trips_df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/pfragkoulakis/data/parquet/yellow_tripdata_2024.parquet"
).select(
    col("PULocationID").cast("integer").alias("PULocationID"),
    col("DOLocationID").cast("integer").alias("DOLocationID")
)

#δεδομενα ζωνωνω
zones_df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/pfragkoulakis/data/parquet/taxi_zone_lookup.parquet"
).select(
    col("LocationID").cast("integer").alias("LocationID"),
    col("Borough")
)



################!!!
trips_df.createOrReplaceTempView("trips")
zones_df.createOrReplaceTempView("zones")

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

#αποθηκευησ parquet
final_result.write \
    .mode("overwrite") \
    .parquet(output_dir)

#τελος καταγραφης χορνου εκτελεσεης
end_time = time.time()
execution_time = end_time - start_time

#cout
print("############################################")
print("\n=== Trips count inside the same Borough (SQL API with Parquet) ===")
final_result.show(5, truncate=False)
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Full results saved to HDFS: {output_dir} (as Parquet)")
print("############################################")

spark.stop()