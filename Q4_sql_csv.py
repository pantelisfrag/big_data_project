from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q4 SQL CSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#καταγραφη χρονου
start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q4/Q4_SQL_csv_{job_id}"

#διαβαζω σε df
trips_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
    header=True,
    inferSchema=True
).select(
    col("VendorID").alias("vendor_id"),
    col("tpep_pickup_datetime").alias("pickup_time")
)

#temporary view για SQL queries
trips_df.createOrReplaceTempView("trips")

query = """
SELECT 
    vendor_id,
    COUNT(*) AS night_trips
FROM (
    SELECT 
        vendor_id,
        hour(pickup_time) AS pickup_hour
    FROM trips
    WHERE hour(pickup_time) >= 23 OR hour(pickup_time) < 7
)
GROUP BY vendor_id
ORDER BY night_trips DESC
"""

night_trips = spark.sql(query)

# Αποθήκευση αποτελεσμάτων
night_trips.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_dir)

# Εκτύπωση αποτελεσμάτων
print("\n=== Nighttime Trips by Vendor (23:00-07:00) ===")
night_trips.show(truncate=False)

# Τέλος καταγραφής χρόνου
end_time = time.time()
execution_time = end_time - start_time

print(f"\nExecution Time: {execution_time:.2f} seconds")
print(f"Results saved to: {output_dir}")
spark.stop()