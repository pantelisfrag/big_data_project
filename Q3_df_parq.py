from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
from pyspark.sql.types import IntegerType
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q3 DF PARQUET") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#εναρξη καταγραφης
start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q3/Q3_df_parq_{job_id}"

#διαβασμα απο parquet
trips_df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/pfragkoulakis/data/parquet/yellow_tripdata_2024.parquet"
).select(
    col("PULocationID").cast(IntegerType()).alias("PULocationID"),
    col("DOLocationID").cast(IntegerType()).alias("DOLocationID")
)

#διαβασμα ζωνων
zones_df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/user/pfragkoulakis/data/parquet/taxi_zone_lookup.parquet"
).select(
    col("LocationID").cast(IntegerType()).alias("LocationID"),
    col("Borough")
)

trips_with_boroughs = trips_df.join(
    zones_df.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Borough", "PU_Borough"),
    on="PULocationID",
    how="inner"
).join(
    zones_df.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Borough", "DO_Borough"),
    on="DOLocationID",
    how="inner"
)

same_borough_trips = trips_with_boroughs.filter(
    col("PU_Borough") == col("DO_Borough")
)

final_result = same_borough_trips.groupBy("PU_Borough").agg(
    count("*").alias("total_trips")
).orderBy(desc("total_trips")).withColumnRenamed("PU_Borough", "Borough")

# αποθηκευση parquet
final_result.write \
    .mode("overwrite") \
    .parquet(output_dir)

# τελος καταγραφης χρονυο εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

print("############################################")
print("\n=== Trips count inside the same Borough ===")
final_result.show(5, truncate=False)
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Full results saved to HDFS: {output_dir} (as Parquet)")
print("############################################")

spark.stop()