from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q5 DF PARQ") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Καταγραφή χρόνου εκκίνησης
start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q5/Q5_df_parq_{job_id}"

trips_df = spark.read.parquet(
    f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2024.parquet"
).select(
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id")
)

zones_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
).select(
    col("LocationID").alias("location_id"),
    col("Zone").alias("zone_name")
)

#Join trips & zone names pickup
trips_with_pickup_zone = trips_df.join(
    zones_df,
    trips_df.pickup_location_id == zones_df.location_id,
    "inner"
).select(
    col("pickup_location_id"),
    col("dropoff_location_id"),
    col("zone_name").alias("pickup_zone")
)

#Join trips & zone names dropoff
trips_with_both_zones = trips_with_pickup_zone.join(
    zones_df,
    trips_with_pickup_zone.dropoff_location_id == zones_df.location_id,
    "inner"
).select(
    col("pickup_zone"),
    col("zone_name").alias("dropoff_zone")
)

#Αφαιρω οπου το PU & DO zone ειναι ιδιο...
different_zone_trips = trips_with_both_zones.filter(
    col("pickup_zone") != col("dropoff_zone")
)

#Πληθος trips για ζευγαρια ζωνων
zone_pair_counts = different_zone_trips.groupBy(
    "pickup_zone", "dropoff_zone"
).agg(
    count("*").alias("total_trips")
).orderBy(
    desc("total_trips")
)

zone_pair_counts.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_dir)
    
# Τέλος καταγραφής χρόνου
end_time = time.time()
execution_time = end_time - start_time

print("############################################")
print(f"\nExecution Time: {execution_time:.2f} seconds")
zone_pair_counts.show(5, truncate=False)
print(f"Results saved to: {output_dir}")
print("############################################")

spark.stop()