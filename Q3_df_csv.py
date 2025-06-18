from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum
from pyspark.sql.types import IntegerType, StructType, StructField
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q3 DF CSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#αρχη καταγραφης χρονου εκτελεσης
start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q3/Q3_df_csv_{job_id}"

trips_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
    header=True,
    inferSchema=True
).select(
    col("PULocationID").cast(IntegerType()).alias("PULocationID"),
    col("DOLocationID").cast(IntegerType()).alias("DOLocationID")
)

#Δεδομενα zones, schema...αυτοματα
zones_df = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
).select(
    col("LocationID").cast(IntegerType()).alias("LocationID"),
    col("Borough")
)

#φιλτραρω διαδρομες με ιδια PU/DO boroughs και βρισω το πληθος τους
trips_with_boroughs = trips_df.join(
    zones_df.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Borough", "PU_Borough"),
    on="PULocationID",
    how="inner"
).join(
    zones_df.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Borough", "DO_Borough"),
    on="DOLocationID",
    how="inner"
)
#μορφη trips_with_boroughs 
'''
PULocationID	DOLocationID	PU_Borough	DO_Borough
1 	            4	            Manhattan	Manhattan
1	            7	            Manhattan	Brooklyn
4	            4	            Brooklyn	Brooklyn
'''

#ταξιδια με PU_Borough == DO_Borough
same_borough_trips = trips_with_boroughs.filter(
    col("PU_Borough") == col("DO_Borough")
)

# ομαδοποιηση και μετρημα
final_result = same_borough_trips.groupBy("PU_Borough").agg(
    count("*").alias("total_trips")
).orderBy(desc("total_trips")).withColumnRenamed("PU_Borough", "Borough")

final_result.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_dir)
    
#Τελος καταγραφης χρονου εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

#cout
print("############################################")
print("\n=== Trips count inside the same Borough ===")
final_result.show(5 , truncate=False)
print(f"Execution Time: {execution_time:.2f} seconds")  #εμφανιση χρονου εκτελεσης
print(f"Full results saved to HDFS: {output_dir}")
print("############################################")

spark.stop()