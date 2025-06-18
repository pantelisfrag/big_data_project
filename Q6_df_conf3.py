from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc
import time


username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q6: Revenue by Borough") \
    .config("spark.executor.instances", "8") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q6/Q6_df_conf3_{job_id}"
 
#καταγραφη
start_time = time.time()

#φορτωση csv
trips_df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",header=True,inferSchema=True)
zones_df = spark.read.csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",header=True,inferSchema=True)

#JOIN 
revenue_by_borough = trips_df.join(
    zones_df,
    trips_df.PULocationID == zones_df.LocationID,
    "inner"
).groupBy("Borough").agg(
    sum("fare_amount").alias("Fare ($)"),
    sum("tip_amount").alias("Tips ($)"),
    sum("tolls_amount").alias("Tolls ($)"),
    sum("extra").alias("Extras ($)"),
    sum("mta_tax").alias("MTA Tax ($)"),
    sum("congestion_surcharge").alias("Congestion ($)"),
    sum("airport_fee").alias("Airport Fee ($)"),
    sum("total_amount").alias("Total Revenue ($)")
).orderBy(desc("Total Revenue ($)"))

#Τέλος καταγραφής χρόνου
end_time = time.time()
execution_time = end_time - start_time

revenue_by_borough.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_dir)
    
print("############################################")
print(f"\nExecution Time: {execution_time:.2f} seconds")
print(f"Results saved to: {output_dir}")
print("############################################")

# Εμφάνιση αποτελεσμάτων
revenue_by_borough.show(truncate=False)