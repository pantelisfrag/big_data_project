from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q1 DF") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q1/Q1_df_{job_id}"

start_time = time.time()

# φορτωση csv
df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv", header=True)

# κραταω γραμμες με μη μηδενικο lon και lat
# εξαγω την ωρα, μετονομαζω στηλη σε hour
#μετατρεπω τα cords σε numeric values 
clean_df = df.filter(
    (col("pickup_longitude") != 0) &
    (col("pickup_latitude") != 0)
).select(
    hour(col("tpep_pickup_datetime")).alias("hour"),
    col("pickup_longitude").cast("double").alias("lon"),
    col("pickup_latitude").cast("double").alias("lat")
)

# υπολογιζω μεση τιμη ανα ωρα
hourly_avg = clean_df.groupBy("hour") \
    .agg(
        avg("lon").alias("avg_lon"), 
        avg("lat").alias("avg_lat")
    ) \
    .orderBy("hour")

#αποθηκευση εξοδου
hourly_avg.write \
    .option("header", "true") \
    .csv(output_dir)

#Τελος καταγραφης χρονου εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

# εμφανιση αποτελεσματων στα logs
print("############################################")
print(" Hourly Averages (Hour, Longitude, Latitude):")
hourly_avg.show(24, truncate=False)
print("Saved to:", output_dir)
print(f"Execution Time: {execution_time:.2f} seconds")
print("############################################")
spark.stop()