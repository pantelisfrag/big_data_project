from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg
from pyspark.sql.types import IntegerType, BooleanType
from datetime import datetime
import time

username = "pfragkoulakis"

spark = SparkSession.builder \
    .appName("Q1 DF UDF") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# εξοδος path
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q1/Q1_df_udf_{job_id}"

# εναρξη καταγραφης χρονου εκτελεσης
start_time = time.time()

# φορτωση δεδομενων
df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv", header=True)

#UDF
def extract_hour(datetime_str):
    try:
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").hour
    except:
        return None

def is_valid_coord(lon, lat):
    try:
        return float(lon) != 0 and float(lat) != 0
    except:
        return False

#χρειαζεται
extract_hour_udf = udf(extract_hour, IntegerType())
is_valid_coord_udf = udf(is_valid_coord, BooleanType())

# Φιλτραρω μη μηδενικες lon + lat τιμες
# εξαγω την ωρα με την udf που εφτιαξα
clean_df = df.filter(
    is_valid_coord_udf(col("pickup_longitude"), col("pickup_latitude"))
).select(
    extract_hour_udf(col("tpep_pickup_datetime")).alias("hour"),
    col("pickup_longitude").cast("double").alias("lon"),
    col("pickup_latitude").cast("double").alias("lat")
)

# υπολογιζω μεση τιμη ανα ωρα
hourly_avg = clean_df.groupBy("hour") \
    .agg(
        avg("lon").alias("avg_lon"), # Μέσος όρος της στήλης "lon" ανά ώρα
        avg("lat").alias("avg_lat") # Μέσος όρος της στήλης "lat" ανά ώρα
    ) \
    .orderBy("hour")

#αποθηκευω
hourly_avg.write \
    .option("header", "true") \
    .csv(output_dir)

# τελος καταγραφης χρονου εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

#εμφανιση
print("############################################")
print(" Hourly Averages (Hour, Longitude, Latitude):")
hourly_avg.show(24, truncate=False)
print("Saved to:", output_dir)
print(f"Execution Time: {execution_time:.2f} seconds")
print("############################################")
spark.stop()