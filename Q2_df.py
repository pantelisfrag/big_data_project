from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt, pow, sin, cos, asin, radians, lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import time

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q2 DF") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Αρχη καταγραφης χρονου εκτελεσης
start_time = time.time()
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q2/Q2_df_{job_id}"

#φορτωση
df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv", header=True)

valid_trips = df.select(
    "VendorID",
    "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude",
    "tpep_pickup_datetime", "tpep_dropoff_datetime"
).filter(
    (col("pickup_longitude").isNotNull()) &
    (col("pickup_latitude").isNotNull()) &
    (col("dropoff_longitude").isNotNull()) &
    (col("dropoff_latitude").isNotNull()) &
    (col("pickup_longitude").between(-180, 180)) &
    (col("pickup_latitude").between(-90, 90)) &
    (col("dropoff_longitude").between(-180, 180)) &
    (col("dropoff_latitude").between(-90, 90)) &
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull())
)

#placeholder
#υπολογισμος Haversine distance using built in functions
#γινεται bind στο valid_trips αργοτερα και εκτελειται πανω σε αυτο το dataframe
distance_expr = lit(6371) * lit(2) * asin(
    sqrt(
        pow(sin(radians(col("dropoff_latitude") - col("pickup_latitude")) / lit(2)), lit(2)) +
        cos(radians(col("pickup_latitude"))) * 
        cos(radians(col("dropoff_latitude"))) * 
        pow(sin(radians(col("dropoff_longitude") - col("pickup_longitude")) / lit(2)), lit(2))
    )
)

#placeholder
#υπολογισμος δαιρκειας ταξιδιου σε λεπτα
#γινεται bind στο valid_trips αργοτερα και εκτελειται πανω σε αυτο το dataframe
duration_expr = (
    unix_timestamp(col("tpep_dropoff_datetime")) - 
    unix_timestamp(col("tpep_pickup_datetime"))
) / lit(60)

# στηνεται το τελικο df με τα ζητουμενα features
trips_with_metrics = valid_trips.select(
    col("VendorID"),
    distance_expr.alias("distance_km"),
    duration_expr.alias("duration_minutes"),
    col("tpep_pickup_datetime").alias("pickup_time"),
    col("tpep_dropoff_datetime").alias("dropoff_time")
)

#ομαδοποιώ ανα feature VendorID
# ταξινομω σε φθινουσα ανα feature distance_km
#αρα εχω πανω πανω το μεγαλυτερο distance
window_spec = Window.partitionBy("VendorID").orderBy(col("distance_km").desc())

#δημιουργια τελικου df
#κανω αριθμιση γραμμών ξεχωριστα για καθε vendorID π.χ.
#αρα κρατάω το row_num == 1 ΑΝΑ VendoID
#
# VendorID	distance_km	duration_minutes	row_num
#   1	        15	    50	           1  <--- το κρατάω
#   1	        5	    2	           2
#   2	        20	    5	           1  <--- το κρατάω
#   2	        15	    3	           2
#
#μετά droparw την  στήλη row_num
max_distance_trips = trips_with_metrics.withColumn(
    "row_num", row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")

#αποθηκευση εξοδου
max_distance_trips.write \
    .option("header", "true") \
    .csv(output_dir)


#Τελος καταγραφης χρονου εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

#εφμανιση
print("############################################")
print("\n=== Trips with Maximum Distance per Vendor ===")
max_distance_trips.show(5, truncate=False)
print(f"Execution Time: {execution_time:.2f} seconds")  #εμφανιση χρονου εκτελεσης

print(f"Full results saved to HDFS: {output_dir}")
print("############################################")

spark.stop()