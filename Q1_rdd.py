from pyspark.sql import SparkSession
from datetime import datetime
import time

username = "pfragkoulakis"
sc = SparkSession \
    .builder \
    .appName("Q1 RDD") \
    .getOrCreate() \
    .sparkContext

sc.setLogLevel("ERROR")


job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q1/Q1_rdd_{job_id}"

# parts[5] is pickup_longitude 
# parts[6] is pickup_latitude
def is_valid(line):
    try:
        parts = line.split(",")
        lon = float(parts[5])
        lat = float(parts[6])
        return lon != 0 and lat != 0
    except:
        return False
    
def parse_hour_and_location(line):
    parts = line.split(",")
    try:
        hour = datetime.strptime(parts[1], "%Y-%m-%d %H:%M:%S").hour
        lon = float(parts[5])
        lat = float(parts[6])
        return (hour, (lon, lat))
    except:
        return None

# Αρχη καταγραφης χρονου εκτελεσης
start_time = time.time()

# φορτωση δεδομενων
raw_trip_data = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv") #rdd

# διαχωρισμος δεδομενων απο features
header = raw_trip_data.first()
data = raw_trip_data.filter(lambda line: line != header)


# κρατάω γραμμές με μη ηδενικο lon και lat
filtered_data = data.filter(is_valid)
# μετατροπή απο μορφή
# "1,2015-01-15 14:30:00,3.5,-73.992,40.750,1.2,1.4"
# σε
# (14, (-73.992, 40.750))

hour_location_pairs = filtered_data.map(parse_hour_and_location).filter(lambda x: x is not None)

#υπολογισμος μεσης τιμης ανα ωρα 
#map ανα ωρ (key ειναι η ωρα)
#αυξουσα ταξινομιση ανα ωρα
hourly_avg = hour_location_pairs \
    .groupByKey() \
    .mapValues(lambda locations: (
        sum(lon for lon, lat in locations) / len(locations),
        sum(lat for lon, lat in locations) / len(locations)
    )) \
    .sortByKey()

# Αποθηκευση
# x[0] ωρα
# x[1][0] lon
# x[1][1] lat
hourly_avg.map(lambda x: f"{x[0]},{x[1][0]:.6f},{x[1][1]:.6f}").coalesce(1).saveAsTextFile(output_dir)

#Τελος καταγραφης χρονου εκτελεσης
end_time = time.time()
execution_time = end_time - start_time

#εμφανιση αποτελεσματων στα logs
print("############################################")
print("Hourly Averages (Hour, Longitude, Latitude):")
for hour, (avg_lon, avg_lat) in hourly_avg.collect():
    print(f"Hour {hour}: Lon={avg_lon:.6f}, Lat={avg_lat:.6f}")

print("Saved to:", output_dir)
print(f"Execution Time: {execution_time:.2f} seconds")  # Εμφάνιση χρόνου εκτέλεσης
print("############################################")