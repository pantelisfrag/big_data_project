from pyspark.sql import SparkSession
from math import radians, sin, cos, sqrt, asin
from datetime import datetime

username = "pfragkoulakis"
sc = SparkSession \
    .builder \
    .appName("Q2 RDD") \
    .getOrCreate() \
    .sparkContext
 
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q2/Q2_rdd_{job_id}"

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 6371 * 2 * asin(sqrt(a))  # km

def calculate_duration(pickup_time, dropoff_time):
    try:
        pickup = datetime.strptime(pickup_time, "%Y-%m-%d %H:%M:%S")
        dropoff = datetime.strptime(dropoff_time, "%Y-%m-%d %H:%M:%S")
        return (dropoff - pickup).total_seconds() / 60  # Duration in minutes
    except:
        return None

def safe_float(x):
    try:
        return float(x)
    except ValueError:
        return None

# Function to find the trip with max distance
def max_distance_trip(a, b):
    return a if a[0] > b[0] else b

# Format and save results
def format_result(vendor_data):
    vendor_id = vendor_data[0]
    max_distance = vendor_data[1][0]
    duration = vendor_data[1][1]
    pickup_time = vendor_data[1][2]
    dropoff_time = vendor_data[1][3]
    return (vendor_id, (max_distance, duration, pickup_time, dropoff_time))

# Load data and take first 10,000 rows
lines = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")
sample_data = lines.take(500)  # Takes first 10,000 rows into driver memory
sample_rdd = sc.parallelize(sample_data)  # Convert back to RDD

# Skip header and split into columns
header = sample_rdd.first()  # Extract header
rdd = sample_rdd.filter(lambda line: line != header) \
    .map(lambda line: line.split(",")) \
    .filter(lambda row: len(row) > 10)  # Ensure row has enough columns

# Helper function to safely convert to float

# Filter valid coordinates and timestamps
valid_trips = rdd.filter(lambda row: (
    safe_float(row[5]) is not None and safe_float(row[6]) is not None and
    safe_float(row[9]) is not None and safe_float(row[10]) is not None and
    -180 <= safe_float(row[5]) <= 180 and -90 <= safe_float(row[6]) <= 90 and
    -180 <= safe_float(row[9]) <= 180 and -90 <= safe_float(row[10]) <= 90 and
    calculate_duration(row[1], row[2]) is not None  # Valid duration
))

# Compute distances and include duration
trips_with_metrics = valid_trips.map(lambda row: (
    row[0],  # VendorID
    (
        haversine(
            safe_float(row[5]), safe_float(row[6]),  # pickup_lon, pickup_lat
            safe_float(row[9]), safe_float(row[10])   # dropoff_lon, dropoff_lat
        ),
        calculate_duration(row[1], row[2]),  # trip duration in minutes
        row[1],  # pickup time (for reference)
        row[2]   # dropoff time (for reference)
    )
))




# Find trip with max distance per VendorID (keeping all metrics)
max_distance_trips = trips_with_metrics.reduceByKey(max_distance_trip)




formatted_results = max_distance_trips.map(format_result)

# Save to HDFS
formatted_results.saveAsTextFile(output_dir)

# Print sample results
print("\n=== Trips with Maximum Distance per Vendor ===")
for result in formatted_results.take(5):
    vendor_id, (distance, duration, pickup, dropoff) = result
    print(f"Vendor {vendor_id}:")
    print(f"  Max Distance: {distance:.2f} km")
    print(f"  Trip Duration: {duration:.2f} minutes")
    print(f"  Pickup Time: {pickup}")
    print(f"  Dropoff Time: {dropoff}\n")

print(f"Full results saved to HDFS: {output_dir}")
sc.stop()