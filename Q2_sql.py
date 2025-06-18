from pyspark.sql import SparkSession

username = "pfragkoulakis"
spark = SparkSession.builder \
    .appName("Q2 SQL API") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/answers/q2/Q2_sql_{job_id}"

df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv", header=True)

df.createOrReplaceTempView("trips")

result = spark.sql("""
    WITH valid_trips AS (
        SELECT 
            VendorID,
            pickup_longitude, pickup_latitude,
            dropoff_longitude, dropoff_latitude,
            tpep_pickup_datetime, tpep_dropoff_datetime
        FROM trips
        WHERE pickup_longitude IS NOT NULL
          AND pickup_latitude IS NOT NULL
          AND dropoff_longitude IS NOT NULL
          AND dropoff_latitude IS NOT NULL
          AND pickup_longitude BETWEEN -180 AND 180
          AND pickup_latitude BETWEEN -90 AND 90
          AND dropoff_longitude BETWEEN -180 AND 180
          AND dropoff_latitude BETWEEN -90 AND 90
          AND tpep_pickup_datetime IS NOT NULL
          AND tpep_dropoff_datetime IS NOT NULL
    ),
    trip_metrics AS (
        SELECT 
            VendorID,
            -- Calculate Haversine distance in km
            6371 * 2 * ASIN(SQRT(
                POWER(SIN(RADIANS(dropoff_latitude - pickup_latitude)/2), 2) +
                COS(RADIANS(pickup_latitude)) * 
                COS(RADIANS(dropoff_latitude)) * 
                POWER(SIN(RADIANS(dropoff_longitude - pickup_longitude)/2), 2)
            )) AS distance_km,
            -- Calculate duration in minutes
            (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime))/60 AS duration_minutes,
            tpep_pickup_datetime AS pickup_time,
            tpep_dropoff_datetime AS dropoff_time
        FROM valid_trips
    ),
    ranked_trips AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY VendorID ORDER BY distance_km DESC) as rank
        FROM trip_metrics
    )
    SELECT 
        VendorID, 
        ROUND(distance_km, 2) AS distance_km,
        ROUND(duration_minutes, 2) AS duration_minutes,
        pickup_time,
        dropoff_time
    FROM ranked_trips
    WHERE rank = 1
""")

result.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_dir)

print("\n=== Trips with Maximum Distance per Vendor (SQL API) ===")
result.show(5, truncate=False)

print(f"Full results saved to HDFS: {output_dir}")
spark.stop()