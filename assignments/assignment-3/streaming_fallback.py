import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, desc
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Schema
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("vehicle_count", IntegerType()) \
    .add("average_speed", DoubleType()) \
    .add("congestion_level", StringType())

# Create Spark session
spark = SparkSession.builder \
    .appName("TrafficMonitoringFull") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# InfluxDB Client - Option-fix
influx_client = InfluxDBClient(
    url="http://localhost:8086",
    token="g6QijRA4AJ9KDRkv7nEX0qKBf-DvS2cqYDVEpBJ4ZT1OFtD7obXFD8GCI-ETIgtE6IgPBPmjriDFOl_CJuHf3A==",
    org="Primary"
)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

while True:
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "traffic_data") \
        .option("startingOffsets", "earliest") \
        .load()

    if df.rdd.isEmpty():
        print("No new data. Waiting...")
    else:
        parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*")

        # Data Quality Checks
        parsed = parsed.filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
        parsed = parsed.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))
        parsed = parsed.dropDuplicates(["sensor_id", "timestamp"])

        # Task 1: Real-Time Traffic Volume Per Sensor
        traffic_volume = parsed.groupBy("sensor_id") \
            .sum("vehicle_count") \
            .withColumnRenamed("sum(vehicle_count)", "total_vehicles")
        print("\n Task 1: Traffic Volume Per Sensor:")
        traffic_volume.show(truncate=False)

        # Task 2: Congestion Hotspot Detection
        congestion_hotspots = parsed.filter(col("congestion_level") == "HIGH")
        print("\n Task 2: Congestion Hotspots:")
        congestion_hotspots.show(truncate=False)

        # Task 3: Average Speed per Sensor
        avg_speed = parsed.groupBy("sensor_id") \
            .avg("average_speed") \
            .withColumnRenamed("avg(average_speed)", "avg_speed")
        print("\n Task 3: Average Speed per Sensor:")
        avg_speed.show(truncate=False)

        # Send to Kafka (traffic_analysis)
        merged = traffic_volume.join(avg_speed, on="sensor_id")
        for row in merged.collect():
            message = {
                "sensor_id": row["sensor_id"],
                "total_count": int(row["total_vehicles"]),
                "avg_speed": round(row["avg_speed"], 2)
            }
            producer.send("traffic_analysis", value=message)

            # Send to InfluxDB (traffic_metrics)
            point = Point("traffic_analysis") \
                .tag("sensor_id", row["sensor_id"]) \
                .field("total_count", int(row["total_vehicles"])) \
                .field("avg_speed", float(row["avg_speed"])) \
                .time(time.time_ns())

            write_api.write(bucket="traffic_metrics", org="Primary", record=point)

        # Task 4: Anomaly Detection (Speed Drop Below 20)
        anomalies = parsed.filter(col("average_speed") < 20)
        print("\n Task 4: Anomalies - Speed Drop Detected:")
        anomalies.show(truncate=False)

        # Task 5: Busiest Sensors (Highest Vehicle Count)
        busiest = parsed.groupBy("sensor_id") \
            .sum("vehicle_count") \
            .withColumnRenamed("sum(vehicle_count)", "total_vehicles") \
            .orderBy(desc("total_vehicles")) \
            .limit(5)
        print("\n Task 5: Busiest Sensors (Top 5 by Volume):")
        busiest.show(truncate=False)

    time.sleep(5)
