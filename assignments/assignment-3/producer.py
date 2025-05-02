import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sensor IDs
sensor_ids = ['S101', 'S102', 'S103', 'S104', 'S105']

def generate_event(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(10, 100), 2),
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"])
    }

# Infinite loop to send messages every second
try:
    while True:
        for sensor in sensor_ids:
            event = generate_event(sensor)
            print("Sending:", event)
            producer.send("traffic_data", value=event)
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped by user")
