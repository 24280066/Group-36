import random
import time
from kafka import KafkaProducer

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# List of possible actions
actions = ['play', 'skip', 'download']

# List of regions
regions = ['EU', 'US', 'APAC']

# Function to generate events
def generate_event():
    song_id = random.randint(100, 500)  # Random song ID between 100 and 500
    timestamp = time.time()  # Current timestamp
    region = random.choice(regions)  # Random region
    action = random.choice(actions)  # Random action (play, skip, or download)
    
    event = {
        'song_id': song_id,
        'timestamp': timestamp,
        'region': region,
        'action': action
    }
    return event

# Main loop to produce events
try:
    while True:
        event = generate_event()
        print(f"Sent event: {event}")
        producer.send('music_events', value=str(event).encode('utf-8'))
        time.sleep(1)  # Wait for 1 second before sending the next event
except KeyboardInterrupt:
    print("Stopping event producer...")
finally:
    producer.close()