import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage
import json

# Define the project ID, topic, subscription names, and bucket name
project_id = "datastorage-422521"
subscription_name = "archivesub"
bucket_name = "bus_bucket1"

# Create a Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Create a Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Callback function to handle received messages
def callback(message):
    print(f"Received message: {message.data}")
    try:
        # Parse message data
        data = json.loads(message.data)
        # Create a unique filename based on current timestamp
        filename = f"data_{int(time.time())}.json"
        # Upload data to Google Cloud Storage
        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data))
        print(f"Data uploaded to {bucket_name}/{filename}")
        # Acknowledge the message to remove it from the queue
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")

# Subscribe to the topic
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the main thread alive to listen for messages
try:
    streaming_pull_future.result(timeout=60)  # Timeout in seconds
except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()

# Close the subscriber client
subscriber.close()
