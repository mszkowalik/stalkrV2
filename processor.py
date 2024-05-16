from kafka import KafkaConsumer, TopicPartition
from shapely.geometry import Point
import json
from pymongo import MongoClient
import logging

# Establish MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27018/')
db = mongo_client['processed_data_db']
processed_data_collection = db['processed_data']

# Set up basic configuration to suppress all logging from other modules
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.CRITICAL + 1)

# Create and configure a logger for your specific module
logger = logging.getLogger('stalkrV2')
logger.setLevel(logging.DEBUG)  # Set your logger's level to DEBUG

# Establish Kafka Consumer
localize_consumer = KafkaConsumer(
    'user_topic',
    group_id='localize_group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start reading at the latest message
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def localize_user(user_data):
    # Placeholder for actual data processing logic
    profileId = user_data.get("profileId")
    # profileId = ""
    # print(f"Processing data for profileId: {profileId}")
    profiles = user_data.get("data")

    anchors = [profile.get("anchorPoint") for profile in profiles]
    distances = [profile.get("distanceMeters") for profile in profiles]
    anchor_data = []
    # print(len(profiles))
    return user_data

def save_processed_data(processed_data):
    # Save processed data to MongoDB
    # processed_data_collection.insert_one(processed_data)
    profileId = processed_data.get("profileId")
    print(f"Saved processed data for profileId: {profileId}")

def main():
    for message in localize_consumer:
        try:
            user_data = message.value
            processed_data = localize_user(user_data)
            save_processed_data(processed_data)
        except Exception as e:
            logging.error(f"Failed to process message {message}: {e}")


if __name__ == '__main__':
    main()
