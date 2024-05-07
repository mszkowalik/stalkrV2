from kafka import KafkaConsumer, TopicPartition
import json
from pymongo import MongoClient
import logging

# Establish MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27018/')
db = mongo_client['processed_data_db']
processed_data_collection = db['processed_data']

# Establish Kafka Consumer
localize_consumer = KafkaConsumer(
    'user_topic',
    group_id='localize_group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start reading at the latest message
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def localize_user(user_data):
    # Placeholder for actual data processing logic
    profileId = user_data.get("profileId")
    # profileId = ""
    # print(f"Processing data for profileId: {profileId}")
    profiles = user_data.get("data")
    # print(len(profiles))
    return user_data

def save_processed_data(processed_data):
    # Save processed data to MongoDB
    # processed_data_collection.insert_one(processed_data)
    profileId = processed_data.get("profileId")
    # print(f"Saved processed data for profileId: {profileId}")

def main():
    count = 0
    print(localize_consumer.beginning_offsets(localize_consumer.assignment()))
    for message in localize_consumer:
        try:
            print("----------------------")
            print(f"Processing message at offset: {message.offset}")
            user_data = message.value
            processed_data = localize_user(user_data)
            save_processed_data(processed_data)
            count += 1
            localize_consumer.commit()
            # print(f"Processed message at offset: {message.offset}")
            print(f"Total messages processed: {count}")
        except Exception as e:
            logging.error(f"Failed to process message {message}: {e}")
        # Optionally commit after each message if auto commit is off


if __name__ == '__main__':
    main()
