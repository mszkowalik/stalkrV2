from kafka import KafkaConsumer
import json
from pymongo import MongoClient

# Establish MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27018/')
db = mongo_client['processed_data_db']
processed_data_collection = db['processed_data']

# Establish Kafka Consumer
consumer = KafkaConsumer(
    'user_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start reading at the earliest message
    enable_auto_commit=True,       # Enable automatic offset committing
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def process_user_data(user_data):
    # Placeholder for actual data processing logic
    print(f"Processing data for user_id: {user_data['user_id']}")
    # Simulate processing by appending '_processed' to the data
    user_data['data'] += '_processed'
    return user_data

def save_processed_data(processed_data):
    # Save processed data to MongoDB
    # processed_data_collection.insert_one(processed_data)
    print(f"Saved processed data for user_id: {processed_data['user_id']}")

def main():
    for message in consumer:
        user_data = message.value
        processed_data = process_user_data(user_data)
        save_processed_data(processed_data)

if __name__ == '__main__':
    main()
