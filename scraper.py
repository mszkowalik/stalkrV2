from kafka import KafkaProducer
import json
from pymongo import MongoClient
import concurrent.futures
from datetime import datetime

# Establish MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27018/')
db = mongo_client['location_db']
locations_collection = db['locations']

# Establish Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_locations():
    return list([
        {"_id": "location1"},
        {"_id": "location2"}])
    return list(locations_collection.find())

def query_data(location, query):
    # Placeholder for actual query logic
    # Assume each query returns user data for processing
    print(f"Querying data for location {location['_id']} with query {query}")
    return {"user_id": f"user_{query}", "data": f"data_from_{location['_id']}_{query}"}

def produce_user_data(user_data):
    producer.send('user_topic', user_data)
    print(f"Produced {user_data} to Kafka")

def scrape_location_data(location):
    print(f"Scraping data for location {location}")
    batch_id = datetime.utcnow().isoformat()
    queries = [f"query_{i}" for i in range(100)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(lambda query: query_data(location, query), queries)
    for result in results:
        result['batch_id'] = batch_id
        produce_user_data(result)

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        locations = fetch_locations()
        executor.map(scrape_location_data, locations)
