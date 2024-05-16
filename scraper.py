import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import pygeohash as gh
import json
from time import sleep
from pymongo import MongoClient
import concurrent.futures
from datetime import datetime
import logging
from stalkr import GH_PRECISION, generate_points_in_geojson_feature, query_anchor_point
from grindr_access.grindr_user import GrindrUser
import hashlib


# Set up basic configuration to suppress all logging from other modules
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.CRITICAL + 1)

# Create and configure a logger for your specific module
logger = logging.getLogger('stalkrV2')
logger.setLevel(logging.DEBUG+1)  # Set your logger's level to DEBUG

load_dotenv()
# Establish MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27018/')
db = mongo_client['stalkr']
locations_collection = db['locations']

# Establish Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         key_serializer=lambda k: json.dumps(k).encode('utf-8'),
                        #  api_version=(2, 5, 0),
                         acks='all',
                         retries=5)

user = GrindrUser()
mail = os.getenv('GRINDR_MAIL')
password = os.getenv('GRINDR_PASS')

DRY_RUN = 1
SAVE_FILE = 1

def fetch_locations():
    locations = list()
    active_locations = locations_collection.find({'properties.isActive': True})
    for location in active_locations:
        locations.append(location)
    return locations

def produce_user_data(user_data: dict):
    profile_id = str(user_data.get('profileId', ''))
    data = user_data.get('data')
    if data:
        batch_id=str(data[0].get('batch_id', ''))
    key = hashlib.sha256((profile_id + batch_id).encode('utf-8')).hexdigest()
    # Send the message
    future = producer.send(topic='user_topic', value=user_data, key=key)
    # Wait for the send to complete and check for exceptions
    try:
        record_metadata = future.get(timeout=10)  # Adjust timeout as needed
        logger.debug(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
    except Exception as e:
        logger.error(f"Failed to send message for profile_id {profile_id} with key {key}: {e}")
    
    logger.debug(f"Produced {profile_id} to Kafka with key: {key}")

def scrape_location_data(location):
    logger.info(f"Scraping data for location '{location['properties']['name']}'")
    batch_id = datetime.utcnow().isoformat()  # corrected datetime usage
    feature_points = generate_points_in_geojson_feature(location)
    logger.info(f"Generated {len(feature_points)} points for location {location['properties']['name']}")
    # Use ThreadPoolExecutor to parallelize the scraping process
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Directly use process_anchor_point in the executor without a wrapper
        futures = [executor.submit(query_anchor_point, anchor_point, user) for anchor_point in feature_points]
        results = [future.result() for future in futures]
    
    for result in results:
        for user_data in result:
            user_data['batch_id'] = batch_id
    
    profiles = {}
    for result in results:
        for user_data in result:
            profileId = user_data.get('profileId')
            if profileId:
                if profileId not in profiles:
                    profiles[profileId] = {"profileId": profileId, "data": [user_data]}
                else:
                    profiles[profileId]["data"].append(user_data)

    if SAVE_FILE:
        with open(f"results_{location['properties']['name']}.json", "w") as f:
            json.dump(profiles, f)
    else:
        for user_data in profiles.values():
            produce_user_data(user_data)


if __name__ == '__main__':
    if not DRY_RUN:
        logger.info("Logging in...")
        while not user.login(mail, password):
            logger.error("Login failed, retrying in 5 minutes")
            sleep(60*5)

    locations = fetch_locations()
    workers = len(locations)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        if DRY_RUN:
            for location in locations:
                with open(f"results_{location['properties']['name']}.json", "r") as f:
                    profiles = json.load(f)

                for user_data in profiles.values():
                    produce_user_data(user_data)
        else:
            executor.map(scrape_location_data, locations)
