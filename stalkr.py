from shapely.geometry import shape, Point
import numpy as np
import logging
import pygeohash as gh
from copy import deepcopy
# from grindr_access.paths import *
from grindr_access.grindr_user import GrindrUser
GH_PRECISION = 8

def gh_to_point(gh_str):
    lat, lon = gh.decode(gh_str)
    return Point(lon, lat)
                 
def point_to_gh(point):
    return gh.encode(longitude=point.x, latitude=point.y, precision=GH_PRECISION)

def generate_points_in_geojson_feature(feature): # 100 points for kraków
    # Iterate over each feature in the GeoJSON
    polygon = shape(feature['geometry'])
    population = feature["properties"].get("population",0)
    points_density_population = 120.0 / 775000.0 # 100 points for kraków
    points_density_km2 = 330.26/100
    points_density_deg2 = points_density_km2 / 7857.0
    # Calculate bounds of the polygon
    minx, miny, maxx, maxy = polygon.bounds
    if population > 0:
        points_number = int(population * points_density_population)
    else:
        points_number = int(polygon.area / points_density_deg2)
    # Initialize list to store points for the current feature
    feature_points = []
    # Generate points
    while len(feature_points) < points_number:
        # Generate a random point within the bounding box of the polygon
        random_point = Point(np.random.uniform(minx, maxx), np.random.uniform(miny, maxy))
        
        # Check if the point is inside the polygon
        if polygon.contains(random_point):
            feature_points.append(deepcopy(random_point))
    return feature_points

def query_anchor_point(anchor_point:Point, user:GrindrUser):
    logging.debug(f"Processing anchor point: {anchor_point}")
    scraped_profiles = []
    anchor_gh = point_to_gh(anchor_point)
    actual_anchor_point = gh_to_point(anchor_gh)
    profile_list = user.getProfiles(lon=actual_anchor_point.x, lat=actual_anchor_point.y)
    for profile in profile_list['items']:
        processed_profile = process_profile_response(profile)
        if processed_profile:
            processed_profile['anchorGh'] = anchor_gh
            processed_profile['anchorPoint'] = actual_anchor_point.__geo_interface__
            scraped_profiles.append(processed_profile)
    return scraped_profiles

def process_profile_response(response):
    response_profile = response['data']
    profile_types = ["PartialProfileV1", "FullProfileV1"]
    is_profile = any(element in response_profile.get('@type') for element in profile_types)
    
    has_distance = response_profile.get('distanceMeters') is not None
    if is_profile and has_distance:
        response_profile.pop('upsellItemType', None)
        return response_profile
    else :
        return None