import numpy as np
from scipy.optimize import minimize
import json
from shapely.geometry import Point

def meters_to_degrees(lat, dist_meters):
    """Convert a distance in meters to degrees of longitude at a specific latitude."""
    meters_per_degree_lat = 111000  # Approximate meters in one degree of latitude
    # Conversion factor for longitude at the given latitude
    meters_per_degree_lon = np.cos(np.radians(lat)) * meters_per_degree_lat
    return dist_meters / meters_per_degree_lon

def localize_point(anchor_data):
    # Extract coordinates and distances from anchor_data
    anchors = np.array([anchor[0]['coordinates'] for anchor in anchor_data])
    distances_m = np.array([anchor[1] for anchor in anchor_data])
    
    # Convert distances from meters to degrees of longitude based on each anchor's latitude
    distances_deg = np.array([meters_to_degrees(lat, dist) for lat, dist in zip(anchors[:, 1], distances_m)])

    # Define the objective function to minimize
    def objective_function(p):
        # Calculate distances from p to each anchor in degrees
        # We use only the longitude conversion as distances in degrees because typically longitude degrees are smaller than latitude degrees and vary with latitude
        dist_calculated = np.sqrt(((anchors[:, 0] - p[0]) ** 2) * np.cos(np.radians(p[1]))**2 + (anchors[:, 1] - p[1]) ** 2)
        return np.sum((dist_calculated - distances_deg) ** 2)
    
    # Initial guess for the position of P (average of anchor coordinates)
    initial_guess = np.mean(anchors, axis=0)
    
    # Perform minimization using the L-BFGS-B algorithm
    result = minimize(objective_function, initial_guess, method='L-BFGS-B')
    
    if result.success:
        estimated_point = result.x
        return Point(estimated_point)
    else:
        raise ValueError("Optimization failed to converge")
    
# Example usage:
anchor_data = [
    ({"type": "Point", "coordinates": [20.243, 49.959]}, 3462),
    ({"type": "Point", "coordinates": [20.32, 50.092]}, 13133),
    ({"type": "Point", "coordinates": [20.29, 50.047]}, 7744)
]

estimated_geojson_point = localize_point(anchor_data)
print(estimated_geojson_point.__geo_interface__)

print([Point(geojson_point[0]['coordinates']) for geojson_point in anchor_data])