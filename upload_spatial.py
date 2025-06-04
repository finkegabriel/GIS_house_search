import os
import pandas as pd
import geopandas as gpd
from google.oauth2 import service_account
from pandas_gbq import to_gbq
import json
import shapely

# CONFIG
input_file = "co_census.geojson"
project_id = "orobytes"
dataset_id = "az_homes"
table_id = "az_homes_geom"
full_table_id = f"{dataset_id}.{table_id}"
credentials_path = "orobytes.json"

# Read spatial data using GeoPandas
gdf = gpd.read_file(input_file)

# Add centroid coordinates

# Project to Web Mercator for accurate centroid calculation
gdf_projected = gdf.to_crs('EPSG:3857')
# Calculate centroids in projected space, then convert back to WGS84
centroids = gdf_projected.geometry.centroid.to_crs('EPSG:4326')
gdf['longitude'] = centroids.x
gdf['latitude'] = centroids.y
# Convert geometry to GeoJSON string (RFC 7946)
# gdf['geometry_geojson'] = gdf.geometry.to_json()

def force_2d(geom):
    """Convert geometry to 2D only (drop Z dimension) and validate coordinates"""
    if geom.is_empty:
        return geom
    if geom.geom_type == 'Point':
        return shapely.geometry.Point([geom.x, geom.y])
    elif geom.geom_type == 'LineString':
        return shapely.geometry.LineString([[x, y] for x, y, *_ in geom.coords])
    elif geom.geom_type == 'Polygon':
        exterior = [[x, y] for x, y, *_ in geom.exterior.coords]
        interiors = [[[x, y] for x, y, *_ in ring.coords] for ring in geom.interiors]
        return shapely.geometry.Polygon(exterior, interiors)
    elif geom.geom_type == 'MultiPolygon':
        polygons = []
        for polygon in geom.geoms:
            exterior = [[x, y] for x, y, *_ in polygon.exterior.coords]
            interiors = [[[x, y] for x, y, *_ in ring.coords] for ring in polygon.interiors]
            polygons.append(shapely.geometry.Polygon(exterior, interiors))
        return shapely.geometry.MultiPolygon(polygons)
    else:
        raise ValueError(f"Unsupported geometry type: {geom.geom_type}")
def clean_geometry(geom):
    """Clean geometry by making it valid and removing self-intersections"""
    if geom.is_empty:
        return geom
    # Make geometry valid
    if not geom.is_valid:
        geom = shapely.ops.make_valid(geom)
    # Force 2D
    if geom.geom_type == 'Point':
        return shapely.geometry.Point([geom.x, geom.y])
    elif geom.geom_type == 'LineString':
        return shapely.geometry.LineString([[x, y] for x, y, *_ in geom.coords])
    elif geom.geom_type == 'Polygon':
        exterior = [[x, y] for x, y, *_ in geom.exterior.coords]
        interiors = [[[x, y] for x, y, *_ in ring.coords] for ring in geom.interiors]
        return shapely.geometry.Polygon(exterior, interiors)
    elif geom.geom_type == 'MultiPolygon':
        polygons = []
        for polygon in geom.geoms:
            exterior = [[x, y] for x, y, *_ in polygon.exterior.coords]
            interiors = [[[x, y] for x, y, *_ in ring.coords] for ring in polygon.interiors]
            polygons.append(shapely.geometry.Polygon(exterior, interiors))
        return shapely.geometry.MultiPolygon(polygons)
    else:
        raise ValueError(f"Unsupported geometry type: {geom.geom_type}")

# Replace the existing geometry processing code with:
# Clean and validate geometries
gdf['geometry'] = gdf.geometry.apply(clean_geometry)

# Convert to GeoJSON
gdf['geometry_geojson'] = gdf.geometry.apply(lambda geom: json.dumps(geom.__geo_interface__))

# Add extra validation to check for valid polygons
def validate_geojson(geojson_str):
    """Validate GeoJSON structure and geometry validity"""
    data = json.loads(geojson_str)
    if data['type'] == 'Polygon' or data['type'] == 'MultiPolygon':
        # Convert back to shapely to check validity
        geom = shapely.geometry.shape(data)
        if not geom.is_valid:
            raise ValueError(f"Invalid {data['type']}: contains self-intersections or invalid ring orientation")
    return geojson_str

# Apply validation
gdf['geometry_geojson'] = gdf['geometry_geojson'].apply(validate_geojson)

# Add validation check
def validate_geojson(geojson_str):
    data = json.loads(geojson_str)
    if 'coordinates' in data:
        coords = data['coordinates']
        if isinstance(coords[0], (list, tuple)) and len(coords[0]) > 2:
            raise ValueError("Found coordinates with more than 2 dimensions")
    return geojson_str

# Apply validation
gdf['geometry_geojson'] = gdf['geometry_geojson'].apply(validate_geojson)

# Convert to DataFrame and drop geometry column
df = pd.DataFrame(gdf.drop(columns=['geometry']))

credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Upload to BigQuery (without GEOGRAPHY column yet)
to_gbq(
    dataframe=df,
    destination_table=full_table_id,
    project_id=project_id,
    if_exists="replace",
    credentials=credentials
)

print(f"✅ Uploaded {len(gdf)} rows to BigQuery table `{full_table_id}`.")

# Now: Add a GEOGRAPHY column and populate it using BigQuery SQL
from google.cloud import bigquery

bq_client = bigquery.Client(project=project_id, credentials=credentials)
table_ref = f"{project_id}.{full_table_id}"

# Add GEOGRAPHY column
bq_client.query(f"""
ALTER TABLE `{table_ref}`
ADD COLUMN IF NOT EXISTS geometry GEOGRAPHY;
""").result()

# Populate GEOGRAPHY column from GeoJSON
bq_client.query(f"""
UPDATE `{table_ref}`
SET geometry = ST_GEOGFROMGEOJSON(geometry_geojson)
WHERE TRUE;
""").result()

print("✅ GEOGRAPHY column added and populated.")