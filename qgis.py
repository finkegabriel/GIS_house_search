import os
import pandas as pd
from google.oauth2 import service_account
from pandas_gbq import to_gbq

from qgis.core import (
    QgsProject,
    QgsVectorLayer
)

# CONFIG
layer_name = "az_homes"  # QGIS layer name
project_id = "orobytes"
dataset_id = "az_homes"
table_id = "az_homes"
full_table_id = f"{dataset_id}.{table_id}"
credentials_path = "orobytes.json"

# Load QGIS layer
layer = QgsProject.instance().mapLayersByName(layer_name)[0]

# Get attribute table as list of dictionaries
features = [f for f in layer.getFeatures()]
columns = layer.fields().names()
rows = [ {col: f[col] for col in columns} for f in features ]

# OPTIONAL: Add geometry centroid X/Y if it's a polygon or line
add_coords = True
if add_coords:
    for i, f in enumerate(features):
        geom = f.geometry().centroid().asPoint()
        rows[i]["longitude"] = geom.x()
        rows[i]["latitude"] = geom.y()
    columns += ["longitude", "latitude"]

# Convert to DataFrame
df = pd.DataFrame(rows)

# Load credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Upload to BigQuery
to_gbq(
    dataframe=df,
    destination_table=full_table_id,
    project_id=project_id,
    if_exists="replace",  # or "append"
    credentials=credentials
)

print(f"✅ Uploaded {len(df)} rows to BigQuery table `{full_table_id}`.")
