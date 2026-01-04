import os
import json
import polars as pl
import math
import requests
import sys

OUTPUT_DIR = "static_tiles"
CSV_URL = "https://pub-ecf2cacf42304db4aff89b230d889189.r2.dev/source_data.csv"
CSV_FILE = "source_data.csv"

def download_csv():
    if not os.path.exists(CSV_FILE):
        print(f"Downloading CSV from {CSV_URL}...")
        try:
            response = requests.get(CSV_URL, stream=True)
            response.raise_for_status()
            
            with open(CSV_FILE, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download completed.")
        except Exception as e:
            print(f"Error downloading CSV: {e}")
            sys.exit(1)

def tile_to_bbox(x, y, zoom):
    n = 2.0 ** zoom
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_rad_min = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    lat_min = math.degrees(lat_rad_min)
    lat_rad_max = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_max = math.degrees(lat_rad_max)
    return lat_min, lat_max, lon_min, lon_max

def perform_clustering(df, min_lat, max_lat, min_lon, max_lon, zoom):
    if df.is_empty():
        return []
    if zoom >= 14:
        return df.head(500).to_dicts()
    
    resolution = {6: 3, 7: 4, 8: 5, 9: 6, 10: 7, 11: 8, 12: 9, 13: 10}.get(zoom, 10)
    
    lat_step = (max_lat - min_lat) / resolution or 0.0001
    lon_step = (max_lon - min_lon) / resolution or 0.0001
    
    # Check required columns exist
    available_cols = df.columns
    required_aggs = [
        pl.col('latitude').mean().alias('latitude'),
        pl.col('longitude').mean().alias('longitude'),
        pl.len().alias('count')
    ]
    
    # Optional columns - only include if they exist in source
    if 'id' in available_cols:
        required_aggs.append(pl.col('id').first())
    elif 'property_id' in available_cols:
        required_aggs.append(pl.col('property_id').first().alias('id'))
        
    if 'margin' in available_cols:
        required_aggs.append(pl.col('margin').max())
        
    if 'type_local' in available_cols:
        required_aggs.append(pl.col('type_local').first())
        
    if 'address' in available_cols:
        required_aggs.append(pl.col('address').first())
    
    return (
        df.with_columns([
            ((pl.col('latitude') - min_lat) / lat_step).cast(pl.Int32).alias('lat_idx'),
            ((pl.col('longitude') - min_lon) / lon_step).cast(pl.Int32).alias('lon_idx')
        ])
        .group_by(['lat_idx', 'lon_idx'])
        .agg(required_aggs)
    ).to_dicts()

def generate():
    download_csv()
    
    print("Loading CSV...")
    try:
        df = pl.read_csv(CSV_FILE)
        # Rename property_id to id if needed early
        if "property_id" in df.columns and "id" not in df.columns:
            df = df.rename({"property_id": "id"})
            
        df = df.drop_nulls(subset=['latitude', 'longitude'])
        print(f"Loaded {len(df)} rows")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return
    
    total = 0
    
    # Using range 6 to 15 (exclusive 15, so 6-14)
    for zoom in range(6, 15):
        print(f"Zoom {zoom}...", end=" ")
        n = 2.0 ** zoom
        
        # Calculate tile coordinates
        df_zoom = df.with_columns([
            ((pl.col("longitude") + 180.0) / 360.0 * n).floor().cast(pl.Int32).alias("tile_x"),
            (pl.col("latitude") * math.pi / 180.0).alias("lat_rad")
        ])
        
        # y calculation
        df_zoom = df_zoom.with_columns(
            ((1.0 - ((pl.col("lat_rad").tan() + (1.0 / pl.col("lat_rad").cos())).log()) / math.pi) / 2.0 * n)
            .floor().cast(pl.Int32).alias("tile_y")
        )
        
        # Filter for France region to avoid excessive tile generation
        tiles = df_zoom.filter(
            (pl.col("latitude") >= 41) & (pl.col("latitude") <= 51) &
            (pl.col("longitude") >= -5) & (pl.col("longitude") <= 10)
        ).partition_by(["tile_x", "tile_y"], as_dict=True)
        
        count = 0
        for (tx, ty), tile_df in tiles.items():
            min_lat, max_lat, min_lon, max_lon = tile_to_bbox(tx, ty, zoom)
            result = perform_clustering(tile_df, min_lat, max_lat, min_lon, max_lon, zoom)
            
            if result:
                path = f"{OUTPUT_DIR}/{zoom}/{tx}/{ty}.json"
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'w') as f:
                    json.dump(result, f, separators=(',', ':'))
                count += 1
        
        print(f"{count} tiles")
        total += count
    
    print(f"\nDone! {total} files in ./{OUTPUT_DIR}/")

if __name__ == "__main__":
    generate()
