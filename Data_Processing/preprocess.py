# preprocess.py
"""
Preprocesses NetCDF ARGO float data into CSV and JSON format directly from a URL.
Usage:
    python preprocess.py
"""

import xarray as xr
import pandas as pd
import numpy as np
import requests
import io
from pathlib import Path
import json

def find_variable(ds, possible_names):
    """Find first matching variable name from a list of possibilities"""
    for name in possible_names:
        if name in ds.variables:
            return name
    return None

def extract_metadata(ds):
    """Extract float metadata from dataset attributes"""
    metadata = {}
    
    meta_fields = ['platform_number', 'project_name', 'pi_name', 
                   'data_centre', 'float_serial_no', 'wmo_inst_type']
    
    for field in meta_fields:
        if field in ds.attrs:
            metadata[field] = ds.attrs[field]
    
    lat_names = ['LATITUDE', 'latitude', 'lat', 'LAT']
    lon_names = ['LONGITUDE', 'longitude', 'lon', 'LON']
    time_names = ['TIME', 'time', 'JULD', 'juld']
    
    lat_var = find_variable(ds, lat_names)
    lon_var = find_variable(ds, lon_names)
    time_var = find_variable(ds, time_names)
    
    if lat_var:
        lat_vals = ds[lat_var].values
        metadata['latitude'] = float(lat_vals[0]) if lat_vals.size > 0 else None
    
    if lon_var:
        lon_vals = ds[lon_var].values
        metadata['longitude'] = float(lon_vals[0]) if lon_vals.size > 0 else None
    
    if time_var:
        try:
            time_vals = pd.to_datetime(ds[time_var].values[0])
            metadata['timestamp'] = str(time_vals)
        except:
            metadata['timestamp'] = None
    
    return metadata

def process_profiles(ds):
    """Extract profile data from dataset"""
    temp_names = ['TEMP', 'temp', 'TEMPERATURE', 'temperature', 'TEMP_ADJUSTED']
    pres_names = ['PRES', 'pres', 'PRESSURE', 'pressure', 'PRES_ADJUSTED']
    psal_names = ['PSAL', 'psal', 'SALINITY', 'salinity', 'PSAL_ADJUSTED']
    
    temp_var = find_variable(ds, temp_names)
    pres_var = find_variable(ds, pres_names)
    psal_var = find_variable(ds, psal_names)
    
    print(f"Found variables: TEMP={temp_var}, PRES={pres_var}, PSAL={psal_var}")
    
    if not temp_var or not pres_var:
        print("WARNING: Required temperature or pressure variables not found!")
        return pd.DataFrame()
    
    temp = ds[temp_var].values
    pres = ds[pres_var].values
    psal = ds[psal_var].values if psal_var else None
    profiles = []
    
    if temp.ndim == 2:
        n_profiles, n_depths = temp.shape
        print(f"Processing {n_profiles} profiles with up to {n_depths} depth levels")
        for prof_idx in range(n_profiles):
            for depth_idx in range(n_depths):
                t = temp[prof_idx, depth_idx]
                p = pres[prof_idx, depth_idx] if pres.shape == temp.shape else pres[depth_idx]
                s = psal[prof_idx, depth_idx] if psal is not None and psal.shape == temp.shape else None
                if not np.isnan(p) and not np.isnan(t):
                    profiles.append({
                        'profile_id': prof_idx,
                        'depth_level': depth_idx,
                        'pressure': float(p),
                        'temperature': float(t),
                        'salinity': float(s) if s is not None and not np.isnan(s) else None
                    })
    
    elif temp.ndim == 1:
        n_depths = len(temp)
        print(f"Processing single profile with {n_depths} depth levels")
        for depth_idx in range(n_depths):
            t = temp[depth_idx]
            p = pres[depth_idx]
            s = psal[depth_idx] if psal is not None else None
            if not np.isnan(p) and not np.isnan(t):
                profiles.append({
                    'profile_id': 0,
                    'depth_level': depth_idx,
                    'pressure': float(p),
                    'temperature': float(t),
                    'salinity': float(s) if s is not None and not np.isnan(s) else None
                })
    
    return pd.DataFrame(profiles)

def main():
    # Hard-coded URL of the NetCDF file
    url = "http://data-argo.ifremer.fr/geo/indian_ocean/2025/08/20250803_prof.nc"
    
    print(f"Streaming dataset from: {url}")
    try:
        print("trying....")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        print("success!")
    except Exception as e:
        print(f"Failed to fetch URL: {e}")
        return
    
    # Read into memory
    print("Reading data into memory...")
    nc_bytes = io.BytesIO(response.content)
    print("success!")
    
    # Open with xarray
    try:
        print("Opening dataset...")
        ds = xr.open_dataset(nc_bytes)
        print("success!")
    except Exception as e:
        print(f"Failed to open dataset: {e}")
        return
    
    print("\nDataset info:")
    print(f"  Dimensions: {dict(ds.dims)}")
    print(f"  Variables: {list(ds.variables.keys())}")
    print(f"  Attributes: {list(ds.attrs.keys())[:10]}...")
    
    metadata = extract_metadata(ds)
    print(f"\nMetadata extracted: {metadata}")
    
    df = process_profiles(ds)
    if df.empty:
        print("No valid profile data found!")
        return
    
    for key, value in metadata.items():
        df[key] = value
    
    # Output file names
    file_name = Path(url).name
    out_csv = Path(file_name).with_suffix(".csv")
    df.to_csv(out_csv, index=False)
    meta_json = out_csv.with_suffix(".json")
    with open(meta_json, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"\n✅ Success! CSV: {out_csv}, JSON: {meta_json}")
    print(f"  Total rows: {len(df)}")
    print(f"  Profiles: {df['profile_id'].nunique()}")
    print(f"  Depth range: {df['pressure'].min():.1f} to {df['pressure'].max():.1f} dbar")
    print(f"  Temp range: {df['temperature'].min():.2f} to {df['temperature'].max():.2f} °C")
    
    ds.close()

if __name__ == "__main__":
    main()