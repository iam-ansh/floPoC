import requests
import io
import xarray as xr
import pandas as pd
import numpy as np
import json
from pathlib import Path
from bs4 import BeautifulSoup


def find_variable(ds, possible_names):
    for name in possible_names:
        if name in ds.variables:
            return name
    return None


def extract_metadata(ds):
    metadata = {k: str(v) for k, v in ds.attrs.items()}
    return metadata


def process_profiles(ds):
    temp_names = ["TEMP", "temp", "TEMPERATURE", "temperature", "TEMP_ADJUSTED"]
    pres_names = ["PRES", "pres", "PRESSURE", "pressure", "PRES_ADJUSTED"]
    psal_names = ["PSAL", "psal", "SALINITY", "salinity", "PSAL_ADJUSTED"]
    time_names = ["TIME", "time", "JULD", "juld", "TIME_ADJUSTED"]

    temp_var = find_variable(ds, temp_names)
    pres_var = find_variable(ds, pres_names)
    psal_var = find_variable(ds, psal_names)
    time_var = find_variable(ds, time_names)

    if not temp_var or not pres_var:
        print("❌ Missing required variables")
        return pd.DataFrame()

    temp = ds[temp_var].values
    pres = ds[pres_var].values
    psal = ds[psal_var].values if psal_var else None
    time = ds[time_var].values if time_var else None

    # Convert numeric times to datetime if possible
    if time is not None:
        try:
            time = xr.decode_cf(ds[[time_var]])[time_var].values
        except Exception:
            pass  # fallback: keep raw values

    profiles = []
    if temp.ndim == 2:
        n_profiles, n_depths = temp.shape
        for prof_idx in range(n_profiles):
            prof_time = (
                str(time[prof_idx]) if time is not None and len(time) > prof_idx else None
            )
            for depth_idx in range(n_depths):
                t = temp[prof_idx, depth_idx]
                p = pres[prof_idx, depth_idx] if pres.shape == temp.shape else pres[depth_idx]
                s = psal[prof_idx, depth_idx] if psal is not None and psal.shape == temp.shape else None
                if not np.isnan(p) and not np.isnan(t):
                    profiles.append({
                        "profile_id": prof_idx,
                        "depth_level": depth_idx,
                        "time": prof_time,
                        "pressure": float(p),
                        "temperature": float(t),
                        "salinity": float(s) if s is not None and not np.isnan(s) else None
                    })
    elif temp.ndim == 1:
        prof_time = str(time[0]) if time is not None else None
        for depth_idx in range(len(temp)):
            t = temp[depth_idx]
            p = pres[depth_idx]
            s = psal[depth_idx] if psal is not None else None
            if not np.isnan(p) and not np.isnan(t):
                profiles.append({
                    "profile_id": 0,
                    "depth_level": depth_idx,
                    "time": prof_time,
                    "pressure": float(p),
                    "temperature": float(t),
                    "salinity": float(s) if s is not None and not np.isnan(s) else None
                })

    return pd.DataFrame(profiles)


def download_and_process(url):
    print(f"\n📂 Streaming dataset from: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch URL: {e}")
        return

    nc_bytes = io.BytesIO(response.content)

    try:
        ds = xr.open_dataset(nc_bytes)
    except Exception as e:
        print(f"❌ Failed to open dataset: {e}")
        return

    df = process_profiles(ds)
    if df.empty:
        print("⚠️ No valid profile data found!")
        return

    metadata = extract_metadata(ds)
    for var in ["temperature", "pressure", "salinity"]:
        if var in df.columns:
            metadata[f"{var}_min"] = float(df[var].min())
            metadata[f"{var}_max"] = float(df[var].max())

    # Add time range to metadata
    if "time" in df.columns and not df["time"].isnull().all():
        metadata["time_start"] = str(df["time"].dropna().min())
        metadata["time_end"] = str(df["time"].dropna().max())

    Path("csvs").mkdir(exist_ok=True)
    Path("jsons").mkdir(exist_ok=True)

    file_name = Path(url).name
    out_csv = Path("csvs") / Path(file_name).with_suffix(".csv")
    out_json = Path("jsons") / Path(file_name).with_suffix(".json")

    df.to_csv(out_csv, index=False)
    with open(out_json, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"✅ Saved {out_csv} and {out_json}")


def get_nc_files(base_url):
    try:
        resp = requests.get(base_url)
        resp.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch base URL: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True)]
    return [base_url + link for link in links if link.endswith(".nc")]


if __name__ == "__main__":
    base_url = "https://data-argo.ifremer.fr/geo/indian_ocean/2025/08/"
    nc_files = get_nc_files(base_url)

    print(f"Found {len(nc_files)} .nc files in {base_url}")

    for url in nc_files:
        download_and_process(url)
