import requests
import pandas as pd
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from tenacity import retry, stop_after_attempt, wait_random_exponential


# ===========================
# CONFIGURATION
# ===========================

# Eurostat SDMX-JSON API endpoint for HICP item weights (ECOICOP v2)
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_iw"

# Geographic coverage: EU aggregates + all EU-27 member states
GEO_KEY = "EU+EA+BE+BG+CZ+DK+DE+EE+IE+EL+ES+FR+HR+IT+CY+LV+LT+LU+HU+MT+NL+AT+PL+PT+RO+SI+SK+FI+SE"

# File paths
MAPS_DIR = Path(__file__).parent
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ===========================
# DOWNLOAD FUNCTION
# ===========================

@retry(stop=stop_after_attempt(7), wait=wait_random_exponential(multiplier=1, max=60), reraise=True)
def download_weights(coicop_code: str) -> pd.DataFrame:
    """Download HICP item weights for a specific ECOICOP v2 code.

    Args:
        coicop_code: ECOICOP v2 classification code (e.g. 'TOTAL', 'CP01', 'CP0111A')

    Returns:
        DataFrame with columns: [coicop18, geo, time, value]

    Raises:
        requests.exceptions.RequestException: on network/HTTP errors after retries
    """
    params = {
        "geo": GEO_KEY,
        "coicop18": coicop_code,
    }

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if not data.get("value"):
        print(f"  No data for {coicop_code}")
        return pd.DataFrame()

    # Extract dimension category labels and indices
    dims = data["dimension"]
    dim_ids = data["id"]
    dim_sizes = data["size"]

    # Build index -> label mappings for each dimension
    dim_labels = {}
    for i, dim_id in enumerate(dim_ids):
        cat = dims[dim_id]["category"]
        idx_map = cat.get("index", {})
        lbl_map = cat.get("label", {})
        # Create position -> label mapping
        dim_labels[dim_id] = {v: lbl_map[k] for k, v in idx_map.items()}

    # Convert flat value array to records
    records = []
    obs_values = data["value"]
    total_size = 1
    for s in dim_sizes:
        total_size *= s

    for flat_idx, val in obs_values.items():
        if val is None:
            continue
        idx = int(flat_idx)
        # Decode multi-dimensional index
        coords = []
        remaining = idx
        for i in range(len(dim_sizes) - 1, -1, -1):
            dim_id = dim_ids[i]
            size = dim_sizes[i]
            pos = remaining % size
            remaining = remaining // size
            coords.append((dim_id, dim_labels[dim_id].get(pos, pos)))
        coords.reverse()

        record = {dim_id: label for dim_id, label in coords}
        record["value"] = val
        records.append(record)

    if not records:
        print(f"  No valid records for {coicop_code}")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    print(f"  Downloaded {len(df)} records for {coicop_code}")

    time.sleep(0.3)
    return df


# ===========================
# MAIN
# ===========================

def main():
    # Load the COICOP v2 classification map
    code_map = pd.read_csv(MAPS_DIR / "coicop18.csv")
    print(f"Loaded {len(code_map)} COICOP v2 categories from coicop18.csv")

    all_data = []
    codes = code_map["code"].tolist()

    for i, item_id in enumerate(codes):
        print(f"\n[{i + 1}/{len(codes)}] Downloading weights for: {item_id}")

        try:
            df = download_weights(item_id)
            if not df.empty:
                # Add the human-readable name from the classification map
                name_row = code_map[code_map["code"] == item_id]
                if not name_row.empty:
                    df["name"] = name_row.iloc[0]["name"]
                all_data.append(df)

        except Exception as e:
            print(f"  Error for {item_id}: {e}")
            continue

    # ===========================
    # SAVE TO PARQUET
    # ===========================

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.drop_duplicates(keep="last")

        # Drop the freq column (always 'A' for annual weights)
        combined_df = combined_df.drop(columns=["freq"], errors="ignore")

        # Reorder columns for clarity
        cols_order = ["coicop18", "name", "geo", "time", "value"]
        cols_order = [c for c in cols_order if c in combined_df.columns]
        combined_df = combined_df[cols_order]

        output_file = OUTPUT_DIR / "hicp_weights.parquet"
        combined_df.to_parquet(output_file, index=False)
        print(f"\n✅ Saved {len(combined_df):,} records to {output_file}")
        print(f"   Columns: {list(combined_df.columns)}")
        print(f"   COICOP codes: {combined_df['coicop18'].nunique()}")
        print(f"   Countries/areas: {combined_df['geo'].nunique()}")
        print(f"   Years: {combined_df['time'].min()} – {combined_df['time'].max()}")
    else:
        print("\n❌ No data downloaded.")


if __name__ == "__main__":
    main()
