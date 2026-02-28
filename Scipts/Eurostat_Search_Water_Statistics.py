import requests
import pandas as pd
from typing import Dict, List, Optional, Union
from datetime import datetime

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# -------------------------------------------------
# 1. Call Eurostat Statistics API (supports repeated params)
# -------------------------------------------------
#
def fetch_eurostat_json(dataset_code: str,
                        filters: Optional[Dict[str, Union[str, List[str]]]] = None,
                        lang: str = "EN") -> Optional[Dict]:
    """
    Call Eurostat Statistics API for a given dataset code and filters.
    Returns JSON on success, or None if dataset is not available or any HTTP error occurs.
    """
    params: List[tuple] = [("lang", lang)]

    if filters:
        for dim, values in filters.items():
            if isinstance(values, str):
                vals = [v.strip() for v in values.split(",")]
            else:
                vals = [str(v).strip() for v in values]

            for v in vals:
                if v:
                    params.append((dim, v))

    url = f"{BASE_URL}/{dataset_code}"
    resp = requests.get(url, params=params, timeout=60)

    if resp.status_code == 404:
        # Dataset not available for dissemination → just log and skip
        print(f"⚠ Dataset '{dataset_code}' not available (404). Skipping.")
        print("URL:", resp.url)
        print("Body:", resp.text[:300])
        return None

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print("❌ Eurostat API error")
        print("Dataset:", dataset_code)
        print("URL:", resp.url)
        print("Status:", resp.status_code)
        print("Body:", resp.text[:500])
        # Don’t crash – just skip this dataset
        return None

    return resp.json()

# -------------------------------------------------
# 2. JSON-stat → flat pandas.DataFrame
# -------------------------------------------------
def jsonstat_to_dataframe(js: Dict) -> pd.DataFrame:
    """
    Convert a Eurostat JSON-stat 2.0 dataset (single 'dataset') into a flat DataFrame.
    Each row = one observation, with one column per dimension + 'value'.
    """
    # Unwrap "dataset" if present
    if "dataset" in js:
        js = js["dataset"]

    dim = js["dimension"]
    value = js["value"]

    dim_ids = js["id"]          # e.g. ["geo", "time", "unit", "wat_src", ...]
    dim_sizes = js["size"]

    # Build ordered category lists for each dimension
    dim_categories: Dict[str, List[str]] = {}
    for dim_name in dim_ids:
        cat = dim[dim_name]["category"]
        index = cat.get("index", {})  # safer than cat["index"]
        
        # Build position → code map
        pos_to_code = {pos: code for code, pos in index.items()}
        if not pos_to_code:
            # Eurostat returned no categories for this dimension → insert placeholder
            dim_categories[dim_name] = ["_missing_"]
            continue
        max_pos = max(pos_to_code.keys())
        ordered_codes = [pos_to_code[i] for i in range(max_pos + 1)]
        dim_categories[dim_name] = ordered_codes


    # Total number of observations (cartesian product of dims)
    n_obs = 1
    for s in dim_sizes:
        n_obs *= s

    rows = []

    # Precompute strides to decode flat index
    strides = []
    acc = 1
    for size in reversed(dim_sizes):
        strides.insert(0, acc)
        acc *= size

    # Handle both dense list and sparse dict "value"
    def get_value_at(idx: int):
        if isinstance(value, list):
            return value[idx]
        elif isinstance(value, dict):
            return value.get(str(idx), None)
        else:
            return None

    for flat_idx in range(n_obs):
        remaining = flat_idx
        coord_codes = {}
        for dim_idx, size in enumerate(dim_sizes):
            stride = strides[dim_idx]
            coord_index = remaining // stride
            remaining = remaining % stride

            dim_name = dim_ids[dim_idx]
            code_list = dim_categories[dim_name]
            code = code_list[coord_index]
            coord_codes[dim_name] = code

        v = get_value_at(flat_idx)
        # Skip missing values (Eurostat uses None or ":" in other formats)
        if v is None:
            continue

        row = coord_codes.copy()
        row["value"] = v
        rows.append(row)

    return pd.DataFrame(rows)


# -------------------------------------------------
# 3. High-level: crawl env_nwat datasets and save CSV
# -------------------------------------------------
#
def crawl_eurostat_datasets(dataset_codes: List[str],
                            filters_per_dataset: Optional[Dict[str, Dict[str, Union[str, List[str]]]]] = None,
                            lang: str = "EN",
                            out_csv: str = None ) -> pd.DataFrame:
    all_frames = []

    for code in dataset_codes:
        print(f"🔎 Fetching Eurostat dataset: {code}")
        filters = (filters_per_dataset or {}).get(code, {})

        js = fetch_eurostat_json(code, filters=filters, lang=lang)
        if js is None:
            print(f"⏭ Skipping dataset '{code}' due to previous error.")
            continue

        df = jsonstat_to_dataframe(js)

        if df.empty:
            print(f"⚠ No data returned for {code}")
            continue

        df["dataset_code"] = code

        # -------------------------------------------------
        # Generate OpenKIWAS_ID field
        # -------------------------------------------------
        df = df.reset_index(drop=True)

        df["OpenKIWAS_ID"] = [
            f"eurostat-{code}-{str(i+1).zfill(4)}"
            for i in range(len(df))
        ]

        print(f"   {len(df)} rows retrieved for {code}")
        all_frames.append(df)

    if not all_frames:
        print("⚠ No data retrieved, CSV will not be created.")
        return pd.DataFrame()

     # -------------------------------------------------
    # Enforce required column order
    # -------------------------------------------------
    full_df = pd.concat(all_frames, ignore_index=True)   

    desired_order = [
        "OpenKIWAS_ID",
        "dataset_code",
        "freq",
        "wat_proc",
        "unit",
        "geo",
        "time",
        "value",
        "wat_src",
    ]

    # Keep only columns that exist (some datasets may not contain all dimensions)
    existing_columns = [col for col in desired_order if col in full_df.columns]

    full_df = full_df[existing_columns]

    # -------------------------------------------------
    # Timestamped filename
    # ------------------------------------------------- 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = f"eurostat_env_nwat_{timestamp}.csv"

    full_df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"✅ Done! Combined results saved to {out_csv}")

    return full_df


# -------------------------------------------------
# 4. Run for Water statistics on national level (env_nwat)
# -------------------------------------------------
if __name__ == "__main__":
    # env_nwat family datasets (you can add/remove as needed)
    DATASETS = [
        "env_wat_ltaa",  # Renewable freshwater resources – long term annual averages
        "env_wat_res",   # Renewable freshwater resources
        "env_wat_abs",   # Annual freshwater abstraction by source and sector
        "env_wat_use",   # Water made available for use
        "env_wat_pop",   # Population connected to public water supply
        "env_wat_bal",
        "env_wat_con",
        "env_wat_spd",
        "env_wat_genv",
        "env_wat_genp",
    ]

    # Example: filter by years and a few countries
    years = [str(y) for y in range(2015, 2025)]
    countries = ["EU27_2020","AL","AT","BA","BE","BG","BY","CH","CY","CZ","DE","DK","EE","ES","FI","FR","GR","HR","HU","IE","IS","IT","LT","LU","LV","MD","ME","MK","MT","NL","NM","NO","PL","PT","RO","RS","RU","SE","SI","SK","TR","UA","UK"]
    
    FILTERS = {
        code: {
            "time": years,        # same for all env_nwat datasets here
            "geo": countries,
            # other dimensions (wat_src, wat_proc, nace_r2, etc.) are left "all"
        }
        for code in DATASETS
    }


    crawl_eurostat_datasets(
        DATASETS,
        filters_per_dataset=FILTERS,
        lang="EN",
        out_csv="Eurostat",
    )

