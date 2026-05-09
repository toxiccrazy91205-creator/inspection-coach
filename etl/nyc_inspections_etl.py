import os
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_URL = os.getenv("SOCRATA_BASE_URL", "https://data.cityofnewyork.us")
DATASET = os.getenv("SOCRATA_DATASET_ID", "43nn-pn8j")
FEATURE_DIR = os.getenv("FEATURE_STORE_DIR", "./data/parquet")

# Only fetch columns the app actually uses — keeps each page small
SELECT_COLS = ",".join([
    "camis", "dba", "boro", "building", "street", "zipcode",
    "cuisine_description", "inspection_date", "action",
    "violation_code", "violation_description", "critical_flag",
    "score", "grade", "grade_date", "inspection_type",
    "latitude", "longitude",
])

PAGE_SIZE = 20_000


def fetch_to_parquet(name: str = "inspections_raw") -> int:
    """Stream the full NYC inspection dataset page-by-page directly to Parquet.
    Keeps memory use bounded to one page (~20k rows) at a time."""
    url = f"{BASE_URL}/resource/{DATASET}.json"
    app_token = os.getenv("NYC_APP_TOKEN", "")
    headers = {"X-App-Token": app_token} if app_token else {}

    os.makedirs(FEATURE_DIR, exist_ok=True)
    path = os.path.join(FEATURE_DIR, f"{name}.parquet")
    tmp_path = path + ".tmp"

    writer = None
    offset = 0
    total = 0

    try:
        while True:
            params = {
                "$select": SELECT_COLS,
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "camis,inspection_date",
            }
            r = httpx.get(url, params=params, headers=headers, timeout=90)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break

            df = pd.DataFrame(batch)
            if "inspection_date" in df.columns:
                df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
            for col in ["score"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if writer is None:
                table = pa.Table.from_pandas(df, preserve_index=False)
                writer = pq.ParquetWriter(tmp_path, table.schema, compression="snappy")
            else:
                # Reindex to match the established schema so column order is consistent
                df = df.reindex(columns=[s.name for s in writer.schema], fill_value=None)
                table = pa.Table.from_pandas(df, schema=writer.schema, preserve_index=False)
            writer.write_table(table)

            total += len(batch)
            offset += PAGE_SIZE
            print(f"  fetched {total} rows...", flush=True)

            if len(batch) < PAGE_SIZE:
                break
    finally:
        if writer:
            writer.close()

    # Atomically replace the old parquet once fully written
    if total > 0:
        os.replace(tmp_path, path)
        print(f"Wrote {total} rows to {path}")
    elif os.path.exists(tmp_path):
        os.remove(tmp_path)

    return total


# Legacy single-shot helpers kept for compatibility
def fetch(limit=50_000) -> pd.DataFrame:
    url = f"{BASE_URL}/resource/{DATASET}.json"
    app_token = os.getenv("NYC_APP_TOKEN", "")
    headers = {"X-App-Token": app_token} if app_token else {}
    params = {"$select": SELECT_COLS, "$limit": limit, "$order": "inspection_date DESC"}
    r = httpx.get(url, params=params, headers=headers, timeout=90)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if not df.empty and "inspection_date" in df.columns:
        df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    return df


def write_parquet(df: pd.DataFrame, name: str):
    os.makedirs(FEATURE_DIR, exist_ok=True)
    path = os.path.join(FEATURE_DIR, f"{name}.parquet")
    df.to_parquet(path, index=False)
    print(f"Wrote {len(df)} rows to {path}")


if __name__ == "__main__":
    fetch_to_parquet()
