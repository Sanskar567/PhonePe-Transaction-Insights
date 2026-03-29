"""
PhonePe Pulse Data - ETL Pipeline
===================================
Extracts JSON data from the cloned PhonePe/pulse repository,
transforms it into structured DataFrames, and loads it into
an SQLite database with 9 tables.

Tables created:
  1. aggregated_transaction
  2. aggregated_user
  3. aggregated_insurance
  4. map_transaction
  5. map_user
  6. map_insurance
  7. top_transaction
  8. top_user
  9. top_insurance
"""

import os
import json
import sqlite3
import pandas as pd

# ============================================================
# CONFIGURATION
# ============================================================
# Path where this script is located (Data_Analysis_Files/)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Root directory of the project
ROOT_DIR = os.path.dirname(CURRENT_DIR)

# Path to the pulse repository (expected in project root)
DATA_DIR = os.path.join(ROOT_DIR, "pulse", "data")
# Path where the database will be created/updated
DB_PATH = os.path.join(CURRENT_DIR, "phonepe.db")


# ============================================================
# HELPER: Walk year/quarter JSON files at both country & state level
# ============================================================
def walk_json_files(category_path):
    """
    Yields (state, year, quarter, data_dict) for every JSON file
    found under the given category path.

    For country-level files (e.g. .../india/2024/1.json),
    state is returned as 'india' (aggregate).
    For state-level files (e.g. .../india/state/karnataka/2024/1.json),
    state is the state folder name.
    """
    india_path = os.path.join(category_path, "country", "india")
    if not os.path.exists(india_path):
        # Try alternate path for map data (hover sub-folder)
        india_path = os.path.join(category_path, "hover", "country", "india")
    if not os.path.exists(india_path):
        print(f"  [WARN] Path not found: {india_path}")
        return

    for entry in os.listdir(india_path):
        entry_path = os.path.join(india_path, entry)

        # --- Year directories at country level ---
        if entry.isdigit() and os.path.isdir(entry_path):
            year = int(entry)
            for qfile in os.listdir(entry_path):
                if qfile.endswith(".json"):
                    quarter = int(qfile.replace(".json", ""))
                    fpath = os.path.join(entry_path, qfile)
                    try:
                        with open(fpath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        yield ("india", year, quarter, data)
                    except Exception as e:
                        print(f"  [ERR] {fpath}: {e}")

        # --- State directory ---
        elif entry == "state" and os.path.isdir(entry_path):
            for state_name in os.listdir(entry_path):
                state_path = os.path.join(entry_path, state_name)
                if not os.path.isdir(state_path):
                    continue
                for year_dir in os.listdir(state_path):
                    year_path = os.path.join(state_path, year_dir)
                    if not year_dir.isdigit() or not os.path.isdir(year_path):
                        continue
                    year = int(year_dir)
                    for qfile in os.listdir(year_path):
                        if qfile.endswith(".json"):
                            quarter = int(qfile.replace(".json", ""))
                            fpath = os.path.join(year_path, qfile)
                            try:
                                with open(fpath, "r", encoding="utf-8") as f:
                                    data = json.load(f)
                                yield (state_name, year, quarter, data)
                            except Exception as e:
                                print(f"  [ERR] {fpath}: {e}")


# ============================================================
# 1. AGGREGATED TRANSACTION
# ============================================================
def extract_aggregated_transaction():
    """
    Columns: state, year, quarter, transaction_type, transaction_count, transaction_amount
    """
    print("[1/9] Extracting aggregated_transaction ...")
    rows = []
    path = os.path.join(DATA_DIR, "aggregated", "transaction")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            for item in raw["data"]["transactionData"]:
                name = item["name"]
                for pi in item["paymentInstruments"]:
                    rows.append({
                        "state": state,
                        "year": year,
                        "quarter": quarter,
                        "transaction_type": name,
                        "transaction_count": pi["count"],
                        "transaction_amount": pi["amount"],
                    })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 2. AGGREGATED USER
# ============================================================
def extract_aggregated_user():
    """
    Columns: state, year, quarter, registered_users, app_opens,
             brand, brand_count, brand_percentage
    """
    print("[2/9] Extracting aggregated_user ...")
    rows = []
    path = os.path.join(DATA_DIR, "aggregated", "user")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            agg = raw["data"]["aggregated"]
            reg_users = agg.get("registeredUsers", 0)
            app_opens = agg.get("appOpens", 0)

            devices = raw["data"].get("usersByDevice")
            if devices:
                for dev in devices:
                    rows.append({
                        "state": state,
                        "year": year,
                        "quarter": quarter,
                        "registered_users": reg_users,
                        "app_opens": app_opens,
                        "brand": dev.get("brand", "Unknown"),
                        "brand_count": dev.get("count", 0),
                        "brand_percentage": dev.get("percentage", 0.0),
                    })
            else:
                # Country-level files often have null for usersByDevice
                rows.append({
                    "state": state,
                    "year": year,
                    "quarter": quarter,
                    "registered_users": reg_users,
                    "app_opens": app_opens,
                    "brand": None,
                    "brand_count": None,
                    "brand_percentage": None,
                })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 3. AGGREGATED INSURANCE
# ============================================================
def extract_aggregated_insurance():
    """
    Columns: state, year, quarter, transaction_type, transaction_count, transaction_amount
    """
    print("[3/9] Extracting aggregated_insurance ...")
    rows = []
    path = os.path.join(DATA_DIR, "aggregated", "insurance")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            for item in raw["data"]["transactionData"]:
                name = item["name"]
                for pi in item["paymentInstruments"]:
                    rows.append({
                        "state": state,
                        "year": year,
                        "quarter": quarter,
                        "transaction_type": name,
                        "transaction_count": pi["count"],
                        "transaction_amount": pi["amount"],
                    })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 4. MAP TRANSACTION
# ============================================================
def extract_map_transaction():
    """
    Columns: state, year, quarter, district, transaction_count, transaction_amount
    """
    print("[4/9] Extracting map_transaction ...")
    rows = []
    path = os.path.join(DATA_DIR, "map", "transaction")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            hover_list = raw["data"].get("hoverDataList", [])
            if hover_list:
                for item in hover_list:
                    district = item["name"]
                    for m in item["metric"]:
                        rows.append({
                            "state": state,
                            "year": year,
                            "quarter": quarter,
                            "district": district,
                            "transaction_count": m["count"],
                            "transaction_amount": m["amount"],
                        })
            else:
                # Some files use hoverData dict format
                hover_data = raw["data"].get("hoverData", {})
                if isinstance(hover_data, dict):
                    for district, info in hover_data.items():
                        if isinstance(info, dict) and "metric" in info:
                            for m in info["metric"]:
                                rows.append({
                                    "state": state,
                                    "year": year,
                                    "quarter": quarter,
                                    "district": district,
                                    "transaction_count": m["count"],
                                    "transaction_amount": m["amount"],
                                })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 5. MAP USER
# ============================================================
def extract_map_user():
    """
    Columns: state, year, quarter, district, registered_users, app_opens
    """
    print("[5/9] Extracting map_user ...")
    rows = []
    path = os.path.join(DATA_DIR, "map", "user")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            hover_data = raw["data"].get("hoverData", {})
            if isinstance(hover_data, dict):
                for district, info in hover_data.items():
                    rows.append({
                        "state": state,
                        "year": year,
                        "quarter": quarter,
                        "district": district,
                        "registered_users": info.get("registeredUsers", 0),
                        "app_opens": info.get("appOpens", 0),
                    })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 6. MAP INSURANCE
# ============================================================
def extract_map_insurance():
    """
    Columns: state, year, quarter, district, transaction_count, transaction_amount
    
    Map insurance data at state level may use hoverDataList (district-level),
    while country level uses grid-based lat/lng data. We extract both.
    """
    print("[6/9] Extracting map_insurance ...")
    rows = []
    path = os.path.join(DATA_DIR, "map", "insurance")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            # Try hoverDataList format first (state-level district data)
            hover_list = raw["data"].get("hoverDataList", [])
            if hover_list:
                for item in hover_list:
                    district = item["name"]
                    for m in item["metric"]:
                        rows.append({
                            "state": state,
                            "year": year,
                            "quarter": quarter,
                            "district": district,
                            "transaction_count": m["count"],
                            "transaction_amount": m["amount"],
                        })
            else:
                # Country-level grid data — extract label as district
                grid = raw["data"].get("data", {})
                if isinstance(grid, dict) and "data" in grid:
                    for point in grid["data"]:
                        # [lat, lng, metric, label]
                        if len(point) >= 4:
                            rows.append({
                                "state": state,
                                "year": year,
                                "quarter": quarter,
                                "district": str(point[3]),
                                "transaction_count": int(point[2]),
                                "transaction_amount": 0,
                            })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 7. TOP TRANSACTION
# ============================================================
def extract_top_transaction():
    """
    Columns: state, year, quarter, entity_name, entity_type, transaction_count, transaction_amount
    entity_type is one of: 'state', 'district', 'pincode'
    """
    print("[7/9] Extracting top_transaction ...")
    rows = []
    path = os.path.join(DATA_DIR, "top", "transaction")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            data = raw["data"]
            for entity_type, key in [("state", "states"), ("district", "districts"), ("pincode", "pincodes")]:
                items = data.get(key, [])
                if items:
                    for item in items:
                        metric = item.get("metric", {})
                        rows.append({
                            "state": state,
                            "year": year,
                            "quarter": quarter,
                            "entity_name": item.get("entityName", ""),
                            "entity_type": entity_type,
                            "transaction_count": metric.get("count", 0),
                            "transaction_amount": metric.get("amount", 0),
                        })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 8. TOP USER
# ============================================================
def extract_top_user():
    """
    Columns: state, year, quarter, entity_name, entity_type, registered_users
    """
    print("[8/9] Extracting top_user ...")
    rows = []
    path = os.path.join(DATA_DIR, "top", "user")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            data = raw["data"]
            for entity_type, key in [("state", "states"), ("district", "districts"), ("pincode", "pincodes")]:
                items = data.get(key, [])
                if items:
                    for item in items:
                        rows.append({
                            "state": state,
                            "year": year,
                            "quarter": quarter,
                            "entity_name": item.get("name", ""),
                            "entity_type": entity_type,
                            "registered_users": item.get("registeredUsers", 0),
                        })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# 9. TOP INSURANCE
# ============================================================
def extract_top_insurance():
    """
    Columns: state, year, quarter, entity_name, entity_type, transaction_count, transaction_amount
    """
    print("[9/9] Extracting top_insurance ...")
    rows = []
    path = os.path.join(DATA_DIR, "top", "insurance")
    for state, year, quarter, raw in walk_json_files(path):
        try:
            data = raw["data"]
            for entity_type, key in [("state", "states"), ("district", "districts"), ("pincode", "pincodes")]:
                items = data.get(key, [])
                if items:
                    for item in items:
                        metric = item.get("metric", {})
                        rows.append({
                            "state": state,
                            "year": year,
                            "quarter": quarter,
                            "entity_name": item.get("entityName", ""),
                            "entity_type": entity_type,
                            "transaction_count": metric.get("count", 0),
                            "transaction_amount": metric.get("amount", 0),
                        })
        except (KeyError, TypeError):
            pass
    df = pd.DataFrame(rows)
    print(f"       -> {len(df)} rows")
    return df


# ============================================================
# LOAD INTO SQLITE
# ============================================================
def load_to_sqlite(dataframes: dict):
    """
    Takes a dict of {table_name: DataFrame} and writes each to SQLite.
    """
    print(f"\nLoading data into SQLite database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    for table_name, df in dataframes.items():
        if df.empty:
            print(f"  [SKIP] {table_name} — empty DataFrame")
            continue
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"  [OK]   {table_name} — {len(df)} rows written")
    conn.close()
    print("\nDatabase created successfully!")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  PhonePe Pulse ETL Pipeline")
    print("=" * 60)
    print(f"  Data source : {DATA_DIR}")
    print(f"  Database    : {DB_PATH}")
    print("=" * 60)

    # Extract all 9 datasets
    tables = {
        "aggregated_transaction": extract_aggregated_transaction(),
        "aggregated_user": extract_aggregated_user(),
        "aggregated_insurance": extract_aggregated_insurance(),
        "map_transaction": extract_map_transaction(),
        "map_user": extract_map_user(),
        "map_insurance": extract_map_insurance(),
        "top_transaction": extract_top_transaction(),
        "top_user": extract_top_user(),
        "top_insurance": extract_top_insurance(),
    }

    # Load into SQLite
    load_to_sqlite(tables)

    # Summary
    print("\n" + "=" * 60)
    print("  ETL Summary")
    print("=" * 60)
    total_rows = 0
    for name, df in tables.items():
        total_rows += len(df)
        print(f"  {name:30s} : {len(df):>8,} rows")
    print(f"  {'TOTAL':30s} : {total_rows:>8,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
