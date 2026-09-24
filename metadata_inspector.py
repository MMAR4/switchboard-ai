import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

def get_table_size_gb(full_table_name: str) -> float:
    """
    Queries Delta metadata to return table size in Gigabytes without 
    scanning table rows.
    """
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    query = f"DESCRIBE DETAIL {full_table_name}"

    with sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            
            # Map column names to row values
            columns = [col[0] for col in cursor.description]
            row_dict = dict(zip(columns, result[0]))

            # Extract file size in bytes and convert to GB
            size_bytes = row_dict.get("sizeInBytes", 0)
            size_gb = size_bytes / (1024 ** 3)
            return size_gb

if __name__ == "__main__":
    # Using Databricks' built-in sample table
    target_table = "samples.nyctaxi.trips"
    print(f"Fetching zero-scan metadata for: {target_table}...")
    
    try:
        size_gb = get_table_size_gb(target_table)
        print(f"📊 Table Size: {size_gb:.4f} GB ({size_gb * 1024:.2f} MB)")
        
        # Switchboard routing threshold
        THRESHOLD_GB = 1.0
        if size_gb < THRESHOLD_GB:
            print(f"🎯 Route Target: DUCKDB (Local / Single-node Driver)")
        else:
            print(f"🔥 Route Target: DATABRICKS SPARK (Distributed Cluster)")
    except Exception as e:
        print("❌ Failed to fetch metadata:", e)