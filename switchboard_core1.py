import os
import time
import duckdb
import sqlglot
from sqlglot import parse_one, exp
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

# Standard Databricks Serverless SQL rate baseline ($/hr)
DBU_COST_PER_HOUR = 0.70


class SwitchboardEngine:
    def __init__(self, threshold_gb: float = 1.0):
        self.threshold_gb = threshold_gb
        self.hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.token = os.getenv("DATABRICKS_TOKEN")

    def _get_table_size_gb(self, full_table_name: str) -> float:
        """Fetch Delta metadata size without scanning data files."""
        if not full_table_name:
            return 0.0

        query = f"DESCRIBE DETAIL {full_table_name}"
        with sql.connect(
            server_hostname=self.hostname,
            http_path=self.http_path,
            access_token=self.token
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                cols = [c[0] for c in cursor.description]
                row = dict(zip(cols, result[0]))
                return row.get("sizeInBytes", 0) / (1024 ** 3)

    def _extract_table_name(self, query: str) -> str:
        """Extract target table name using sqlglot AST parser."""
        try:
            parsed = parse_one(query, read="spark")
            tables = [t.sql(dialect="spark")
                      for t in parsed.find_all(exp.Table)]
            return tables[0] if tables else ""
        except Exception:
            return ""

    def execute(self, query: str):
        """Core Switchboard Router with FinOps Telemetry."""
        start_time = time.time()
        clean_query = query.strip().rstrip(";")
        table_name = self._extract_table_name(clean_query)

        print("\n" + "="*60)
        print("🔀 SWITCHBOARD SMART COMPUTE ROUTER")
        print("="*60)
        print(f"🔍 Query Received       : {clean_query}")
        print(
            f"📌 Table Identified      : '{table_name if table_name else 'None (System/Utility Query)'}'")

        # 1. Handle system/catalog/utility commands (e.g., SHOW TABLES, SELECT CURRENT_SCHEMA())
        if not table_name:
            print("ℹ️ Directing utility/system command to Databricks SQL Warehouse...")
            return self._execute_databricks(clean_query, start_time)

        # 2. Metadata Inspection for Data Queries
        table_size_gb = self._get_table_size_gb(table_name)
        print(
            f"📊 Dataset Size         : {table_size_gb:.6f} GB ({table_size_gb * 1024:.2f} MB)")

        # 3. Routing Decision
        if table_size_gb < self.threshold_gb:
            print(f"🎯 Route Decision       : DUCKDB (Single-Node Local)")
            try:
                return self._execute_duckdb(clean_query, table_name, start_time)
            except Exception as e:
                print(
                    f"\n⚠️ DuckDB execution failed ({e}). Falling back to Databricks...")
                return self._execute_databricks(clean_query, start_time)
        else:
            print(f"🔥 Route Decision       : DATABRICKS SPARK (Distributed Cluster)")
            return self._execute_databricks(clean_query, start_time)

    def _execute_duckdb(self, query: str, table_name: str, start_time: float):
        """Fetch Arrow stream & execute query inside local DuckDB engine."""
        fetch_query = f"SELECT * FROM {table_name}"

        with sql.connect(
            server_hostname=self.hostname,
            http_path=self.http_path,
            access_token=self.token
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(fetch_query)
                arrow_table = cursor.fetchall_arrow()

        con = duckdb.connect()
        con.register("raw_data", arrow_table)

        local_query = query.replace(table_name, "raw_data")
        result_df = con.execute(local_query).df()

        elapsed = time.time() - start_time
        estimated_databricks_cost = (elapsed / 3600) * DBU_COST_PER_HOUR

        print("-" * 60)
        print("📈 TELEMETRY & FINOPS METRICS:")
        print(f"⚡ Execution Engine    : DuckDB Vectorized C++")
        print(f"⏱️ Total Execution Time: {elapsed:.4f} seconds")
        print(
            f"💰 Cloud Compute Cost   : $0.00 (Saved est. ${estimated_databricks_cost:.6f} in DBUs)")
        print("=" * 60)
        return result_df

    def _execute_databricks(self, query: str, start_time: float):
        """Execute directly on remote Databricks SQL Warehouse."""
        with sql.connect(
            server_hostname=self.hostname,
            http_path=self.http_path,
            access_token=self.token
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                try:
                    # Preferred zero-copy path using PyArrow
                    result_df = cursor.fetchall_arrow().to_pandas()
                except Exception:
                    # Fallback for catalog/DDL queries that don't emit Arrow record batches
                    rows = cursor.fetchall()
                    cols = [c[0] for c in cursor.description] if cursor.description else []
                    import pandas as pd
                    result_df = pd.DataFrame(rows, columns=cols)
        
        elapsed = time.time() - start_time
        estimated_cost = (elapsed / 3600) * DBU_COST_PER_HOUR

        print("-" * 60)
        print("📈 TELEMETRY & FINOPS METRICS:")
        print(f"🔥 Execution Engine    : Databricks Distributed Spark")
        print(f"⏱️ Total Execution Time: {elapsed:.4f} seconds")
        print(f"💰 Estimated Cost      : ${estimated_cost:.6f}")
        print("=" * 60)
        return result_df


if __name__ == "__main__":
    engine = SwitchboardEngine(threshold_gb=1.0)

    sample_query = """
    SELECT pickup_zip, COUNT(*) as total_trips, ROUND(AVG(trip_distance), 2) as avg_distance 
    FROM samples.nyctaxi.trips 
    GROUP BY pickup_zip 
    ORDER BY total_trips DESC 
    LIMIT 10
    """

    df = engine.execute(sample_query)
    print("\n--- Query Results ---")
    print(df)
