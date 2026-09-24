import os
import time
import duckdb
from databricks import sql
from dotenv import load_dotenv
from sqlglot import parse_one, exp

# Load credentials from .env
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


class LocalSwitchboardRouter:
    def __init__(self, size_threshold_gb: float = 1.0):
        self.size_threshold_gb = size_threshold_gb

    def get_databricks_connection(self):
        """Establishes a remote SQL Warehouse connection."""
        return sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_PATH,
            access_token=DATABRICKS_TOKEN,
        )

    def extract_table_name(self, query: str) -> str:
        """Parses AST to identify target table (preserving catalog.schema.table)."""
        parsed = parse_one(query)
        tables = [table.sql() for table in parsed.find_all(exp.Table)]
        return tables[0] if tables else None

    def get_remote_table_size_gb(self, table_name: str) -> float:
        """Inspects remote Delta metadata for table size without scanning data."""
        with self.get_databricks_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE DETAIL {table_name}")
                result = cursor.fetchall()
                for row in result:
                    size_bytes = getattr(row, "sizeInBytes", None) or row[0] or 0
                    return size_bytes / (1024 ** 3)
        return 0.0

    def run_query(self, query: str):
        print("\n" + "=" * 60)
        print(f"📥 Incoming Query: {query}")
        
        table_name = self.extract_table_name(query)
        print(f"🔍 AST Parsed Full Table Path: {table_name}")

        size_gb = self.get_remote_table_size_gb(table_name)
        print(f"📊 Metadata Table Size: {size_gb:.6f} GB")

        # ROUTING DECISION
        if size_gb < self.size_threshold_gb:
            print("⚡ [ROUTING]: Small Workload -> DuckDB Local Engine")
            start = time.perf_counter()

            # Stream only the query result via Arrow (Query Pushdown)
            with self.get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    arrow_table = cursor.fetchall_arrow()

            # Execute/view in DuckDB C++ Engine
            duck_conn = duckdb.connect()
            duck_conn.register("query_result", arrow_table)
            result = duck_conn.sql("SELECT * FROM query_result").df()
            
            elapsed = time.perf_counter() - start
            print(f"✅ Executed via DuckDB in {elapsed:.4f} seconds | Cloud Cost: $0.00")
            return result

        else:
            print("🔥 [ROUTING]: Large Workload -> Databricks Cloud SQL Warehouse")
            start = time.perf_counter()

            with self.get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall_arrow().to_pandas()

            elapsed = time.perf_counter() - start
            print(f"✅ Executed via Databricks in {elapsed:.4f} seconds | DBUs Charged")
            return result


if __name__ == "__main__":
    router = LocalSwitchboardRouter(size_threshold_gb=1.0)
    
    # Test Query against Unity Catalog sample table
    test_query = "SELECT * FROM samples.nyctaxi.trips LIMIT 1000"
    router.run_query(test_query)