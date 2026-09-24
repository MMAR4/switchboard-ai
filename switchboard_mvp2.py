import time
import duckdb
from pyspark.sql import SparkSession
from sqlglot import parse_one, exp


class SwitchboardRouterMVP2:
    def __init__(self, size_threshold_gb: float = 10.0):
        self.size_threshold_gb = size_threshold_gb
        
        # Initialize local Spark Session to simulate Databricks engine locally
        print("[System]: Initializing local PySpark fallback engine...")
        self.spark = (
            SparkSession.builder
            .appName("SwitchboardSparkEngine")
            .master("local[*]")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def execute_query(self, query: str, dataset_size_gb: float):
        """Routes execution based on workload size and measures execution metrics."""
        print(f"\n" + "=" * 60)
        print(f"Incoming Workload Size: {dataset_size_gb} GB")
        print(f"Executing Query: {query.strip()}")
        print("=" * 60)

        # ROUTING BRANCH 1: Single-Node DuckDB Processing
        if dataset_size_gb < self.size_threshold_gb:
            print("--> [Routing Decision]: Single-Node Engine (DuckDB)")
            
            try:
                start_time = time.perf_counter()
                
                # Execute query via DuckDB
                duckdb_result = duckdb.sql(query).fetchall()
                
                elapsed_time = time.perf_counter() - start_time
                
                print(f"[Engine]: DuckDB Execution Successful!")
                print(f"[Latency]: {elapsed_time:.4f} seconds")
                print(f"[Estimated Cost]: ~$0.00001 (Local CPU)")
                return duckdb_result

            except Exception as e:
                print(f"[Warning]: DuckDB execution failed ({e}). Falling back to Spark...")
                return self._execute_on_spark(query)

        # ROUTING BRANCH 2: Heavy Engine (Spark Session)
        else:
            print("--> [Routing Decision]: Distributed Cluster Engine (Databricks/Spark)")
            return self._execute_on_spark(query)

    def _execute_on_spark(self, query: str):
        """Executes query using PySpark fallback engine."""
        start_time = time.perf_counter()
        
        spark_result = self.spark.sql(query).collect()
        
        elapsed_time = time.perf_counter() - start_time
        
        print(f"[Engine]: Spark Execution Successful!")
        print(f"[Latency]: {elapsed_time:.4f} seconds")
        print(f"[Estimated Cost]: ~$0.05 - $0.50 (Cluster DBUs)")
        return spark_result


# ==========================================
# BENCHMARK RUNNER
# ==========================================
if __name__ == "__main__":
    router = SwitchboardRouterMVP2(size_threshold_gb=10.0)

    # 1. Register Mock In-Memory Tables for Testing
    # DuckDB Setup
    duckdb.sql("CREATE TABLE sales AS SELECT range AS id, range * 10 AS amount FROM range(100000)")
    
    # Spark Setup
    df = router.spark.range(100000).selectExpr("id", "id * 10 as amount")
    df.createOrReplaceTempView("sales")

    sample_query = "SELECT SUM(amount) AS total_revenue FROM sales WHERE id > 50000"

    print("\n--- TEST CASE 1: Sub-10GB Query (Routes to DuckDB) ---")
    router.execute_query(query=sample_query, dataset_size_gb=0.5)

    print("\n--- TEST CASE 2: Over-10GB Query (Routes to Spark Engine) ---")
    router.execute_query(query=sample_query, dataset_size_gb=45.0)