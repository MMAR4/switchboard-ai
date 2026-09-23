import os
import time
import duckdb
from typing import Dict, List, Any
import sqlglot
from sqlglot import parse_one, exp
from deltalake import DeltaTable, write_deltalake
import pandas as pd


class SwitchboardASTParser:
    """Parses SQL queries using SQLGlot to extract tables, joins, operations, and Dialect targets."""

    def __init__(self, query: str, dialect: str = "spark"):
        self.raw_query = query
        self.dialect = dialect
        self.ast = parse_one(query, read=dialect)

    def extract_tables(self) -> List[str]:
        """Extracts all target table names referenced in the query."""
        tables = []
        for table in self.ast.find_all(exp.Table):
            # table.name extracts 'orders' instead of 'orders AS o'
            table_name = table.name
            if table_name and table_name not in tables:
                tables.append(table_name)
        return tables

    def analyze_complexity(self) -> Dict[str, Any]:
        """Inspects AST for heavy operations."""
        joins = list(self.ast.find_all(exp.Join))
        aggregations = list(self.ast.find_all(exp.Group))
        window_funcs = list(self.ast.find_all(exp.Window))

        return {
            "join_count": len(joins),
            "has_aggregations": len(aggregations) > 0,
            "has_window_functions": len(window_funcs) > 0,
            "cte_count": len(list(self.ast.find_all(exp.CTE))),
        }


class DeltaMetadataEstimator:
    """Reads underlying Delta Lake table metadata using delta-rs without loading active dataframes into memory."""

    def __init__(self, table_path_map: Dict[str, str]):
        self.table_path_map = table_path_map

    def get_table_metrics(self, table_name: str) -> Dict[str, Any]:
        """Calculates byte size directly from local Delta/Parquet storage."""
        if table_name not in self.table_path_map:
            raise ValueError(
                f"Table path mapping not found for table: '{table_name}'")

        path = self.table_path_map[table_name]

        try:
            total_bytes = 0
            file_count = 0
            if os.path.exists(path):
                for root, _, files in os.walk(path):
                    for f in files:
                        if f.endswith('.parquet'):
                            total_bytes += os.path.getsize(
                                os.path.join(root, f))
                            file_count += 1

            size_gb = total_bytes / (1024 ** 3)

            return {
                "table_name": table_name,
                "path": path,
                "size_bytes": total_bytes,
                "size_gb": round(size_gb, 6),
                "num_files": file_count,
            }
        except Exception as e:
            return {
                "table_name": table_name,
                "error": str(e),
                "size_gb": 0.0,
                "size_bytes": 0
            }


class SwitchboardExecutor:
    """Handles query execution on DuckDB or PySpark and measures latency."""

    def __init__(self, table_path_map: Dict[str, str]):
        self.table_path_map = table_path_map

    def execute_duckdb(self, query: str) -> Dict[str, Any]:
        start_time = time.time()

        # Connect to DuckDB and register tables as views pointing to parquet/delta paths
        con = duckdb.connect(database=":memory:")
        for table_name, path in self.table_path_map.items():
            # DuckDB reads parquet files directly from the Delta path
            parquet_glob = os.path.join(path, "*.parquet")
            con.execute(
                f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_glob}')")

        # Execute Query
        result = con.execute(query).fetchdf()
        elapsed_time = time.time() - start_time
        con.close()

        return {
            "engine": "DuckDB",
            "execution_time_seconds": round(elapsed_time, 4),
            "row_count": len(result),
            "sample_result": result.head(3).to_dict(orient="records")
        }

    def execute_pyspark(self, query: str) -> Dict[str, Any]:
        start_time = time.time()

        # Import PySpark locally inside execution to measure JVM spin-up overhead
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .appName("Switchboard-Spark-Fallback") \
            .master("local[*]") \
            .getOrCreate()

        for table_name, path in self.table_path_map.items():
            df = spark.read.format("parquet").load(
                os.path.join(path, "*.parquet"))
            df.createOrReplaceTempView(table_name)

        result_df = spark.sql(query)
        result_pd = result_df.toPandas()

        elapsed_time = time.time() - start_time
        spark.stop()

        return {
            "engine": "PySpark",
            "execution_time_seconds": round(elapsed_time, 4),
            "row_count": len(result_pd),
            "sample_result": result_pd.head(3).to_dict(orient="records")
        }


class SwitchboardRouter:
    """Core routing engine combining AST analysis, Delta metadata estimations, and execution routing."""

    def __init__(self, table_path_map: Dict[str, str], max_duckdb_gb: float = 10.0):
        self.table_path_map = table_path_map
        self.max_duckdb_gb = max_duckdb_gb
        self.executor = SwitchboardExecutor(table_path_map)

    def route_and_execute(self, query: str) -> Dict[str, Any]:
        parser = SwitchboardASTParser(query)
        tables = parser.extract_tables()
        complexity = parser.analyze_complexity()

        estimator = DeltaMetadataEstimator(self.table_path_map)

        total_workload_size_gb = 0.0
        table_breakdown = []

        for table in tables:
            metrics = estimator.get_table_metrics(table)
            table_breakdown.append(metrics)
            total_workload_size_gb += metrics.get("size_gb", 0)

        # Routing decision
        if total_workload_size_gb < self.max_duckdb_gb:
            target_engine = "DUCKDB"
            reason = f"Workload size ({total_workload_size_gb:.6f} GB) is below single-node threshold ({self.max_duckdb_gb} GB)."
            execution_metrics = self.executor.execute_duckdb(query)
        else:
            target_engine = "DATABRICKS_SPARK"
            reason = f"Workload size ({total_workload_size_gb:.6f} GB) exceeds single-node threshold ({self.max_duckdb_gb} GB)."
            execution_metrics = self.executor.execute_pyspark(query)

        return {
            "query": query,
            "target_engine": target_engine,
            "routing_reason": reason,
            "total_size_gb": round(total_workload_size_gb, 6),
            "complexity_metrics": complexity,
            "tables_analyzed": table_breakdown,
            "execution_metrics": execution_metrics
        }


# ==========================================
# DATA GENERATOR & BENCHMARK HELPER
# ==========================================
def generate_sample_delta_data(base_dir="./data/delta"):
    """Generates mock customer and orders datasets stored as Delta/Parquet files."""
    os.makedirs(f"{base_dir}/orders", exist_ok=True)
    os.makedirs(f"{base_dir}/customers", exist_ok=True)

    # 10,000 orders (matching 10000 elements across all columns)
    orders_df = pd.DataFrame({
        "order_id": range(1, 10001),
        "customer_id": [i % 500 for i in range(10000)],
        "amount": [round(10.5 * (i % 20), 2) for i in range(10000)],
        "order_date": ["2026-01-15"] * 10000
    })

    # 500 customers
    customers_df = pd.DataFrame({
        "customer_id": range(500),
        "name": [f"Customer_{i}" for i in range(500)]
    })

    write_deltalake(f"{base_dir}/orders", orders_df, mode="overwrite")
    write_deltalake(f"{base_dir}/customers", customers_df, mode="overwrite")


if __name__ == "__main__":
    print("--- 1. Generating Mock Data for Switchboard ---")
    generate_sample_delta_data()

    table_paths = {
        "orders": "./data/delta/orders",
        "customers": "./data/delta/customers"
    }

    sample_sql = """
    SELECT 
        c.customer_id, 
        c.name, 
        SUM(o.amount) AS total_spent
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2026-01-01'
    GROUP BY c.customer_id, c.name
    ORDER BY total_spent DESC
    """

    print("\n--- 2. Running Switchboard Auto-Router ---")
    router = SwitchboardRouter(table_path_map=table_paths, max_duckdb_gb=10.0)
    decision = router.route_and_execute(sample_sql)

    print(f"\n[Target Engine Selected]: {decision['target_engine']}")
    print(f"[Reason]: {decision['routing_reason']}")
    print(
        f"[Execution Engine Used]: {decision['execution_metrics']['engine']}")
    print(
        f"[Execution Time]: {decision['execution_metrics']['execution_time_seconds']} seconds")
    print(f"[Rows Returned]: {decision['execution_metrics']['row_count']}")
    print(
        f"[Sample Results]: {decision['execution_metrics']['sample_result']}")

    print("\n--- 3. Running Benchmark Comparison (DuckDB vs PySpark) ---")
    executor = SwitchboardExecutor(table_paths)
    duck_res = executor.execute_duckdb(sample_sql)
    spark_res = executor.execute_pyspark(sample_sql)

    print(f"\n⚡ DuckDB Execution Time: {duck_res['execution_time_seconds']}s")
    print(f"🐢 PySpark Execution Time: {spark_res['execution_time_seconds']}s")

    speedup = round(spark_res['execution_time_seconds'] /
                    duck_res['execution_time_seconds'], 2)
    print(
        f"\n🚀 Result: DuckDB was {speedup}x FASTER than Spark for this sub-10GB query!")
