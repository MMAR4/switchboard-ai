import os
import time
from typing import Dict, List, Any
import sqlglot
from sqlglot import parse_one, exp
import duckdb
from deltalake import DeltaTable


class SwitchboardASTParser:
    """Parses SQL queries using SQLGlot to extract referenced tables and analyze query complexity."""

    def __init__(self, query: str, dialect: str = "spark"):
        self.raw_query = query
        self.dialect = dialect
        self.ast = parse_one(query, read=dialect)

    def extract_tables(self) -> List[str]:
        """Extracts unique table names referenced in the query."""
        tables = []
        for table in self.ast.find_all(exp.Table):
            table_name = table.sql(dialect=self.dialect)
            if table_name not in tables:
                tables.append(table_name)
        return tables

    def analyze_complexity(self) -> Dict[str, Any]:
        """Inspects query AST for joins, aggregations, and subqueries."""
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
    """Reads underlying Delta Lake table metadata using delta-rs without reading full data files into memory."""

    def __init__(self, table_path_map: Dict[str, str]):
        self.table_path_map = table_path_map

    def get_table_metrics(self, table_name: str) -> Dict[str, Any]:
        if table_name not in self.table_path_map:
            return {"table_name": table_name, "size_gb": 0.0, "size_bytes": 0, "error": "Path not mapped"}

        path = self.table_path_map[table_name]
        try:
            dt = DeltaTable(path)
            files = dt.files()
            
            # Calculate total file size from Delta metadata
            total_bytes = 0
            for f in files:
                full_file_path = os.path.join(path, f)
                if os.path.exists(full_file_path):
                    total_bytes += os.path.getsize(full_file_path)

            size_gb = total_bytes / (1024 ** 3)
            return {
                "table_name": table_name,
                "path": path,
                "size_bytes": total_bytes,
                "size_gb": round(size_gb, 6),
                "num_files": len(files),
                "version": dt.version(),
            }
        except Exception as e:
            return {"table_name": table_name, "error": str(e), "size_gb": 0.0, "size_bytes": 0}


class SwitchboardRouter:
    """Smart Compute Router: Intercepts queries and chooses between DuckDB and Spark."""

    def __init__(self, table_path_map: Dict[str, str], max_duckdb_gb: float = 10.0):
        self.table_path_map = table_path_map
        self.max_duckdb_gb = max_duckdb_gb

    def route_query(self, query: str) -> Dict[str, Any]:
        parser = SwitchboardASTParser(query)
        tables = parser.extract_tables()
        complexity = parser.analyze_complexity()

        estimator = DeltaMetadataEstimator(self.table_path_map)
        total_workload_size_gb = 0.0
        table_breakdown = []

        for table in tables:
            metrics = estimator.get_table_metrics(table)
            table_breakdown.append(metrics)
            total_workload_size_gb += metrics.get("size_gb", 0.0)

        # Routing decision logic
        if total_workload_size_gb < self.max_duckdb_gb:
            target_engine = "DUCKDB"
            reason = f"Workload size ({total_workload_size_gb:.4f} GB) is below single-node threshold ({self.max_duckdb_gb} GB)."
        else:
            target_engine = "DATABRICKS_SPARK"
            reason = f"Workload size ({total_workload_size_gb:.4f} GB) exceeds single-node threshold ({self.max_duckdb_gb} GB)."

        return {
            "query": query,
            "target_engine": target_engine,
            "routing_reason": reason,
            "total_size_gb": round(total_workload_size_gb, 6),
            "complexity_metrics": complexity,
            "tables_analyzed": table_breakdown,
        }

    def execute(self, query: str) -> Any:
        """Executes query using the target engine decision."""
        decision = self.route_query(query)
        print(f"\n[SWITCHBOARD ROUTER DECISION]: {decision['target_engine']}")
        print(f"[REASON]: {decision['routing_reason']}")

        start_time = time.time()

        if decision["target_engine"] == "DUCKDB":
            # Direct execution in DuckDB
            result = duckdb.sql(query).fetchall()
            elapsed_time = time.time() - start_time
            print(f"[DUCKDB EXECUTION SUCCESSFUL]: Completed in {elapsed_time:.4f} seconds.")
            return result
        else:
            # Fallback delegation to PySpark
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("SwitchboardFallback").getOrCreate()
            result = spark.sql(query).collect()
            elapsed_time = time.time() - start_time
            print(f"[SPARK EXECUTION SUCCESSFUL]: Completed in {elapsed_time:.4f} seconds.")
            return result


# ==========================================
# LOCAL DEMO & BENCHMARK
# ==========================================
if __name__ == "__main__":
    # Create local directory and sample DuckDB memory table for testing
    duckdb.sql("CREATE TABLE sales AS SELECT 1 as sale_id, 'India' as country, 150.0 as amount")

    sample_sql = "SELECT country, SUM(amount) as total_sales FROM sales GROUP BY country"

    # Mapping tables to dummy paths for size estimation test
    MOCK_PATHS = {"sales": "./data/delta/sales"}

    print("==================================================")
    print("      SWITCHBOARD AI - COMPUTE ROUTER MVP 1      ")
    print("==================================================")

    router = SwitchboardRouter(table_path_map=MOCK_PATHS, max_duckdb_gb=10.0)
    output = router.execute(sample_sql)



    print("\nQueryResult:", output)