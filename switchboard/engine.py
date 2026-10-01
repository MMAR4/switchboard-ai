import logging
from typing import Union
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import pyspark.sql.dataframe
except ImportError:
    pass

from .inspectors import get_s3_payload_size_mb
from .duckdb_runner import DuckDBRunner
from .spark_runner import SparkRunner

logger = logging.getLogger("switchboard_ai.engine")

class SwitchboardEngine:
    def __init__(self, threshold_mb: float = 100.0, max_duckdb_memory: str = "4GB"):
        self.threshold_mb = threshold_mb
        self.max_duckdb_memory = max_duckdb_memory
        self.duckdb_runner = None
        self.spark_runner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.duckdb_runner:
            self.duckdb_runner.close()
        if self.spark_runner:
            self.spark_runner.close()

    def execute(
        self,
        s3_input_path: str,
        sql_query: str,
        s3_output_path: str = None
    ) -> Union['pd.DataFrame', 'pyspark.sql.dataframe.DataFrame', None]:
        """
        1. Inspects s3_input_path size.
        2. Routes to DuckDB if size <= threshold_mb, else PySpark.
        3. Executes sql_query.
        4. Writes Parquet to s3_output_path if provided, else returns DataFrame.
        """
        try:
            logger.info(f"Starting execution for input: {s3_input_path}")
            payload_size_mb = get_s3_payload_size_mb(s3_input_path)
            
            logger.info(f"Payload size: {payload_size_mb:.2f} MB. Threshold: {self.threshold_mb} MB")

            if payload_size_mb <= self.threshold_mb:
                logger.info("Routing to DuckDB engine.")
                if not self.duckdb_runner:
                    self.duckdb_runner = DuckDBRunner(max_memory=self.max_duckdb_memory)
                return self.duckdb_runner.execute_query(sql_query, s3_output_path)
            else:
                logger.info("Routing to PySpark engine.")
                if not self.spark_runner:
                    self.spark_runner = SparkRunner()
                return self.spark_runner.execute_query(sql_query, s3_output_path)
                
        except Exception as e:
            logger.error(f"Switchboard execution failed: {e}")
            raise
