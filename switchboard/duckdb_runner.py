import logging
import duckdb
import os
import pandas as pd

logger = logging.getLogger("switchboard_ai.duckdb_runner")

class DuckDBRunner:
    def __init__(self, max_memory: str = "4GB"):
        self.max_memory = max_memory
        self.conn = None

    def connect(self):
        try:
            logger.info("Initializing DuckDB connection...")
            self.conn = duckdb.connect(database=':memory:')
            
            # Install and load extensions
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            self.conn.execute("INSTALL aws;")
            self.conn.execute("LOAD aws;")

            # Configure memory and spill
            self.conn.execute(f"SET max_memory = '{self.max_memory}';")
            
            # Ensure temp directory exists
            temp_dir = '/tmp/duckdb_spill'
            os.makedirs(temp_dir, exist_ok=True)
            self.conn.execute(f"SET temp_directory = '{temp_dir}';")
            
            # Load AWS Credentials
            # Attempt to use standard AWS credential chain
            self.conn.execute("CALL load_aws_credentials();")
            
            # Handle explicit environment variables if present (useful for overriding or specific auth)
            if 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
                self.conn.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
                self.conn.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
            if 'AWS_SESSION_TOKEN' in os.environ:
                self.conn.execute(f"SET s3_session_token='{os.environ['AWS_SESSION_TOKEN']}';")
            if 'AWS_REGION' in os.environ:
                self.conn.execute(f"SET s3_region='{os.environ['AWS_REGION']}';")

            logger.info(f"DuckDB initialized with max_memory={self.max_memory}")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB: {e}")
            if self.conn:
                self.conn.close()
            raise

    def close(self):
        if self.conn:
            logger.info("Closing DuckDB connection.")
            self.conn.close()
            self.conn = None

    def execute_query(self, sql_query: str, s3_output_path: str = None) -> pd.DataFrame:
        if not self.conn:
            self.connect()

        try:
            logger.info("Executing query in DuckDB...")
            
            if s3_output_path:
                logger.info(f"Writing output to {s3_output_path}")
                # Create a temporary view or run copy directly
                # If sql_query is a SELECT, we can wrap it
                write_query = f"COPY ({sql_query}) TO '{s3_output_path}' (FORMAT PARQUET);"
                self.conn.execute(write_query)
                return None
            else:
                logger.info("Returning results as Pandas DataFrame")
                df = self.conn.execute(sql_query).df()
                return df
        except Exception as e:
            logger.error(f"DuckDB query execution failed: {e}")
            raise
