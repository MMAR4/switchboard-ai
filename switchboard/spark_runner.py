import logging
import os
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.dataframe
except ImportError:
    SparkSession = None

logger = logging.getLogger("switchboard_ai.spark_runner")

class SparkRunner:
    def __init__(self):
        self.spark = None

    def connect(self):
        if SparkSession is None:
            raise ImportError("PySpark is not installed. Please install pyspark to use the SparkRunner.")
            
        try:
            logger.info("Checking for active SparkSession...")
            self.spark = SparkSession.getActiveSession()
            
            if self.spark:
                logger.info("Using existing active SparkSession.")
            else:
                logger.info("No active SparkSession found. Building a new one...")
                builder = SparkSession.builder.appName("SwitchboardAI")
                
                # Add Hadoop AWS package if running locally/EC2
                builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
                
                self.spark = builder.getOrCreate()
                
                # Configure S3A
                hadoop_conf = self.spark._jsc.hadoopConfiguration()
                hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                
                logger.info("New SparkSession created successfully.")
                
        except Exception as e:
            logger.error(f"Failed to initialize PySpark: {e}")
            raise

    def close(self):
        # We generally don't want to kill the active session if we didn't create it specifically as a standalone,
        # but for cleanup purposes, if we need to:
        # In a managed environment like Databricks, closing session might break the notebook.
        pass

    def execute_query(self, sql_query: str, s3_output_path: str = None) -> 'pyspark.sql.dataframe.DataFrame':
        if not self.spark:
            self.connect()

        try:
            logger.info("Executing query in PySpark...")
            df = self.spark.sql(sql_query)
            
            if s3_output_path:
                logger.info(f"Writing output to {s3_output_path}")
                # Ensure output path uses s3a for Spark if it starts with s3://
                if s3_output_path.startswith("s3://"):
                    s3_output_path = s3_output_path.replace("s3://", "s3a://", 1)
                    
                df.write.mode("overwrite").parquet(s3_output_path)
                return None
            else:
                logger.info("Returning results as PySpark DataFrame")
                return df
        except Exception as e:
            logger.error(f"PySpark query execution failed: {e}")
            raise
