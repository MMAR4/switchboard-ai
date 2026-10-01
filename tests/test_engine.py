import pytest
from unittest.mock import patch, MagicMock
from switchboard.engine import SwitchboardEngine

@patch('switchboard.engine.get_s3_payload_size_mb')
@patch('switchboard.engine.DuckDBRunner')
@patch('switchboard.engine.SparkRunner')
def test_engine_routes_to_duckdb(MockSparkRunner, MockDuckDBRunner, mock_get_size):
    mock_get_size.return_value = 50.0 # 50 MB, below threshold
    
    mock_duckdb_instance = MagicMock()
    MockDuckDBRunner.return_value = mock_duckdb_instance
    mock_duckdb_instance.execute_query.return_value = "duckdb_result"
    
    with SwitchboardEngine(threshold_mb=100.0) as engine:
        result = engine.execute("s3://bucket/data", "SELECT 1")
        
    assert result == "duckdb_result"
    mock_duckdb_instance.execute_query.assert_called_once_with("SELECT 1", None)
    MockSparkRunner.assert_not_called()

@patch('switchboard.engine.get_s3_payload_size_mb')
@patch('switchboard.engine.DuckDBRunner')
@patch('switchboard.engine.SparkRunner')
def test_engine_routes_to_spark(MockSparkRunner, MockDuckDBRunner, mock_get_size):
    mock_get_size.return_value = 150.0 # 150 MB, above threshold
    
    mock_spark_instance = MagicMock()
    MockSparkRunner.return_value = mock_spark_instance
    mock_spark_instance.execute_query.return_value = "spark_result"
    
    with SwitchboardEngine(threshold_mb=100.0) as engine:
        result = engine.execute("s3://bucket/data", "SELECT 1")
        
    assert result == "spark_result"
    mock_spark_instance.execute_query.assert_called_once_with("SELECT 1", None)
    MockDuckDBRunner.assert_not_called()
