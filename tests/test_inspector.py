import pytest
from unittest.mock import patch, MagicMock
from switchboard.inspectors import get_s3_payload_size_mb

@patch('boto3.client')
def test_get_s3_payload_size_mb_exact_path(mock_boto3_client):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    
    mock_paginator.paginate.return_value = [
        {
            'Contents': [
                {'Key': 'data/file1.parquet', 'Size': 1048576}, # 1MB
                {'Key': 'data/file2.parquet', 'Size': 2097152}  # 2MB
            ]
        }
    ]
    
    size = get_s3_payload_size_mb("s3://my-bucket/data/")
    assert size == 3.0
    mock_s3.get_paginator.assert_called_with('list_objects_v2')
    mock_paginator.paginate.assert_called_with(Bucket='my-bucket', Prefix='data/')

@patch('boto3.client')
def test_get_s3_payload_size_mb_wildcard(mock_boto3_client):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    
    mock_paginator.paginate.return_value = [
        {
            'Contents': [
                {'Key': 'data/file1.parquet', 'Size': 1048576}, # 1MB
                {'Key': 'data/file1.json', 'Size': 1048576}     # 1MB
            ]
        }
    ]
    
    size = get_s3_payload_size_mb("s3://my-bucket/data/*.parquet")
    assert size == 1.0 # Only .parquet should be counted
    mock_paginator.paginate.assert_called_with(Bucket='my-bucket', Prefix='data/')

def test_invalid_scheme():
    with pytest.raises(ValueError):
        get_s3_payload_size_mb("http://example.com/data")
