import logging
import boto3
from urllib.parse import urlparse
import fnmatch

logger = logging.getLogger("switchboard_ai.inspectors")

def get_s3_payload_size_mb(s3_uri: str) -> float:
    """
    Calculates the total payload size of an S3 path in Megabytes.
    Supports prefixes and wildcards.
    """
    logger.info(f"Inspecting S3 path: {s3_uri}")
    
    parsed = urlparse(s3_uri)
    if parsed.scheme not in ("s3", "s3a"):
        raise ValueError(f"Invalid S3 URI scheme: {parsed.scheme}")
    
    bucket = parsed.netloc
    path = parsed.path.lstrip('/')
    
    prefix = path
    pattern = None
    
    if '*' in path or '?' in path:
        # It's a wildcard
        # E.g. folder/*.json -> prefix: folder/, pattern: *.json
        parts = path.split('/')
        prefix_parts = []
        pattern_parts = []
        found_wildcard = False
        for part in parts:
            if '*' in part or '?' in part or found_wildcard:
                found_wildcard = True
                pattern_parts.append(part)
            else:
                prefix_parts.append(part)
                
        prefix = '/'.join(prefix_parts)
        if prefix:
            prefix += '/'
        pattern = '/'.join(pattern_parts)

    try:
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        total_bytes = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if pattern:
                        # Match the remainder of the key against the pattern
                        # We extract the part of the key after the prefix
                        rel_key = key[len(prefix):] if prefix else key
                        if fnmatch.fnmatch(rel_key, pattern):
                            total_bytes += obj['Size']
                    else:
                        total_bytes += obj['Size']
                        
        total_mb = total_bytes / (1024 * 1024)
        logger.info(f"Total size for {s3_uri}: {total_mb:.2f} MB")
        return total_mb

    except Exception as e:
        logger.error(f"Failed to inspect S3 size for {s3_uri}: {e}")
        raise
