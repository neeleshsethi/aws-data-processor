"""
Lambda handler for California Housing data processing pipeline.
Triggered by S3 upload events and processes housing data using Pandas.
"""
import os
import json
import urllib.parse
import boto3
import traceback
from typing import Dict, Any, List, Tuple

from lambda_functions.data_processor import process_california_housing_data
from lambda_functions.db_connector import RDSConnector
from lambda_functions.utils import setup_logging, get_db_credentials, format_query_results
from loguru import logger

# Configure logging
setup_logging()

# Initialize AWS clients
s3_client = boto3.client('s3')

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function that processes S3 events.
    
    Args:
        event: The event dict from AWS Lambda trigger
        context: The Lambda context object
        
    Returns:
        Dict containing status and processing results
    """
    logger.info("Processing new California Housing data file")
    
    try:
        # Extract bucket and key from the S3 event
        bucket, key = _extract_s3_info(event)
        logger.info(f"Processing file {key} from bucket {bucket}")
        # Download file from S3
        download_path = f"/tmp/{os.path.basename(key)}"
        s3_client.download_file(bucket, key, download_path)
        logger.info(f"Downloaded file to {download_path}")
        
        # Process data using Pandas
        summary_stats = process_california_housing_data(download_path)
        logger.info(f"Successfully processed data. Found {len(summary_stats)} categories.")
        
        # Store results in RDS
        db_credentials = get_db_credentials()
        with RDSConnector(db_credentials) as db:
            db.store_summary_statistics(summary_stats)
            logger.info("Successfully stored summary statistics in the database")
            logger.info("Querying database to validate insertion")
            latest_stats = db.query_latest_statistics()
            logger.info(f"Successfully retrieved {len(latest_stats)} records from database")
            formatted_results = format_query_results(latest_stats)
            logger.info(f"Housing data summary:\n{formatted_results}")
        
        # Clean up
        os.remove(download_path)
        logger.info(f"Removed temporary file {download_path}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully processed housing data",
                "categories_processed": len(summary_stats)
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing housing data: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error processing housing data",
                "error": str(e)
            })
        }

def _extract_s3_info(event: Dict[str, Any]) -> Tuple[str, str]:
    """
    Extract the S3 bucket and key from an S3 event.
    
    Args:
        event: The S3 event dictionary
        
    Returns:
        Tuple containing (bucket_name, object_key)
        
    Raises:
        ValueError: If the event is not a valid S3 event
    """
    try:
        # Get the S3 bucket and object information from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key']
        )
        
        return bucket, key
    except (KeyError, IndexError) as e:
        logger.error(f"Invalid S3 event structure: {e}")
        raise ValueError(f"Invalid S3 event structure: {e}")