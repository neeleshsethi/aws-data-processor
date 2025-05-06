"""
Utility functions for the California Housing data processing pipeline.
"""
import os
import json
import boto3
from typing import Dict
from loguru import logger

def setup_logging() -> None:
    """
    Set up and configure the logger.
    """
    # Configure loguru logger
    logger.remove()  # Remove default handler
    logger.add(
        lambda msg: print(msg),
        format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
        level="INFO"
    )

def get_db_credentials() -> Dict[str, str]:
    """
    Retrieve database credentials from Secrets Manager or environment variables.
    
    Returns:
        Dictionary containing database connection parameters
    """
    # Check if we should use AWS Secrets Manager
    secret_name = os.environ.get("DB_SECRET_NAME")
    
    if secret_name:
        logger.info(f"Retrieving database credentials from Secrets Manager: {secret_name}")
        return _get_secret_from_secrets_manager(secret_name)
    else:
        logger.info("Using database credentials from environment variables")
        return {
            "host": os.environ["DB_HOST"],
            "port": os.environ.get("DB_PORT", "5432"),
            "dbname": os.environ["DB_NAME"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"]
        }

def _get_secret_from_secrets_manager(secret_name: str) -> Dict[str, str]:
    """
    Get a secret from AWS Secrets Manager.
    
    Args:
        secret_name: Name of the secret in Secrets Manager
        
    Returns:
        Dictionary containing the secret values
        
    Raises:
        Exception: If the secret cannot be retrieved
    """
    # Create a Secrets Manager client
    client = boto3.client(service_name='secretsmanager')
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret
    except Exception as e:
        logger.error(f"Error retrieving secret from Secrets Manager: {str(e)}")
        raise

def format_query_results(results):
        """
        Format database query results into a readable string.
        
        Args:
            results: List of tuples containing query results
            
        Returns:
            Formatted string representation of the results
        """
        if not results:
            return "No results found."
        
        # Extract timestamp from first record (they should all be the same)
        timestamp = results[0][3].strftime('%Y-%m-%d %H:%M:%S')
        
        # Create header
        header = f"California Housing Data Summary (processed at {timestamp})\n"
        header += "-" * 80 + "\n"
        header += f"{'Category':<15} {'Average Value ($)':>20} {'Record Count':>15}\n"
        header += "-" * 80
        
        # Format each row
        rows = []
        for category, avg_value, count, _ in results:
            # Format the average value with commas and 2 decimal places
            formatted_value = f"${float(avg_value):,.2f}"
            formatted_count = f"{count:,}"
            rows.append(f"{category:<15} {formatted_value:>20} {formatted_count:>15}")
        
        # Combine everything
        formatted_result = header + "\n" + "\n".join(rows)
        
        return formatted_result