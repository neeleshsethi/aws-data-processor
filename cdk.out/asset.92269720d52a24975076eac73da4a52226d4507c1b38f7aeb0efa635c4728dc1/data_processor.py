"""
Data processing module for California Housing dataset.
"""
import pandas as pd
from typing import List, Tuple, Dict, Any
from loguru import logger



def process_california_housing_data(file_path: str) -> List[Dict[str, Any]]:
    """
    Process California Housing dataset to calculate average median house value
    per ocean_proximity category.
    
    Args:
        file_path: Path to the CSV file containing California Housing data
        
    Returns:
        List of dictionaries with category and average value
        
    Raises:
        ValueError: If the data is missing required columns
        FileNotFoundError: If the file cannot be found
    """
    logger.info(f"Processing file: {file_path}")
    
    try:
        # Read the dataset
        df = pd.read_csv(file_path)
        
        # Validate required columns exist
        _validate_dataframe(df)
        
        # Clean data by removing rows with missing values
        original_size = len(df)
        df = df.dropna()
        cleaned_size = len(df)
        
        logger.info(f"Removed {original_size - cleaned_size} rows with missing values")
        
        # Calculate average median house value per ocean_proximity category
        result = calculate_average_by_category(df)
        
        logger.info(f"Calculated averages for {len(result)} categories")
        return result
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def _validate_dataframe(df: pd.DataFrame) -> None:
    """
    Validate that the DataFrame contains the required columns.
    
    Args:
        df: Pandas DataFrame to validate
        
    Raises:
        ValueError: If required columns are missing
    """
    required_columns = ['median_house_value', 'ocean_proximity']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

def calculate_average_by_category(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Calculate average median house value per ocean_proximity category.
    
    Args:
        df: Pandas DataFrame with housing data
        
    Returns:
        List of dictionaries with category and average value
    """
    # Group by ocean_proximity and calculate mean of median_house_value
    averages = df.groupby('ocean_proximity')['median_house_value'].mean().reset_index()
    
    # Convert to list of dictionaries
    result = []
    for _, row in averages.iterrows():
        result.append({
            'category': row['ocean_proximity'],
            'average_value': float(row['median_house_value']),
            'count': int(df[df['ocean_proximity'] == row['ocean_proximity']].shape[0])
        })
    
    return result