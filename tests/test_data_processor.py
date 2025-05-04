"""
Unit tests for the data processor module.
"""
from pathlib import Path
import pandas as pd
import pytest
import tempfile

from src.lambda_functions.data_processor import (
    process_california_housing_data,
    calculate_average_by_category,
    _validate_dataframe
)

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing"""
    return pd.DataFrame({
        'median_house_value': [100000, 200000, 300000, 150000, 250000],
        'ocean_proximity': ['NEAR BAY', 'INLAND', 'NEAR BAY', 'INLAND', '<1H OCEAN'],
        'median_income': [5.0, 4.5, 3.2, 6.1, 5.5],
        'housing_median_age': [30, 15, 40, 25, 22]
    })

@pytest.fixture
def sample_dataframe_with_nulls():
    """Create a sample DataFrame with null values for testing"""
    return pd.DataFrame({
        'median_house_value': [100000, 200000, None, 150000, 250000],
        'ocean_proximity': ['NEAR BAY', None, 'NEAR BAY', 'INLAND', '<1H OCEAN'],
        'median_income': [5.0, 4.5, 3.2, 6.1, 5.5],
        'housing_median_age': [30, 15, 40, 25, 22]
    })

def test_validate_dataframe_with_valid_df(sample_dataframe):
    """Test validation with a valid DataFrame"""
    _validate_dataframe(sample_dataframe)

def test_validate_dataframe_with_missing_columns():
    """Test validation with missing columns"""
    df = pd.DataFrame({
        'population': [1000, 2000, 3000],
        'median_income': [5.0, 4.5, 3.2]
    })
    with pytest.raises(ValueError) as excinfo:
        _validate_dataframe(df)
    assert "Missing required columns" in str(excinfo.value)

def test_calculate_average_by_category(sample_dataframe):
    """Test calculation of average by category"""
    result = calculate_average_by_category(sample_dataframe)
    result_dict = {item['category']: item['average_value'] for item in result}

    assert result_dict['NEAR BAY'] == 200000
    assert result_dict['INLAND'] == 175000
    assert result_dict['<1H OCEAN'] == 250000

    counts = {item['category']: item['count'] for item in result}
    assert counts['NEAR BAY'] == 2
    assert counts['INLAND'] == 2
    assert counts['<1H OCEAN'] == 1

def _write_temp_csv(df: pd.DataFrame, tmp_dir: Path, filename: str = "test.csv") -> Path:
    """Helper function to write a DataFrame to a temporary CSV file"""
    file_path = tmp_dir / filename
    df.to_csv(file_path, index=False)
    return file_path

def test_process_california_housing_data():
    """Test the main processing function using a temporary CSV file"""
    df = pd.DataFrame({
        'median_house_value': [100000, 200000, 300000, 150000],
        'ocean_proximity': ['NEAR BAY', 'INLAND', 'NEAR BAY', 'INLAND'],
        'median_income': [5.0, 4.5, 3.2, 6.1]
    })
    with tempfile.TemporaryDirectory() as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        csv_path = _write_temp_csv(df, tmp_dir)

        result = process_california_housing_data(str(csv_path))
        result_dict = {item['category']: item['average_value'] for item in result}

        assert len(result) == 2
        assert result_dict['NEAR BAY'] == 200000
        assert result_dict['INLAND'] == 175000

def test_process_california_housing_data_with_missing_file():
    """Test processing with a non-existent file"""
    missing_file = Path('/nonexistent/path/data.csv')
    with pytest.raises(FileNotFoundError):
        process_california_housing_data(str(missing_file))

def test_process_california_housing_data_handles_nulls():
    """Test that processing properly handles null values"""
    df = pd.DataFrame({
        'median_house_value': [100000, 200000, None, 150000],
        'ocean_proximity': ['NEAR BAY', 'INLAND', 'NEAR BAY', None],
        'median_income': [5.0, 4.5, None, 6.1]
    })
    with tempfile.TemporaryDirectory() as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        csv_path = _write_temp_csv(df, tmp_dir)

        result = process_california_housing_data(str(csv_path))
        total_count = sum(item['count'] for item in result)
        assert total_count == 2
