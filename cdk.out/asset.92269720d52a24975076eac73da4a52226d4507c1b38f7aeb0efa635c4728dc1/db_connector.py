"""
Database connector module for interacting with RDS PostgreSQL.
"""
from loguru import logger
import psycopg2
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import uuid
from psycopg2.extensions import connection, cursor


class RDSConnector:
    """
    A class to handle connections and operations with the RDS PostgreSQL database.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the RDS connector with database configuration.
        
        Args:   
            db_config: Dictionary containing database connection parameters
                       (host, port, dbname, user, password)
        """
        self.db_config = db_config
        self.conn: Optional[connection] = None
        self.cursor: Optional[cursor] = None
    
    def __enter__(self) -> 'RDSConnector':
        """
        Context manager entry method - establishes database connection.
        
        Returns:
            Self reference for context manager
        """
        try:
            logger.info(f"Connecting to database at {self.db_config['host']}:{self.db_config['port']}")
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.cursor = self.conn.cursor()
            
            # Ensure the required table exists
            self._ensure_table_exists()
            
            return self
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        """
        Context manager exit method - closes database connection.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type is not None:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
    
    def _ensure_table_exists(self) -> None:
        """
        Ensure that the necessary tables exist in the database.
        Creates them if they don't exist.
        """
        if not self.cursor or not self.conn:
            raise RuntimeError("Database connection not established")
            
        create_table_query = """
        CREATE TABLE IF NOT EXISTS housing_summary_statistics (
            id UUID PRIMARY KEY,
            category VARCHAR(50) NOT NULL,
            average_value NUMERIC(12, 2) NOT NULL,
            record_count INTEGER NOT NULL,
            processed_at TIMESTAMP NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_category ON housing_summary_statistics(category);
        """
        
        self.cursor.execute(create_table_query)
        self.conn.commit()
        logger.info("Ensured database table exists")
    
    def store_summary_statistics(self, summary_stats: List[Dict[str, Any]]) -> None:
        """
        Store summary statistics in the database.
        
        Args:
            summary_stats: List of dictionaries containing summary statistics
                          (category, average_value, count)
        """
        if not self.cursor or not self.conn:
            raise RuntimeError("Database connection not established")
            
        now = datetime.utcnow()
        
        for stat in summary_stats:
            insert_query = """
            INSERT INTO housing_summary_statistics 
            (id, category, average_value, record_count, processed_at)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            # Generate a UUID for the record
            record_id = str(uuid.uuid4())
            
            self.cursor.execute(
                insert_query,
                (
                    record_id,
                    stat['category'],
                    stat['average_value'],
                    stat['count'],
                    now
                )
            )
        
        self.conn.commit()
        logger.info(f"Stored {len(summary_stats)} records in the database")
    
    def query_latest_statistics(self) -> List[Tuple[Any, ...]]:
        """
        Query the latest statistics for each category.
        
        Returns:
            List of tuples containing the latest statistics
        """
        if not self.cursor:
            raise RuntimeError("Database connection not established")
            
        query = """
        WITH latest_stats AS (
            SELECT 
                category,
                MAX(processed_at) as latest_processed_at
            FROM 
                housing_summary_statistics
            GROUP BY 
                category
        )
        SELECT 
            h.category,
            h.average_value,
            h.record_count,
            h.processed_at
        FROM 
            housing_summary_statistics h
        JOIN 
            latest_stats ls
        ON 
            h.category = ls.category AND h.processed_at = ls.latest_processed_at
        ORDER BY 
            h.category;
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        return results