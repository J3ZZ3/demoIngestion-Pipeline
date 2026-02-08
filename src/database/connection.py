"""
Database connection management with connection pooling and retry logic.
"""

import os
import logging
from typing import Optional
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections with connection pooling."""
    
    def __init__(self):
        self.connection_pool: Optional[pool.ThreadedConnectionPool] = None
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
    
    def initialize_pool(self, min_connections: int = 1, max_connections: int = 10) -> None:
        """Initialize the connection pool."""
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                minconn=min_connections,
                maxconn=max_connections,
                dsn=self.database_url
            )
            logger.info(f"Database connection pool initialized: {min_connections}-{max_connections} connections")
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool."""
        if not self.connection_pool:
            self.initialize_pool()
        
        try:
            return self.connection_pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            raise
    
    def return_connection(self, connection) -> None:
        """Return a connection to the pool."""
        if self.connection_pool and connection:
            try:
                self.connection_pool.putconn(connection)
            except Exception as e:
                logger.error(f"Failed to return connection to pool: {e}")
    
    def close_all_connections(self) -> None:
        """Close all connections in the pool."""
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("All database connections closed")
            except Exception as e:
                logger.error(f"Error closing database connections: {e}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch: bool = True):
        """Execute a query with automatic connection management."""
        connection = None
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                
                if fetch:
                    if cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        results = cursor.fetchall()
                        return [dict(zip(columns, row)) for row in results]
                    return []
                else:
                    connection.commit()
                    return cursor.rowcount
                    
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database query failed: {e}")
            raise
        finally:
            if connection:
                self.return_connection(connection)
    
    def execute_many(self, query: str, data: list) -> int:
        """Execute a query with multiple rows of data."""
        connection = None
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                execute_values(cursor, query, data)
                connection.commit()
                return cursor.rowcount
                
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Batch database operation failed: {e}")
            raise
        finally:
            if connection:
                self.return_connection(connection)
    
    def health_check(self) -> bool:
        """Check if database is healthy and accessible."""
        try:
            result = self.execute_query("SELECT 1 as health_check")
            return len(result) > 0 and result[0]['health_check'] == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


# Global database manager instance
db_manager = DatabaseManager()
