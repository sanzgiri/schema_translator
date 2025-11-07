"""Database executor for running queries against customer databases."""

import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from schema_translator.config import get_config
from schema_translator.models import QueryResult


class DatabaseExecutor:
    """Executes SQL queries against customer databases."""
    
    def __init__(self):
        """Initialize the database executor."""
        self.config = get_config()
        self._connections: Dict[str, sqlite3.Connection] = {}
    
    def execute_query(
        self,
        customer_id: str,
        sql: str
    ) -> QueryResult:
        """Execute a SQL query for a specific customer.
        
        Args:
            customer_id: Customer identifier
            sql: SQL query to execute
            
        Returns:
            QueryResult with data and execution metadata
        """
        start_time = time.time()
        
        try:
            # Get database connection
            conn = self._get_connection(customer_id)
            cursor = conn.cursor()
            
            # Execute query
            cursor.execute(sql)
            
            # Fetch results
            rows = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Convert to list of dictionaries
            data = []
            for row in rows:
                data.append(dict(zip(column_names, row)))
            
            # Calculate execution time
            execution_time_ms = (time.time() - start_time) * 1000
            
            return QueryResult(
                customer_id=customer_id,
                data=data,
                sql_executed=sql,
                execution_time_ms=execution_time_ms,
                row_count=len(data)
            )
        
        except Exception as e:
            # Calculate execution time even for errors
            execution_time_ms = (time.time() - start_time) * 1000
            
            return QueryResult(
                customer_id=customer_id,
                data=[],
                sql_executed=sql,
                execution_time_ms=execution_time_ms,
                row_count=0,
                error=str(e)
            )
    
    def execute_for_all_customers(
        self,
        sql_by_customer: Dict[str, str]
    ) -> List[QueryResult]:
        """Execute queries for multiple customers.
        
        Args:
            sql_by_customer: Dictionary mapping customer_id to SQL query
            
        Returns:
            List of QueryResult objects
        """
        results = []
        
        for customer_id, sql in sql_by_customer.items():
            result = self.execute_query(customer_id, sql)
            results.append(result)
        
        return results
    
    def execute_raw_query(
        self,
        customer_id: str,
        sql: str
    ) -> List[Dict[str, Any]]:
        """Execute a raw SQL query and return results directly.
        
        Simpler interface for direct queries without QueryResult wrapper.
        
        Args:
            customer_id: Customer identifier
            sql: SQL query to execute
            
        Returns:
            List of result dictionaries
        """
        result = self.execute_query(customer_id, sql)
        
        if result.error:
            raise RuntimeError(f"Query failed: {result.error}")
        
        return result.data
    
    def test_connection(self, customer_id: str) -> bool:
        """Test if database connection is working.
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            True if connection works, False otherwise
        """
        try:
            conn = self._get_connection(customer_id)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False
    
    def get_table_info(self, customer_id: str) -> Dict[str, List[Dict[str, str]]]:
        """Get information about tables in a customer database.
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            Dictionary mapping table names to column info
        """
        conn = self._get_connection(customer_id)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        table_info = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[1],
                    "type": row[2],
                    "notnull": bool(row[3]),
                    "default": row[4],
                    "primary_key": bool(row[5])
                })
            table_info[table] = columns
        
        return table_info
    
    def count_rows(self, customer_id: str, table_name: str) -> int:
        """Count rows in a table.
        
        Args:
            customer_id: Customer identifier
            table_name: Table name
            
        Returns:
            Number of rows
        """
        conn = self._get_connection(customer_id)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    
    def _get_connection(self, customer_id: str) -> sqlite3.Connection:
        """Get or create a database connection for a customer.
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            SQLite connection
            
        Raises:
            FileNotFoundError: If database file doesn't exist
        """
        # Reuse existing connection if available
        if customer_id in self._connections:
            return self._connections[customer_id]
        
        # Get database path
        db_path = self.config.get_database_path(customer_id)
        
        if not db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
        
        # Create connection
        conn = sqlite3.connect(str(db_path))
        
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Store connection
        self._connections[customer_id] = conn
        
        return conn
    
    def close_all_connections(self) -> None:
        """Close all database connections."""
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()
    
    def close_connection(self, customer_id: str) -> None:
        """Close a specific customer's database connection.
        
        Args:
            customer_id: Customer identifier
        """
        if customer_id in self._connections:
            self._connections[customer_id].close()
            del self._connections[customer_id]
    
    def __del__(self):
        """Cleanup connections on deletion."""
        self.close_all_connections()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connections."""
        self.close_all_connections()
