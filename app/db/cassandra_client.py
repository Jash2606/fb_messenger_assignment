"""
Cassandra client for the Messenger application.
This provides a connection to the Cassandra database.
"""
import os
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import time

from cassandra.cluster import Cluster, Session, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger(__name__)

class CassandraClient:
    """Singleton Cassandra client for the application."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the Cassandra connection."""
        if self._initialized:
            return
        
        # In Docker Compose, use the service name as the host
        self.host = os.getenv("CASSANDRA_HOST", "cassandra")
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        
        self.cluster = None
        self.session = None
        self.max_retries = 5
        self.retry_delay = 5  # seconds
        
        self.connect()
        
        self._initialized = True
    
    def connect(self) -> None:
        """Connect to the Cassandra cluster with retry logic."""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connecting to Cassandra at {self.host}:{self.port}, attempt {attempt+1}/{self.max_retries}")
                self.cluster = Cluster([self.host])
                
                # First connect without keyspace to ensure the server is up
                temp_session = self.cluster.connect()
                
                # Check if keyspace exists, create if it doesn't
                self._ensure_keyspace_exists(temp_session)
                
                # Now connect with the keyspace
                self.session = self.cluster.connect(self.keyspace)
                self.session.row_factory = dict_factory
                
                logger.info(f"Successfully connected to Cassandra at {self.host}:{self.port}, keyspace: {self.keyspace}")
                return
            except NoHostAvailable as e:
                logger.warning(f"Connection attempt {attempt+1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("All connection attempts failed")
                    raise
            except Exception as e:
                logger.error(f"Failed to connect to Cassandra: {str(e)}")
                raise
    
    def _ensure_keyspace_exists(self, session):
        """Ensure the keyspace exists, create it if it doesn't."""
        try:
            # Check if keyspace exists
            rows = session.execute(f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{self.keyspace}'")
            if not rows:
                logger.info(f"Creating keyspace {self.keyspace}")
                # Create keyspace with SimpleStrategy and replication factor 1 for development
                session.execute(f"""
                    CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                    AND durable_writes = true
                """)
        except Exception as e:
            logger.error(f"Error ensuring keyspace exists: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def execute(self, query: str, params: dict = None) -> List[Dict[str, Any]]:
        """
        Execute a CQL query.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            List of rows as dictionaries
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, params or {})
            return list(result)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_async(self, query: str, params: dict = None):
        """
        Execute a CQL query asynchronously.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            Async result object
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            return self.session.execute_async(statement, params or {})
        except Exception as e:
            logger.error(f"Async query execution failed: {str(e)}")
            raise
    
    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self.connect()
        return self.session

# Create a global instance
cassandra_client = CassandraClient() 