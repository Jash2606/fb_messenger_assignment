"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None
    
    for _ in range(10):  # Try 10 times
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT )
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5)  # Wait 5 seconds before trying again
    
    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    """
    Create the keyspace if it doesn't exist.
    
    This implementation uses SimpleStrategy with a replication factor of 1.
    In production, consider using NetworkTopologyStrategy and a higher replication factor.
    """
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")

    keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
    WITH replication = {{
        'class': 'SimpleStrategy',
        'replication_factor': '1'
    }}
    """
    session.execute(keyspace_query)
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    """
    Create the tables for the application.
    

    """
    logger.info("Dropping existing tables (if any)...")
    session.execute("DROP TABLE IF EXISTS messages_by_conversation;")
    session.execute("DROP TABLE IF EXISTS conversations_by_user;")
    session.execute("DROP TABLE IF EXISTS conversation_details;")

    logger.info("Creating tables...")
    # 1. messages_by_conversation
    # This table stores all messages within a conversation and sorts them using a timeuuid.
    messages_table_query = """
    CREATE TABLE IF NOT EXISTS messages_by_conversation (
        conversation_id uuid,
        message_id timeuuid,
        sender_id uuid,
        message_text text,
        created_at timestamp,
        PRIMARY KEY (conversation_id, message_id)
    ) WITH CLUSTERING ORDER BY (message_id DESC);
    """
    session.execute(messages_table_query)
    logger.info("Table messages_by_conversation created.")

    # 2. conversations_by_user
    # This table stores conversations per user and sorts them by last message time.
    conversations_table_query = """
    CREATE TABLE IF NOT EXISTS conversations_by_user (
        user_id uuid,
        last_message_time timestamp,
        conversation_id uuid,
        last_message text,
        participants list<uuid>,
        PRIMARY KEY (user_id, last_message_time, conversation_id)
    ) WITH CLUSTERING ORDER BY (last_message_time DESC);
    """
    session.execute(conversations_table_query)
    logger.info("Table conversations_by_user created.")

    # 3. conversation_details
    # This table stores metadata for each conversation.
    conversation_details_query = """
    CREATE TABLE IF NOT EXISTS conversation_details (
        conversation_id uuid,
        participants list<uuid>,
        created_at timestamp,
        PRIMARY KEY (conversation_id)
    );
    """
    session.execute(conversation_details_query)
    logger.info("Table conversation_details created.")

    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")
    
    # Wait for Cassandra to be ready
    cluster = wait_for_cassandra()
    
    try:
        # Connect to the server
        session = cluster.connect()

        # Create keyspace and tables
        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)
        
        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()
