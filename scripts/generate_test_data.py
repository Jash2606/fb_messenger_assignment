"""
Script to generate test data for the Messenger application.
This script creates:
- A set of users (with IDs generated from 1 to NUM_USERS)
- Conversations between random pairs of users
- Messages in each conversation with realistic timestamps
and inserts data into the following Cassandra tables:
- conversation_details
- messages_by_conversation
- conversations_by_user
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10                # Number of users to create
NUM_CONVERSATIONS = 15        # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    This function creates:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    # 1. Create a set of user IDs
    user_ids = [uuid.uuid4() for _ in range(NUM_USERS)]
    logger.info(f"Created {NUM_USERS} user IDs")
    
    # Store user ID mapping for reference
    user_id_mapping = {i+1: user_id for i, user_id in enumerate(user_ids)}
    
    # 2. Create conversations between random pairs of users
    conversations = []
    for i in range(NUM_CONVERSATIONS):
        # Select two random users for a conversation
        participants = random.sample(user_ids, 2)
        conversation_id = uuid.uuid4()
        conversations.append((conversation_id, participants))
        
        # Insert into conversation_details table
        # Note: Using the schema from your setup_db.py
        session.execute(
            """
            INSERT INTO conversation_details (
                conversation_id, participants, created_at
            ) VALUES (%s, %s, %s)
            """,
            (conversation_id, participants, datetime.now())
        )
    
    logger.info(f"Created {NUM_CONVERSATIONS} conversations")
    
    # 3. Generate messages for each conversation
    total_messages = 0
    
    for conversation_id, participants in conversations:
        # Generate a random number of messages for this conversation
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        
        # Start with a base timestamp and work backwards
        current_time = datetime.now()
        last_message = None
        last_message_time = None
        
        for i in range(num_messages):
            # Use timeuuid for message_id as per your schema
            message_id = uuid.uuid1()  # timeuuid
            # Randomly select a sender from the participants
            sender_id = random.choice(participants)
            # Generate a random message content
            content = f"Test message {i+1} in conversation {conversation_id}"
            
            # Calculate a realistic timestamp (messages get older as we go)
            # This creates a random time gap between messages (1-60 minutes)
            time_gap = timedelta(minutes=random.randint(1, 60))
            timestamp = current_time - (i * time_gap)
            
            # Insert into messages_by_conversation table
            # Note: Using the schema from your setup_db.py
            session.execute(
                """
                INSERT INTO messages_by_conversation (
                    conversation_id, message_id, sender_id, message_text, created_at
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (conversation_id, message_id, sender_id, content, timestamp)
            )
            
            # Save the most recent message
            if i == 0:
                last_message = content
                last_message_time = timestamp
        
        # Update conversations_by_user for each participant with the most recent message
        for user_id in participants:
            session.execute(
                """
                INSERT INTO conversations_by_user (
                    user_id, last_message_time, conversation_id, last_message, participants
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, last_message_time, conversation_id, last_message, participants)
            )
        
        total_messages += num_messages
    
    logger.info(f"Generated {total_messages} messages across all conversations")
    
    # Print out some sample IDs for testing
    logger.info("Sample user IDs for testing:")
    for i in range(min(5, NUM_USERS)):
        logger.info(f"User {i+1}: {user_id_mapping[i+1]}")
    
    logger.info("Sample conversation IDs for testing:")
    for i in range(min(5, NUM_CONVERSATIONS)):
        logger.info(f"Conversation {i+1}: {conversations[i][0]}")
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")


def main():
    """Main function to generate test data."""
    cluster = None
    try:
        cluster, session = connect_to_cassandra()
        generate_test_data(session)
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()
