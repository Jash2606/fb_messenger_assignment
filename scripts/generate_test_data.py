"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
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
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    Students should implement this function to generate test data based on their schema design.
    The function should create:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    # TODO: Students should implement the test data generation logic
    # Hint:
    # 1. Create a set of user IDs
    users = [uuid.uuid4() for _ in range(NUM_USERS)]
    logger.info(f"Generated {NUM_USERS} users.")

    # 2. Create conversations and messages
    for conv_num in range(NUM_CONVERSATIONS):
        # Randomly select number of participants: 2 to 4 users per conversation
        num_participants = random.choice([2, 3, 4])
        participants = random.sample(users, num_participants)
        
        conversation_id = uuid.uuid4()
        # Choose a base time between 1 and 30 days ago
        base_time = datetime.now() - timedelta(days=random.randint(1, 30))
        
        # Decide number of messages for this conversation (ensure at least 1)
        num_messages = random.randint(1, MAX_MESSAGES_PER_CONVERSATION)
        last_message_time = base_time
        last_message_text = ""
        
        # Insert messages into messages_by_conversation
        for i in range(num_messages):
            # Increment the timestamp by a random seconds offset (between 60 to 300 seconds)
            message_time = base_time + timedelta(seconds=random.randint(60, 300) * i)
            last_message_time = message_time
            
            # Generate a time-based UUID for the message (this will embed the timestamp)
            message_id = uuid.uuid1(clock_seq=random.randint(0, 0x7FFF))
            
            # Randomly choose a sender from the conversation participants
            sender_id = random.choice(participants)
            message_text = f"Message {i+1} in conversation {str(conversation_id)[:8]}"
            last_message_text = message_text
            
            query_message = """
            INSERT INTO messages_by_conversation (conversation_id, message_id, sender_id, message_text, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """
            session.execute(query_message, (conversation_id, message_id, sender_id, message_text, message_time))
        
        # Insert conversation metadata into conversation_details
        query_details = """
        INSERT INTO conversation_details (conversation_id, participants, created_at)
        VALUES (%s, %s, %s)
        """
        session.execute(query_details, (conversation_id, participants, base_time))
        
        # Insert/update the conversation record for each participant in conversations_by_user
        # We use the final message's timestamp and text as the "last message" update.
        query_convo_by_user = """
        INSERT INTO conversations_by_user (user_id, last_message_time, conversation_id, last_message, participants)
        VALUES (%s, %s, %s, %s, %s)
        """
        for user in participants:
            session.execute(query_convo_by_user, (user, last_message_time, conversation_id, last_message_text, participants))
        
        logger.info(f"Created conversation {conv_num+1}/{NUM_CONVERSATIONS} with {num_messages} messages and {num_participants} participants.")
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages.")
    logger.info(f"User IDs range from {users[0]} to {users[-1]}")
    logger.info("Test data generation completed.")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
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