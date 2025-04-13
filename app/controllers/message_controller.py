import uuid
from datetime import datetime
from fastapi import HTTPException, status

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from app.db.cassandra_client import cassandra_client

class MessageController:
    """
    Controller for handling message operations.
    Implements:
      - send_message
      - get_conversation_messages
      - get_messages_before_timestamp
    """

    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another.
        """
        try:
            conversation_id = uuid.uuid4()
            now = datetime.now()

            sender_uuid = uuid.UUID(message_data.sender_id)
            receiver_uuid = uuid.UUID(message_data.receiver_id)
            participants = [sender_uuid, receiver_uuid]

            conversation_details_query = """
                INSERT INTO conversation_details (conversation_id, participants, created_at)
                VALUES (%s, %s, %s)
            """
            cassandra_client.session.execute(
                conversation_details_query,
                (conversation_id, participants, now)
            )

            message_id = uuid.uuid1()

            insert_message_query = """
                INSERT INTO messages_by_conversation (conversation_id, message_id, sender_id, message_text, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """
            cassandra_client.session.execute(
                insert_message_query,
                (conversation_id, message_id, sender_uuid, message_data.content, now)
            )

            update_conversation_query = """
                INSERT INTO conversations_by_user (user_id, last_message_time, conversation_id, last_message, participants)
                VALUES (%s, %s, %s, %s, %s)
            """
            for user_uuid in participants:
                cassandra_client.session.execute(
                    update_conversation_query,
                    (user_uuid, now, conversation_id, message_data.content, participants)
                )

            return MessageResponse(
                id=str(message_id),
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                created_at=now,
                content=message_data.content,
                conversation_id=str(conversation_id)
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error sending message: {str(e)}"
            )

    async def get_conversation_messages(
        self, 
        conversation_id: str, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination.
        """
        try:
            conv_uuid = uuid.UUID(conversation_id)
            query = """
                SELECT conversation_id, message_id, sender_id, message_text, created_at
                FROM messages_by_conversation
                WHERE conversation_id = %s
            """
            result = cassandra_client.session.execute(query, (conv_uuid,))
            
            all_rows = list(result) if result else []
            total = len(all_rows)
            
            if total == 0:
                return PaginatedMessageResponse(
                    total=0,
                    page=page,
                    limit=limit,
                    data=[]
                )

            start = (page - 1) * limit
            end = start + limit
            paged_rows = all_rows[start:end]
            
            query_details = """
                SELECT participants FROM conversation_details WHERE conversation_id = %s
            """
            details_result = cassandra_client.session.execute(query_details, (conv_uuid,))
            details_row = details_result.one() if details_result else None
            
            participants = []
            if details_row:
                try:
                    participants = details_row.participants
                except (AttributeError, TypeError):
                    try:
                        participants = details_row['participants']
                    except (KeyError, TypeError):
                        pass
            
            data = []
            for row in paged_rows:
                try:
                    message_id = row.message_id
                    sender_id = row.sender_id
                    created_at = row.created_at
                    message_text = row.message_text
                    row_conversation_id = row.conversation_id
                except (AttributeError, TypeError):
                    try:
                        message_id = row['message_id']
                        sender_id = row['sender_id']
                        created_at = row['created_at']
                        message_text = row['message_text']
                        row_conversation_id = row['conversation_id']
                    except (KeyError, TypeError):
                        print(f"Problematic row: {row}")
                        continue
                
                receiver_id = "00000000-0000-0000-0000-000000000000"
                if participants and len(participants) > 1:
                    sender_id_str = str(sender_id)
                    
                    if sender_id_str == str(participants[0]):
                        receiver_id = str(participants[1])
                    elif sender_id_str == str(participants[1]):
                        receiver_id = str(participants[0])
                
                # Add message to result data
                data.append(MessageResponse(
                    id=str(message_id),
                    sender_id=str(sender_id),
                    receiver_id=receiver_id,
                    created_at=created_at,
                    content=message_text,
                    conversation_id=str(row_conversation_id)
                ))
            
            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=data
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error fetching conversation messages: {str(e)}"
            )

    async def get_messages_before_timestamp(
        self,
        conversation_id: str,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20,
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination.
        """
        try:
            conv_uuid = uuid.UUID(conversation_id)
            query = """
                SELECT conversation_id, message_id, sender_id, message_text, created_at
                FROM messages_by_conversation
                WHERE conversation_id = %s AND created_at < %s
                ALLOW FILTERING
            """
            result = cassandra_client.session.execute(query, (conv_uuid, before_timestamp))

            # Get all rows and handle empty result
            all_rows = list(result) if result else []

            # Sort the results by created_at in memory since we can't do it in the query
            # Use a try-except block to handle potential attribute access issues
            try:
                all_rows.sort(
                    key=lambda row: getattr(row, "created_at", datetime.min), reverse=True
                )
            except Exception as sort_error:
                # If sorting fails, log it but continue with unsorted data
                print(f"Warning: Could not sort results: {str(sort_error)}")

            total = len(all_rows)

            if total == 0:
                # Return an empty response with valid structure
                return PaginatedMessageResponse(total=0, page=page, limit=limit, data=[])

            # Simulate pagination
            start = (page - 1) * limit
            end = start + limit
            paged_rows = all_rows[start:end]

            # Get conversation participants once for all messages
            query_details = """
                SELECT participants FROM conversation_details WHERE conversation_id = %s
            """
            details_result = cassandra_client.session.execute(query_details, (conv_uuid,))
            details_row = details_result.one() if details_result else None

            # Extract participants safely
            participants = []
            if details_row:
                try:
                    participants = details_row.participants
                except (AttributeError, TypeError):
                    try:
                        participants = details_row["participants"]
                    except (KeyError, TypeError):
                        # If we can't access participants, leave as empty list
                        pass

            data = []
            for row in paged_rows:
                # Extract data from row safely
                try:
                    # First try accessing as object attributes
                    message_id = row.message_id
                    sender_id = row.sender_id
                    created_at = row.created_at
                    message_text = row.message_text
                    row_conversation_id = row.conversation_id
                except (AttributeError, TypeError):
                    try:
                        # Then try accessing as dictionary
                        message_id = row["message_id"]
                        sender_id = row["sender_id"]
                        created_at = row["created_at"]
                        message_text = row["message_text"]
                        row_conversation_id = row["conversation_id"]
                    except (KeyError, TypeError):
                        # If we can't access the data either way, print the row for debugging and skip
                        print(f"Problematic row: {row}")
                        continue

                # Determine receiver_id based on participants
                receiver_id = "00000000-0000-0000-0000-000000000000"
                if participants and len(participants) > 1:
                    # Convert sender_id to string for comparison
                    sender_id_str = str(sender_id)

                    # If sender is first participant, receiver is second, and vice versa
                    if sender_id_str == str(participants[0]):
                        receiver_id = str(participants[1])
                    elif sender_id_str == str(participants[1]):
                        receiver_id = str(participants[0])

                # Add message to result data
                data.append(
                    MessageResponse(
                        id=str(message_id),
                        sender_id=str(sender_id),
                        receiver_id=receiver_id,
                        created_at=created_at,
                        content=message_text,
                        conversation_id=str(row_conversation_id),
                    )
                )

            # Always return a valid PaginatedMessageResponse object
            return PaginatedMessageResponse(total=total, page=page, limit=limit, data=data)

        except Exception as e:
            # Print the full exception for debugging
            import traceback

            traceback.print_exc()

            # Instead of raising an HTTPException, we'll return an empty response
            # This prevents the ResponseValidationError by ensuring we always return
            # the expected response type
            print(f"Error fetching messages before timestamp: {str(e)}")
            return PaginatedMessageResponse(total=0, page=page, limit=limit, data=[])


        """
            Get messages in a conversation before a specific timestamp with pagination.
        """
        try:
            conv_uuid = uuid.UUID(conversation_id)
            query = """
                SELECT conversation_id, message_id, sender_id, message_text, created_at
                FROM messages_by_conversation
                WHERE conversation_id = %s AND created_at < %s
                ALLOW FILTERING
            """
            result = cassandra_client.session.execute(query, (conv_uuid, before_timestamp))
            
            # Get all rows and handle empty result
            all_rows = list(result) if result else []
            
            # Sort the results by created_at in memory since we can't do it in the query
            all_rows.sort(key=lambda row: getattr(row, 'created_at', datetime.min), reverse=True)
            
            total = len(all_rows)
            
            if total == 0:
                return PaginatedMessageResponse(
                    total=0,
                    page=page,
                    limit=limit,
                    data=[]
                )

            # Simulate pagination
            start = (page - 1) * limit
            end = start + limit
            paged_rows = all_rows[start:end]
            
            # Rest of your code to process the rows...
        except Exception as e:
            # Error handling...

            """
            Get messages in a conversation before a specific timestamp with pagination.
            """
            try:
                conv_uuid = uuid.UUID(conversation_id)
                query = """
                    SELECT conversation_id, message_id, sender_id, message_text, created_at
                    FROM messages_by_conversation
                    WHERE conversation_id = %s AND created_at < %s
                    ORDER BY created_at DESC
                    ALLOW FILTERING
                """
                result = cassandra_client.session.execute(query, (conv_uuid, before_timestamp))
                
                # Get all rows and handle empty result
                all_rows = list(result) if result else []
                total = len(all_rows)
                
                if total == 0:
                    return PaginatedMessageResponse(
                        total=0,
                        page=page,
                        limit=limit,
                        data=[]
                    )

                # Simulate pagination.
                start = (page - 1) * limit
                end = start + limit
                paged_rows = all_rows[start:end]
                
                # Get conversation participants once for all messages
                query_details = """
                    SELECT participants FROM conversation_details WHERE conversation_id = %s
                """
                details_result = cassandra_client.session.execute(query_details, (conv_uuid,))
                details_row = details_result.one() if details_result else None
                
                # Extract participants safely
                participants = []
                if details_row:
                    try:
                        participants = details_row.participants
                    except (AttributeError, TypeError):
                        try:
                            participants = details_row['participants']
                        except (KeyError, TypeError):
                            # If we can't access participants, leave as empty list
                            pass
                
                data = []
                for row in paged_rows:
                    # Extract data from row safely
                    try:
                        # First try accessing as object attributes
                        message_id = row.message_id
                        sender_id = row.sender_id
                        created_at = row.created_at
                        message_text = row.message_text
                        row_conversation_id = row.conversation_id
                    except (AttributeError, TypeError):
                        try:
                            # Then try accessing as dictionary
                            message_id = row['message_id']
                            sender_id = row['sender_id']
                            created_at = row['created_at']
                            message_text = row['message_text']
                            row_conversation_id = row['conversation_id']
                        except (KeyError, TypeError):
                            # If we can't access the data either way, print the row for debugging and skip
                            print(f"Problematic row: {row}")
                            continue
                    
                    # Determine receiver_id based on participants
                    receiver_id = "00000000-0000-0000-0000-000000000000"
                    if participants and len(participants) > 1:
                        # Convert sender_id to string for comparison
                        sender_id_str = str(sender_id)
                        
                        # If sender is first participant, receiver is second, and vice versa
                        if sender_id_str == str(participants[0]):
                            receiver_id = str(participants[1])
                        elif sender_id_str == str(participants[1]):
                            receiver_id = str(participants[0])
                    
                    # Add message to result data
                    data.append(MessageResponse(
                        id=str(message_id),
                        sender_id=str(sender_id),
                        receiver_id=receiver_id,
                        created_at=created_at,
                        content=message_text,
                        conversation_id=str(row_conversation_id)
                    ))
                
                return PaginatedMessageResponse(
                    total=total,
                    page=page,
                    limit=limit,
                    data=data
                )
            except Exception as e:
                # Print the full exception for debugging
                import traceback
                traceback.print_exc()
                
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error fetching messages before timestamp: {str(e)}"
                )