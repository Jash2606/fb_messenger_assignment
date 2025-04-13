# app/models/conversation_model.py
import uuid
from cassandra.query import SimpleStatement
from datetime import datetime
from typing import List, Optional
from app.db.cassandra_client import cassandra_client
from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Handles database operations for conversations.
    """
    
    @staticmethod
    async def get_user_conversations(user_id: str, page: int = 1, limit: int = 20) -> PaginatedConversationResponse:
        """
        Get conversations for a user with pagination.
        
        Args:
            user_id: ID of the user
            page: Page number for pagination
            limit: Number of items per page
            
        Returns:
            PaginatedConversationResponse: Paginated list of conversations
        """
        try:
            # Convert user_id to UUID
            user_uuid = uuid.UUID(user_id)
            
            query = """
                SELECT user_id, last_message_time, conversation_id, last_message, participants 
                FROM conversations_by_user 
                WHERE user_id = %s
            """
            statement = SimpleStatement(query, fetch_size=limit)
            result = cassandra_client.session.execute(statement, (user_uuid,))
            all_rows = list(result) if result else []
            total = len(all_rows)
            
            # Handle empty result
            if total == 0:
                return PaginatedConversationResponse(
                    total=0,
                    page=page,
                    limit=limit,
                    data=[]
                )
            
            # Simulate pagination using slicing
            start = (page - 1) * limit
            end = start + limit
            paged_rows = all_rows[start:end]
            
            conversations = []
            for row in paged_rows:
                try:
                    row_conversation_id = getattr(row, 'conversation_id', None)
                    row_participants = getattr(row, 'participants', None)
                    row_last_message_time = getattr(row, 'last_message_time', None)
                    row_last_message = getattr(row, 'last_message', None)
                except Exception:
                    try:
                        row_conversation_id = row.get('conversation_id')
                        row_participants = row.get('participants')
                        row_last_message_time = row.get('last_message_time')
                        row_last_message = row.get('last_message')
                    except Exception:
                        continue
                
                participants = row_participants if row_participants else []
                
                user1 = user_uuid
                user2 = user_uuid
                
                if participants and len(participants) >= 1:
                    user1 = participants[0]
                if participants and len(participants) >= 2:
                    user2 = participants[1]
                
                try:
                    conv = ConversationResponse(
                        id=str(row_conversation_id),
                        user1_id=str(user1),
                        user2_id=str(user2),
                        last_message_at=row_last_message_time,
                        last_message_content=row_last_message
                    )
                    conversations.append(conv)
                except Exception:
                    continue
            
            return PaginatedConversationResponse(
                total=total,
                page=page,
                limit=limit,
                data=conversations
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error retrieving user conversations: {str(e)}")
            
            return PaginatedConversationResponse(
                total=0,
                page=page,
                limit=limit,
                data=[]
            )
    
    @staticmethod
    async def get_conversation(conversation_id: str) -> Optional[ConversationResponse]:
        """
        Get a conversation by ID.
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            ConversationResponse: Conversation details or None if not found
        """
        try:
            conv_uuid = uuid.UUID(conversation_id)
            
            query_details = """
                SELECT conversation_id, participants, created_at 
                FROM conversation_details 
                WHERE conversation_id = %s
            """
            result_details = cassandra_client.session.execute(query_details, (conv_uuid,))
            details_row = result_details.one() if result_details else None
            
            if not details_row:
                return None
            
            try:
                row_conversation_id = details_row.conversation_id
                row_participants = details_row.participants
                row_created_at = details_row.created_at
            except Exception:
                try:
                    row_conversation_id = details_row.get('conversation_id')
                    row_participants = details_row.get('participants')
                    row_created_at = details_row.get('created_at')
                except Exception:
                    return None
            
            participants = row_participants if row_participants else []
            
            user1_id = str(uuid.UUID(int=0))
            user2_id = str(uuid.UUID(int=0))
            

            if participants and len(participants) >= 1:
                user1_id = str(participants[0])
            if participants and len(participants) >= 2:
                user2_id = str(participants[1])
            
            last_message_time = row_created_at
            last_message_content = None
            
            if participants:
                query_last = """
                    SELECT last_message_time, last_message 
                    FROM conversations_by_user 
                    WHERE user_id = %s AND conversation_id = %s
                    ALLOW FILTERING
                """
                result_last = cassandra_client.session.execute(query_last, (participants[0], conv_uuid))
                last_row = result_last.one() if result_last else None
                
                if last_row:
                    try:
                        last_message_time = getattr(last_row, 'last_message_time', last_message_time)
                        last_message_content = getattr(last_row, 'last_message', None)
                    except Exception:
                        try:
                            last_message_time = last_row.get('last_message_time', last_message_time)
                            last_message_content = last_row.get('last_message', None)
                        except Exception:
                            pass
            
            return ConversationResponse(
                id=str(row_conversation_id),
                user1_id=user1_id,
                user2_id=user2_id,
                last_message_at=last_message_time,
                last_message_content=last_message_content
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error retrieving conversation: {str(e)}")
            return None
