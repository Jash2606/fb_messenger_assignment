# app/controllers/conversation_controller.py
from fastapi import HTTPException, status
from app.models.cassandra_models import ConversationModel
from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class ConversationController:
    """
    Controller for handling conversation operations.
    Implements:
      - get_user_conversations
      - get_conversation
    """
    
    async def get_user_conversations(
        self, 
        user_id: str, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        try:
            result = await ConversationModel.get_user_conversations(user_id, page, limit)
            return result
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving user conversations: {str(e)}"
            )
    
    async def get_conversation(self, conversation_id: str) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            result = await ConversationModel.get_conversation(conversation_id)
            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            return result
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving conversation: {str(e)}"
            )
