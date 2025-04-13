from typing import Optional
from datetime import datetime
from fastapi import HTTPException, status

from cassandra.query import SimpleStatement
from cassandra.util import uuid_from_time
from uuid import uuid4

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from app.db import get_db_session

PAGE_SIZE = 20

class MessageController:
    """
    Controller for handling message operations
    """

    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another
        """
        try:
            session = get_db_session()

            conversation_id = message_data.conversation_id
            message_id = uuid_from_time(datetime.utcnow())

            # Save to messages_by_conversation
            insert_message = """
            INSERT INTO messages_by_conversation (
                conversation_id, message_id, sender_id, recipient_id, message_text, timestamp
            ) VALUES (%s, %s, %s, %s, %s, toTimestamp(now()))
            """
            session.execute(insert_message, (
                conversation_id,
                message_id,
                message_data.sender_id,
                message_data.recipient_id,
                message_data.message_text,
            ))

            # Update conversations_by_user for both sender and recipient
            update_conversations = """
            INSERT INTO conversations_by_user (
                user_id, last_message_timestamp, conversation_id, participant_id
            ) VALUES (%s, toTimestamp(now()), %s, %s)
            """

            session.execute(update_conversations, (
                message_data.sender_id,
                conversation_id,
                message_data.recipient_id
            ))

            session.execute(update_conversations, (
                message_data.recipient_id,
                conversation_id,
                message_data.sender_id
            ))

            return MessageResponse(
                conversation_id=conversation_id,
                message_id=message_id,
                sender_id=message_data.sender_id,
                recipient_id=message_data.recipient_id,
                message_text=message_data.message_text,
                timestamp=datetime.utcnow()
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
            )

    async def get_conversation_messages(
        self,
        conversation_id: str,
        page: int = 1,
        limit: int = PAGE_SIZE
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination
        """
        try:
            session = get_db_session()

            offset = (page - 1) * limit
            query = f"""
            SELECT * FROM messages_by_conversation
            WHERE conversation_id = %s
            LIMIT {offset + limit}
            """

            rows = session.execute(query, (conversation_id,))
            all_rows = list(rows)[offset:]

            messages = [
                MessageResponse(
                    conversation_id=row.conversation_id,
                    message_id=row.message_id,
                    sender_id=row.sender_id,
                    recipient_id=row.recipient_id,
                    message_text=row.message_text,
                    timestamp=row.timestamp
                ) for row in all_rows
            ]

            return PaginatedMessageResponse(
                messages=messages,
                page=page,
                limit=limit
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
            )

    async def get_messages_before_timestamp(
        self,
        conversation_id: str,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = PAGE_SIZE
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination
        """
        try:
            session = get_db_session()

            max_uuid = uuid_from_time(before_timestamp)
            offset = (page - 1) * limit

            query = f"""
            SELECT * FROM messages_by_conversation
            WHERE conversation_id = %s AND message_id < %s
            LIMIT {offset + limit}
            """

            rows = session.execute(query, (conversation_id, max_uuid))
            all_rows = list(rows)[offset:]

            messages = [
                MessageResponse(
                    conversation_id=row.conversation_id,
                    message_id=row.message_id,
                    sender_id=row.sender_id,
                    recipient_id=row.recipient_id,
                    message_text=row.message_text,
                    timestamp=row.timestamp
                ) for row in all_rows
            ]

            return PaginatedMessageResponse(
                messages=messages,
                page=page,
                limit=limit
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
            )
