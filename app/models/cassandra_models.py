

import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import logging

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BoundStatement
from cassandra.util import uuid_from_time

from app.db.cassandra_client import cassandra_client
# Schemas are not strictly needed in the model file, but can be useful for reference
# from app.schemas.message import MessageResponse
# from app.schemas.conversation import ConversationResponse

logger = logging.getLogger(__name__)

WRITE_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM
READ_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM

class MessageModel:
    """
    Message model for interacting with the messages table (using synchronous execute).
    """

    @staticmethod
    async def _get_or_create_conversation_id(user1_id: int, user2_id: int) -> uuid.UUID:
        """
        Finds the existing conversation ID between two users or creates a new one.
        Ensures user IDs are ordered consistently. (Synchronous DB calls)
        """
        if user1_id == user2_id:
             raise ValueError("Cannot create conversation with oneself")
        user_a_id = min(user1_id, user2_id)
        user_b_id = max(user1_id, user2_id)

        session = cassandra_client.get_session()

        # 1. Check if conversation exists (using synchronous execute)
        select_query = """
        SELECT conversation_id FROM conversation_by_users
        WHERE user_a_id = %s AND user_b_id = %s LIMIT 1
        """
        select_statement = SimpleStatement(select_query, consistency_level=READ_CONSISTENCY)

        # Use synchronous execute()
        result = session.execute(select_statement, (user_a_id, user_b_id))
        row = result.one()

        if row:
            return row['conversation_id']
        else:
            # 2. Create new conversation ID if not found (using synchronous execute)
            new_conversation_id = uuid.uuid4()
            insert_query = """
            INSERT INTO conversation_by_users (user_a_id, user_b_id, conversation_id)
            VALUES (%s, %s, %s)
            """
            insert_statement = SimpleStatement(insert_query, consistency_level=WRITE_CONSISTENCY)

            # Use synchronous execute()
            session.execute(insert_statement, (user_a_id, user_b_id, new_conversation_id))
            logger.info(f"Created new conversation between {user_a_id} and {user_b_id} with ID: {new_conversation_id}")
            return new_conversation_id

    @staticmethod
    async def create_message(
        sender_id: int, receiver_id: int, content: str
    ) -> Tuple[Optional[uuid.UUID], Optional[uuid.UUID], Optional[uuid.UUID]]: # Added message_id to return tuple
        """
        Creates a new message, saves it, and updates conversation metadata. (Synchronous DB calls)

        Returns:
            A tuple containing (conversation_id, message_timeuuid, message_id) or (None, None, None) on failure.
        """
        session = cassandra_client.get_session()
        try:
            # 1. Get or create the conversation ID (this method is now internally synchronous with DB)
            conversation_id = await MessageModel._get_or_create_conversation_id(sender_id, receiver_id)

            # 2. Generate message details
            message_time = uuid_from_time(datetime.utcnow())
            message_id = uuid.uuid4() # Keep the generated message_id

            # 3. Insert the message (using synchronous execute)
            insert_message_query = """
            INSERT INTO messages_by_conversation
            (conversation_id, message_time, message_id, sender_id, receiver_id, content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            message_statement = SimpleStatement(insert_message_query, consistency_level=WRITE_CONSISTENCY)
            session.execute(
                message_statement, (conversation_id, message_time, message_id, sender_id, receiver_id, content)
            )

            # 4. Update conversations_by_user (using synchronous execute)
            update_conversation_query = """
            INSERT INTO conversations_by_user
            (user_id, last_message_time, conversation_id, other_user_id, last_message_sender_id, last_message_content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            conv_statement = SimpleStatement(update_conversation_query, consistency_level=WRITE_CONSISTENCY)

            # Update for sender
            session.execute(
                 conv_statement, (sender_id, message_time, conversation_id, receiver_id, sender_id, content[:200])
            )
            # Update for receiver
            session.execute(
                 conv_statement, (receiver_id, message_time, conversation_id, sender_id, sender_id, content[:200])
            )

            logger.info(f"Message {message_id} created in conversation {conversation_id}")
            # Return the generated message_id as well
            return conversation_id, message_time, message_id

        except Exception as e:
            logger.error(f"Failed to create message: {str(e)}", exc_info=True)
            return None, None, None # Indicate failure


    @staticmethod
    async def get_conversation_messages(
        conversation_id: uuid.UUID,
        page_size: int = 20,
        paging_state: Optional[bytes] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
        """
        Get messages for a conversation with pagination. (Synchronous DB calls)
        """
        session = cassandra_client.get_session()
        query = """
        SELECT conversation_id, message_time, message_id, sender_id, receiver_id, content
        FROM messages_by_conversation
        WHERE conversation_id = %s
        """
        # fetch_size is used by the driver for paging even with synchronous execute
        statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

        try:
            # Use synchronous execute(), passing the paging state if it exists
            results = session.execute(statement, (conversation_id,), paging_state=paging_state)

            messages = list(results.current_rows)
            next_paging_state = results.paging_state # Paging state still works

            for msg in messages:
                if msg.get('message_time'):
                    msg_timestamp = uuid.UUID(bytes=msg['message_time'].bytes).time
                    msg['created_at'] = datetime.utcfromtimestamp(msg_timestamp / 1e9)
                else:
                     msg['created_at'] = None
                msg['id'] = msg.get('message_id')

            return messages, next_paging_state

        except Exception as e:
            logger.error(f"Failed to get messages for conversation {conversation_id}: {str(e)}", exc_info=True)
            return [], None


    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: uuid.UUID,
        before_timestamp: datetime,
        page_size: int = 20,
        paging_state: Optional[bytes] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
        """
        Get messages before a timestamp with pagination. (Synchronous DB calls)
        """
        session = cassandra_client.get_session()
        before_timeuuid = uuid_from_time(before_timestamp)

        query = """
        SELECT conversation_id, message_time, message_id, sender_id, receiver_id, content
        FROM messages_by_conversation
        WHERE conversation_id = %s AND message_time < %s
        """
        statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

        try:
            # Use synchronous execute()
            results = session.execute(statement, (conversation_id, before_timeuuid), paging_state=paging_state)

            messages = list(results.current_rows)
            next_paging_state = results.paging_state

            for msg in messages:
                if msg.get('message_time'):
                    msg_timestamp = uuid.UUID(bytes=msg['message_time'].bytes).time
                    msg['created_at'] = datetime.utcfromtimestamp(msg_timestamp / 1e9)
                else:
                     msg['created_at'] = None
                msg['id'] = msg.get('message_id')

            return messages, next_paging_state

        except Exception as e:
            logger.error(f"Failed to get messages before timestamp for conv {conversation_id}: {str(e)}", exc_info=True)
            return [], None


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables. (Synchronous DB calls)
    """

    @staticmethod
    async def get_user_conversations(
        user_id: int,
        page_size: int = 20,
        paging_state: Optional[bytes] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
        """
        Get conversations for a user with pagination, sorted by most recent. (Synchronous DB calls)
        """
        session = cassandra_client.get_session()
        query = """
        SELECT user_id, last_message_time, conversation_id, other_user_id,
               last_message_sender_id, last_message_content
        FROM conversations_by_user
        WHERE user_id = %s
        """
        statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

        try:
            # Use synchronous execute()
            results = session.execute(statement, (user_id,), paging_state=paging_state)

            conversations = list(results.current_rows)
            next_paging_state = results.paging_state

            formatted_convos = []
            for convo in conversations:
                 if convo.get('last_message_time'):
                     last_msg_timestamp_ns = uuid.UUID(bytes=convo['last_message_time'].bytes).time
                     last_msg_dt = datetime.utcfromtimestamp(last_msg_timestamp_ns / 1e9)
                 else:
                     last_msg_dt = None # Handle case where time might be missing

                 formatted_convos.append({
                     "id": convo.get('conversation_id'),
                     "user1_id": user_id,
                     "user2_id": convo.get('other_user_id'),
                     "last_message_at": last_msg_dt,
                     "last_message_content": convo.get('last_message_content'),
                 })

            return formatted_convos, next_paging_state

        except Exception as e:
            logger.error(f"Failed to get conversations for user {user_id}: {str(e)}", exc_info=True)
            return [], None


    @staticmethod
    async def get_conversation(conversation_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """
        Get a specific conversation's metadata. (Remains largely unsupported by schema efficiently)
        """
        logger.warning("get_conversation by ID alone is not efficiently supported by the current schema design. Obtain conversation details via get_user_conversations.")
        return None # Keep returning None as schema doesn't support this well


    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Optional[uuid.UUID]:
        """
        Gets the conversation ID between two users, creating the entry if it doesn't exist.
        (Uses the modified _get_or_create_conversation_id)
        """
        try:
            # Delegate to the internal helper method (which now uses sync execute)
            conversation_id = await MessageModel._get_or_create_conversation_id(user1_id, user2_id)
            return conversation_id
        except Exception as e:
            logger.error(f"Failed in create_or_get_conversation between {user1_id} and {user2_id}: {str(e)}", exc_info=True)
            return None
