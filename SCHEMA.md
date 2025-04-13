# Cassandra Schema for Facebook Messenger MVP

This schema supports the following use cases:
- Sending messages between users
- Fetching all messages in a conversation
- Fetching messages before a given timestamp (pagination)
- Fetching a user's conversations ordered by most recent activity

---

## Table 1: messages_by_conversation

**Purpose**: Stores all messages in a conversation. Supports:
- Viewing all messages
- Paginating messages using message_id (TimeUUID)

```cql
CREATE TABLE IF NOT EXISTS messages_by_conversation (
    conversation_id UUID,
    message_id TIMEUUID,
    sender_id UUID,
    recipient_id UUID,
    message_text TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY (conversation_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id UUID,
    conversation_id UUID,
    last_message_timestamp TIMESTAMP,
    participant_id UUID,
    PRIMARY KEY (user_id, last_message_timestamp, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_timestamp DESC);