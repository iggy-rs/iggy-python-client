import asyncio

# Assuming there's a Python module for iggy with similar functionalities.
from iggy_py import Client, IggyClient, Consumer, Identifier, PollMessages, PollingStrategy, Message

STREAM_ID = 1
TOPIC_ID = 1
PARTITION_ID = 1

async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
    await client.connect()
    await consume_messages(client)

async def consume_messages(client: Client):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    print(f"Messages will be consumed from stream: {STREAM_ID}, topic: {TOPIC_ID}, partition: {PARTITION_ID} with interval {interval * 1000} ms.")
    
    offset = 0
    messages_per_batch = 10
    while True:
        polled_messages = await client.poll_messages(PollMessages(
            consumer=Consumer(),  # Assuming a default constructor for Consumer.
            stream_id=Identifier.numeric(STREAM_ID),
            topic_id=Identifier.numeric(TOPIC_ID),
            partition_id=PARTITION_ID,
            strategy=PollingStrategy.offset(offset),
            count=messages_per_batch,
            auto_commit=False
        ))
        
        if not polled_messages.messages:
            print("No messages found.")
            await asyncio.sleep(interval)
            continue
        
        offset += len(polled_messages.messages)
        for message in polled_messages.messages:
            handle_message(message)
        await asyncio.sleep(interval)

def handle_message(message: Message):
    # Assuming message.payload is of type bytes in Python.
    payload = message.payload.decode('utf-8')
    print(f"Handling message at offset: {message.offset}, payload: {payload}...")

if __name__ == "__main__":
    asyncio.run(main())
