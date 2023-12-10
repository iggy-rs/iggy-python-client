import asyncio

# Assuming we have a Python module for iggy with similar functionality as the Rust one.
from iggy_py import IggyClient, SendMessage as Message

STREAM_ID = 1
TOPIC_ID = 1
PARTITION_ID = 1

async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
    client.connect()
    client.login_user("iggy", "iggy")
    init_system(client)
    await produce_messages(client)

def init_system(client: IggyClient):
    try:
        client.create_stream(stream_id=STREAM_ID, name="sample-stream")
        print("Stream was created.")
    except Exception as e:
        print("stream error {}", e)
    
    try:
        client.create_topic(
            stream_id=STREAM_ID,  # Assuming a method exists to create a numeric Identifier.
            topic_id=TOPIC_ID,
            partitions_count=1,
            name="sample-topic"
        )
        print("Topic was created.")
    except Exception as e:
        print("topic error {}", e)
async def produce_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    print(f"Messages will be sent to stream: {STREAM_ID}, topic: {TOPIC_ID}, partition: {PARTITION_ID} with interval {interval * 1000} ms.")
    
    current_id = 0
    messages_per_batch = 10
    while True:
        messages = []
        for _ in range(messages_per_batch):
            current_id += 1
            payload = f"message-{current_id}"
            message = Message(payload)  # Assuming a method exists to convert str to Message.
            messages.append(message)
        try:
            client.send_messages(
                stream_id=STREAM_ID,
                topic_id=TOPIC_ID,
                partitioning=PARTITION_ID,
                messages=messages
            )
        except Exception as e:
            print("exception: {}", e)
        
        print(f"Sent {messages_per_batch} message(s).")
        await asyncio.sleep(interval)

if __name__ == "__main__":
    asyncio.run(main())
