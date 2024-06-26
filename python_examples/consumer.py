import asyncio

# Assuming there's a Python module for iggy with similar functionalities.
from iggy_py import IggyClient, ReceiveMessage

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 1


async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
    try:
        client.connect()
        client.login_user("iggy", "iggy")
        await consume_messages(client)
    except Exception as e:
        print("exception: {}", e)


async def consume_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    print(f"Messages will be consumed from stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with interval {interval * 1000} ms.")

    offset = 0
    messages_per_batch = 10
    while True:
        try:
            polled_messages = client.poll_messages(
                stream_id=STREAM_NAME,
                topic_id=TOPIC_NAME,
                partition_id=PARTITION_ID,
                count=messages_per_batch,
                auto_commit=False
            )
            if not polled_messages:
                print("No messages found.")
                await asyncio.sleep(interval)
                continue

            offset += len(polled_messages)
            for message in polled_messages:
                handle_message(message)
            await asyncio.sleep(interval)
        except Exception as e:
            print("exception: {}", e)
            break


def handle_message(message: ReceiveMessage):
    payload = message.payload().decode('utf-8')
    print(f"Handling message at offset: {message.offset()}, payload: {payload}...")


if __name__ == "__main__":
    asyncio.run(main())
