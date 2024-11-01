import asyncio
from loguru import logger

# Assuming there's a Python module for iggy with similar functionalities.
from iggy_py import IggyClient, ReceiveMessage, PollingStrategy

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 1


async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
    try:
        logger.info("Connecting to IggyClient...")
        client.connect()
        logger.info("Connected. Logging in user...")
        client.login_user("iggy", "iggy")
        logger.info("Logged in.")
        await consume_messages(client)
    except Exception as error:
        logger.exception("Exception occurred in main function: {}", error)


async def consume_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be consumed from stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with "
        f"interval {interval * 1000} ms.")
    offset = 0
    messages_per_batch = 10
    while True:
        try:
            logger.debug("Polling for messages...")
            polled_messages = client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partition_id=PARTITION_ID,
                polling_strategy=PollingStrategy.Next(),
                count=messages_per_batch,
                auto_commit=True
            )
            if not polled_messages:
                logger.warning("No messages found in current poll")
                await asyncio.sleep(interval)
                continue

            offset += len(polled_messages)
            for message in polled_messages:
                handle_message(message)
            await asyncio.sleep(interval)
        except Exception as error:
            logger.exception("Exception occurred while consuming messages: {}", error)
            break


def handle_message(message: ReceiveMessage):
    payload = message.payload().decode('utf-8')
    logger.info(f"Handling message at offset: {message.offset()} with payload: {payload}...")


if __name__ == "__main__":
    asyncio.run(main())
