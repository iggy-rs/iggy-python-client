import asyncio
from loguru import logger

# Assuming we have a Python module for iggy with similar functionality as the Rust one.
from iggy_py import IggyClient, SendMessage as Message

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
PARTITION_ID = 1


async def main():
    client = IggyClient()  # Assuming default constructor has similar functionality.
    logger.info("Connecting to IggyClient")
    client.connect()
    logger.info("Connected. Logging in user...")
    client.login_user("iggy", "iggy")
    logger.info("Logged in.")
    init_system(client)
    await produce_messages(client)


def init_system(client: IggyClient):
    try:
        logger.info(f"Creating stream with NAME {STREAM_NAME}...")
        client.create_stream(name="sample-stream")
        logger.info("Stream was created successfuly.")
    except Exception as error:
        logger.error(f"Error creating stream: {error}")
        logger.exception(error)

    try:
        logger.info(f"Creating topic {TOPIC_NAME} in stream {STREAM_NAME}")
        client.create_topic(
            stream=STREAM_NAME,  # Assuming a method exists to create a numeric Identifier.
            partitions_count=1,
            name="sample-topic",
            replication_factor=1
        )
        logger.info(f"Topic: was created successfullly.")
    except Exception as error:
        logger.error(f"Error creating topic {error}")
        logger.exception(error)


async def produce_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(f"Messages will be sent to stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with interval {interval * 1000} ms.")
    current_id = 0
    messages_per_batch = 10
    while True:
        messages = []
        for _ in range(messages_per_batch):
            current_id += 1
            payload = f"message-{current_id}"
            message = Message(payload)  # Assuming a method exists to convert str to Message.
            messages.append(message)
        logger.info(f"Attempting to send batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}")
        try:
            client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=messages,
            )
            logger.info(f"Succesffuly sent batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}")
        except Exception as error:
            logger.error(f"Exception type: {type(error).__name__}, message: {error}")
            logger.exception(error)

        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())
