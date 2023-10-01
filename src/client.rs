use crate::message::Message;
use iggy::binary::messages;
use iggy::client::TopicClient;
use iggy::client::{Client, MessageClient, StreamClient};
use iggy::clients::client::IggyClient as RustIggyClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message as RustMessage, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};

#[pyclass]
pub struct IggyClient {
    inner: RustIggyClient,
    runtime: Runtime,
}

#[pymethods]
impl IggyClient {
    #[new]
    fn new() -> Self {
        // TODO: use asyncio
        let runtime = Builder::new_multi_thread()
            .worker_threads(4) // number of worker threads
            .enable_all() // enables all available Tokio features
            .build()
            .unwrap();
        IggyClient {
            inner: RustIggyClient::default(),
            runtime,
        }
    }

    fn connect(&mut self) -> PyResult<()> {
        let connect_future = self.inner.connect();
        let connect = self.runtime.block_on(async move { connect_future.await });
        PyResult::Ok(())
    }

    fn create_stream(&self, stream_id: u32, name: String) -> PyResult<()> {
        let create_stream_future = self.inner.create_stream(&CreateStream { stream_id, name });
        let create_stream = self
            .runtime
            .block_on(async move { create_stream_future.await });
        PyResult::Ok(())
    }

    fn create_topic(
        &self,
        stream_id: u32,
        topic_id: u32,
        partitions_count: u32,
        name: String,
    ) -> PyResult<()> {
        let create_topic_future = self.inner.create_topic(&CreateTopic {
            stream_id: Identifier::numeric(stream_id).unwrap(),
            topic_id,
            name,
            partitions_count,
            message_expiry: None,
        });
        let create_topic = self
            .runtime
            .block_on(async move { create_topic_future.await });
        PyResult::Ok(())
    }

    fn send_message(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        messages: Vec<Message>,
    ) -> PyResult<()> {
        let messages = messages
            .into_iter()
            .map(|message| message.inner)
            .collect::<Vec<RustMessage>>();
        let send_message_future = self.inner.send_messages(&mut SendMessages {
            stream_id: Identifier::numeric(stream_id).unwrap(),
            topic_id: Identifier::numeric(topic_id).unwrap(),
            partitioning: Partitioning::partition_id(partition_id),
            messages,
        });
        let send_message = self
            .runtime
            .block_on(async move { send_message_future.await });
        PyResult::Ok(())
    }
}
