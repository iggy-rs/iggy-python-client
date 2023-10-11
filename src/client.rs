use crate::consumer::Consumer;
use crate::receive_message::ReceiveMessage;
use crate::send_message::SendMessage;
use iggy::client::TopicClient;
use iggy::client::{Client, MessageClient, StreamClient};
use iggy::clients::client::IggyClient as RustIggyClient;
use iggy::consumer::{Consumer as RustConsumer, ConsumerKind};
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::{PollMessages, PollingKind, PollingStrategy};
use iggy::messages::send_messages::{Message as RustMessage, Partitioning, SendMessages};
use iggy::streams::create_stream::CreateStream;
use iggy::topics::create_topic::CreateTopic;
use pyo3::prelude::*;
use pyo3::types::PyList;
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
        let _connect = self
            .runtime
            .block_on(async move { connect_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        PyResult::Ok(())
    }

    fn create_stream(&self, stream_id: u32, name: String) -> PyResult<()> {
        let create_stream = CreateStream { stream_id, name };
        let create_stream_future = self.inner.create_stream(&create_stream);
        let _create_stream = self
            .runtime
            .block_on(async move { create_stream_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        PyResult::Ok(())
    }

    fn create_topic(
        &self,
        stream_id: u32,
        topic_id: u32,
        partitions_count: u32,
        name: String,
    ) -> PyResult<()> {
        let create_topic = CreateTopic {
            stream_id: Identifier::numeric(stream_id).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e))
            })?,
            topic_id,
            name,
            partitions_count,
            message_expiry: None,
        };
        let create_topic_future = self.inner.create_topic(&create_topic);
        let _create_topic = self
            .runtime
            .block_on(async move { create_topic_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        PyResult::Ok(())
    }

    fn send_messages(
        &self,
        stream_id: u32,
        topic_id: u32,
        partitioning: u32,
        messages: &PyList,
    ) -> PyResult<()> {
        let messages: Vec<SendMessage> = messages
            .iter()
            .map(|item| item.extract::<SendMessage>())
            .collect::<Result<Vec<_>, _>>()?;
        let messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| message.inner)
            .collect::<Vec<_>>();

        let mut messages = SendMessages {
            stream_id: Identifier::numeric(stream_id).unwrap(),
            topic_id: Identifier::numeric(topic_id).unwrap(),
            partitioning: Partitioning::partition_id(partitioning),
            messages,
        };

        let send_message_future = self.inner.send_messages(&mut messages);
        self.runtime
            .block_on(async move { send_message_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        PyResult::Ok(())
    }

    fn poll_messages(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        count: u32,
        auto_commit: bool,
    ) -> PyResult<Vec<ReceiveMessage>> {
        let poll_message_cmd = PollMessages {
            consumer: RustConsumer::default(),
            stream_id: Identifier::numeric(stream_id).unwrap(),
            topic_id: Identifier::numeric(topic_id).unwrap(),
            partition_id: Some(partition_id),
            strategy: PollingStrategy::next(),
            count,
            auto_commit,
        };
        let poll_messages = self.inner.poll_messages(&poll_message_cmd);

        let polled_messages = self
            .runtime
            .block_on(async move { poll_messages.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;

        let messages = polled_messages
            .messages
            .into_iter()
            .map(|message| ReceiveMessage::from_rust_message(message))
            .collect::<Vec<_>>();
        PyResult::Ok(messages)
    }
}
