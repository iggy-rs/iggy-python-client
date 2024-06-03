use std::str::FromStr;

use iggy::client::TopicClient;
use iggy::client::{Client, MessageClient, StreamClient, UserClient};
use iggy::clients::client::IggyClient as RustIggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer::Consumer as RustConsumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::messages::send_messages::{Message as RustMessage, Partitioning};
use iggy::utils::expiry::IggyExpiry;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::runtime::{Builder, Runtime};

use crate::receive_message::ReceiveMessage;
use crate::send_message::SendMessage;

/// A Python class representing the Iggy client.
/// It wraps the RustIggyClient and provides asynchronous functionality
/// through the contained runtime.
#[pyclass]
pub struct IggyClient {
    inner: RustIggyClient,
    runtime: Runtime,
}

#[pymethods]
impl IggyClient {
    /// Constructs a new IggyClient.
    ///
    /// This initializes a new runtime for asynchronous operations.
    /// Future versions might utilize asyncio for more Pythonic async.
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

    /// Logs in the user with the given credentials.
    ///
    /// Returns `Ok(())` on success, or a PyRuntimeError on failure.
    fn login_user(&self, username: String, password: String) -> PyResult<()> {
        let login_future = self.inner.login_user(&username, &password);
        self.runtime
            .block_on(async move { login_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(())
    }

    /// Connects the IggyClient to its service.
    ///
    /// Returns Ok(()) on successful connection or a PyRuntimeError on failure.
    fn connect(&mut self) -> PyResult<()> {
        let connect_future = self.inner.connect();
        let _connect = self
            .runtime
            .block_on(async move { connect_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(())
    }

    /// Creates a new stream with the provided ID and name.
    ///
    /// Returns Ok(()) on successful stream creation or a PyRuntimeError on failure.
    fn create_stream(&self, stream_id: u32, name: String) -> PyResult<()> {
        let create_stream_future = self.inner.create_stream(&name, Some(stream_id));
        let _create_stream = self
            .runtime
            .block_on(async move { create_stream_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(())
    }

    /// Creates a new topic with the given parameters.
    ///
    /// Returns Ok(()) on successful topic creation or a PyRuntimeError on failure.
    fn create_topic(
        &self,
        stream_id: u32,
        topic_id: u32,
        partitions_count: u32,
        name: String,
        compression_algorithm: String,
        replication_factor: Option<u8>,
    ) -> PyResult<()> {
        let stream_id = Identifier::numeric(stream_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        let compression_algorithm = CompressionAlgorithm::from_str(&compression_algorithm)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;

        let create_topic_future = self.inner.create_topic(
            &stream_id,
            &name,
            partitions_count,
            compression_algorithm,
            replication_factor,
            Some(topic_id),
            IggyExpiry::NeverExpire,
            None,
        );
        let _create_topic = self
            .runtime
            .block_on(async move { create_topic_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        PyResult::Ok(())
    }

    /// Sends a list of messages to the specified topic.
    ///
    /// Returns Ok(()) on successful sending or a PyRuntimeError on failure.
    fn send_messages(
        &self,
        stream_id: u32,
        topic_id: u32,
        partitioning: u32,
        messages: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let messages: Vec<SendMessage> = messages
            .iter()
            .map(|item| item.extract::<SendMessage>())
            .collect::<Result<Vec<_>, _>>()?;
        let mut messages: Vec<RustMessage> = messages
            .into_iter()
            .map(|message| message.inner)
            .collect::<Vec<_>>();

        let stream_id = Identifier::numeric(stream_id).unwrap();
        let topic_id = Identifier::numeric(topic_id).unwrap();
        let partitioning = Partitioning::partition_id(partitioning);

        let send_message_future =
            self.inner
                .send_messages(&stream_id, &topic_id, &partitioning, messages.as_mut());
        self.runtime
            .block_on(async move { send_message_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(())
    }

    /// Polls for messages from the specified topic and partition.
    ///
    /// Returns a list of received messages or a PyRuntimeError on failure.
    fn poll_messages(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        count: u32,
        auto_commit: bool,
    ) -> PyResult<Vec<ReceiveMessage>> {
        let consumer = RustConsumer::default();
        let stream_id = Identifier::numeric(stream_id).unwrap();
        let topic_id = Identifier::numeric(topic_id).unwrap();
        let strategy = PollingStrategy::next();

        let poll_messages = self.inner.poll_messages(
            &stream_id,
            &topic_id,
            Some(partition_id),
            &consumer,
            &strategy,
            count,
            auto_commit,
        );

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
