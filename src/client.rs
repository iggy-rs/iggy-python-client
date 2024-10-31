use std::str::FromStr;

use iggy::client::TopicClient;
use iggy::client::{Client, MessageClient, StreamClient, UserClient};
use iggy::clients::builder::IggyClientBuilder;
use iggy::clients::client::IggyClient as RustIggyClient;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::consumer::Consumer as RustConsumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy as RustPollingStrategy;
use iggy::messages::send_messages::{Message as RustMessage, Partitioning};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::runtime::{Builder, Runtime};

use crate::receive_message::{PollingStrategy, ReceiveMessage};
use crate::send_message::SendMessage;
use crate::stream::StreamDetails;
use crate::topic::TopicDetails;

/// A Python class representing the Iggy client.
/// It wraps the RustIggyClient and provides asynchronous functionality
/// through the contained runtime.
#[pyclass]
pub struct IggyClient {
    inner: RustIggyClient,
    runtime: Runtime,
}

#[derive(FromPyObject)]
enum PyIdentifier {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "int")]
    Int(u32),
}

impl From<PyIdentifier> for Identifier {
    fn from(py_identifier: PyIdentifier) -> Self {
        match py_identifier {
            PyIdentifier::String(s) => Identifier::from_str(&s).unwrap(),
            PyIdentifier::Int(i) => Identifier::numeric(i).unwrap(),
        }
    }
}

#[pymethods]
impl IggyClient {
    /// Constructs a new IggyClient.
    ///
    /// This initializes a new runtime for asynchronous operations.
    /// Future versions might utilize asyncio for more Pythonic async.
    #[new]
    #[pyo3(signature = (conn=None))]
    fn new(conn: Option<String>) -> Self {
        // TODO: use asyncio
        let runtime = Builder::new_multi_thread()
            .worker_threads(4) // number of worker threads
            .enable_all() // enables all available Tokio features
            .build()
            .unwrap();
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(conn.unwrap_or("127.0.0.1:8090".to_string()))
            .build()
            .unwrap();
        IggyClient {
            inner: client,
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
    #[pyo3(signature = (name, stream_id = None))]
    fn create_stream(&self, name: String, stream_id: Option<u32>) -> PyResult<()> {
        let create_stream_future = self.inner.create_stream(&name, stream_id);
        let _create_stream = self
            .runtime
            .block_on(async move { create_stream_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(())
    }

    /// Gets stream by id.
    ///
    /// Returns Option of stream details or a PyRuntimeError on failure.
    fn get_stream(&self, stream_id: PyIdentifier) -> PyResult<Option<StreamDetails>> {
        let stream_id = Identifier::from(stream_id);
        let stream_future = self.inner.get_stream(&stream_id);
        let stream = self
            .runtime
            .block_on(async move { stream_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(stream.map(StreamDetails::from))
    }

    /// Creates a new topic with the given parameters.
    ///
    /// Returns Ok(()) on successful topic creation or a PyRuntimeError on failure.
    #[pyo3(
        signature = (stream, name, partitions_count, compression_algorithm = None, topic_id = None, replication_factor = None)
    )]
    fn create_topic(
        &self,
        stream: PyIdentifier,
        name: String,
        partitions_count: u32,
        compression_algorithm: Option<String>,
        topic_id: Option<u32>,
        replication_factor: Option<u8>,
    ) -> PyResult<()> {
        let compression_algorithm = match compression_algorithm {
            Some(algo) => CompressionAlgorithm::from_str(&algo).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e))
            })?,
            None => CompressionAlgorithm::default(),
        };

        let stream = Identifier::from(stream);
        let create_topic_future = self.inner.create_topic(
            &stream,
            &name,
            partitions_count,
            compression_algorithm,
            replication_factor,
            topic_id,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        );
        let _create_topic = self
            .runtime
            .block_on(async move { create_topic_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        PyResult::Ok(())
    }

    /// Gets topic by stream and id.
    ///
    /// Returns Option of topic details or a PyRuntimeError on failure.
    fn get_topic(
        &self,
        stream_id: PyIdentifier,
        topic_id: PyIdentifier,
    ) -> PyResult<Option<TopicDetails>> {
        let stream_id = Identifier::from(stream_id);
        let topic_id = Identifier::from(topic_id);
        let topic_future = self.inner.get_topic(&stream_id, &topic_id);
        let topic = self
            .runtime
            .block_on(async move { topic_future.await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{:?}", e)))?;
        Ok(topic.map(TopicDetails::from))
    }

    /// Sends a list of messages to the specified topic.
    ///
    /// Returns Ok(()) on successful sending or a PyRuntimeError on failure.
    fn send_messages(
        &self,
        stream: PyIdentifier,
        topic: PyIdentifier,
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

        let stream = Identifier::from(stream);
        let topic = Identifier::from(topic);
        let partitioning = Partitioning::partition_id(partitioning);

        let send_message_future =
            self.inner
                .send_messages(&stream, &topic, &partitioning, messages.as_mut());
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
        stream: PyIdentifier,
        topic: PyIdentifier,
        partition_id: u32,
        polling_strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> PyResult<Vec<ReceiveMessage>> {
        let consumer = RustConsumer::default();
        let stream = Identifier::from(stream);
        let topic = Identifier::from(topic);
        let strategy: RustPollingStrategy = (*polling_strategy).into();

        let poll_messages = self.inner.poll_messages(
            &stream,
            &topic,
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
