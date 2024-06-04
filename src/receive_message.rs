use iggy::models::messages::PolledMessage as RustReceiveMessage;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// A Python class representing a received message.
///
/// This class wraps a Rust message, allowing for access to its payload and offset from Python.
#[pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
}

impl ReceiveMessage {
    /// Converts a Rust message into its corresponding Python representation.
    ///
    /// This is an internal utility function, not exposed to Python.
    pub(crate) fn from_rust_message(message: RustReceiveMessage) -> Self {
        Self { inner: message }
    }
}

#[pymethods]
impl ReceiveMessage {
    /// Retrieves the payload of the received message.
    ///
    /// The payload is returned as a Python bytes object.
    pub fn payload(&self, py: Python) -> PyObject {
        PyBytes::new_bound(py, &self.inner.payload.to_vec()).into()
    }

    /// Retrieves the offset of the received message.
    ///
    /// The offset represents the position of the message within its topic.
    pub fn offset(&self) -> u64 {
        self.inner.offset
    }
}
