use iggy::models::messages::Message as RustReceiveMessage;
use pyo3::prelude::*;

#[pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
}

impl ReceiveMessage {
    pub fn from_rust_message(message: RustReceiveMessage) -> Self {
        Self { inner: message }
    }
}
