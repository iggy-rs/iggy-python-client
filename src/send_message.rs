use iggy::messages::send_messages::Message as RustSendMessage;
use pyo3::prelude::*;
use std::str::FromStr;

#[pyclass]
pub struct SendMessage {
    pub(crate) inner: RustSendMessage,
}

impl SendMessage {
    pub fn from_rust_message(message: RustSendMessage) -> Self {
        Self { inner: message }
    }
}

impl Clone for SendMessage {
    fn clone(&self) -> Self {
        Self {
            inner: RustSendMessage::from_str(&self.inner.to_string()).unwrap(),
        }
    }
}

#[pymethods]
impl SendMessage {
    #[new]
    pub fn new(data: String) -> Self {
        let inner = RustSendMessage::from_str(&data).unwrap();
        Self { inner }
    }
}
