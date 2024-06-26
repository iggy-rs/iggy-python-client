use iggy::messages::send_messages::Message as RustSendMessage;
use pyo3::prelude::*;
use std::str::FromStr;

/// A Python class representing a message to be sent.
///
/// This class wraps a Rust message meant for sending, facilitating
/// the creation of such messages from Python and their subsequent use in Rust.
#[pyclass]
pub struct SendMessage {
    pub(crate) inner: RustSendMessage,
}

/// Provides the capability to clone a SendMessage.
///
/// This implementation creates a new `RustSendMessage` instance from
/// the string representation of the original `RustSendMessage`.
impl Clone for SendMessage {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[pymethods]
impl SendMessage {
    /// Constructs a new `SendMessage` instance from a string.
    ///
    /// This method allows for the creation of a `SendMessage` instance
    /// directly from Python using the provided string data.
    #[new]
    pub fn new(data: String) -> Self {
        // TODO: handle errors
        let inner = RustSendMessage::from_str(&data).unwrap();
        Self { inner }
    }
}
