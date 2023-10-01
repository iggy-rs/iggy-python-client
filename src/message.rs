use iggy::messages::send_messages::Message as RustMessage;
use pyo3::prelude::*;
use std::str::FromStr;

#[pyclass]
pub struct Message {
    pub(crate) inner: RustMessage,
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            inner: RustMessage::from_str(&self.inner.to_string()).unwrap(),
        }
    }
}

#[pymethods]
impl Message {
    #[new]
    fn new(data: String) -> Self {
        let inner = RustMessage::from_str(&data).unwrap();
        Message { inner }
    }
}
