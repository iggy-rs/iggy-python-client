use iggy::messages::send_messages::Message as RustMessage;
use pyo3::{prelude::*, types::PyString};
use std::str::FromStr;

#[pyclass]
pub struct Message {
    pub(crate) inner: RustMessage,
}

#[pymethods]
impl Message {
    fn from_str(data: String) -> Self {
        let inner = RustMessage::from_str(&data).unwrap();
        Message { inner }
    }
}

impl<'source> FromPyObject<'source> for Message {
    fn extract(obj: &'source PyAny) -> PyResult<Self> {
        let inner: RustMessage = obj.getattr("inner")?.downcast_unchecked();
        Ok(Message { inner })
    }
}
