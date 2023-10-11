use iggy::models::messages::Message as RustReceiveMessage;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass]
pub struct ReceiveMessage {
    pub(crate) inner: RustReceiveMessage,
}

impl ReceiveMessage {
    pub fn from_rust_message(message: RustReceiveMessage) -> Self {
        Self { inner: message }
    }
}

#[pymethods]
impl ReceiveMessage {
    pub fn payload(&self, py: Python) -> PyObject {
        PyBytes::new(py, &self.inner.payload.to_vec()).into()
    }

    pub fn offset(&self) -> u64 {
        self.inner.offset
    }
}
