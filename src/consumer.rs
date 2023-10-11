use iggy::consumer::Consumer as RustConsumer;
use pyo3::prelude::*;

#[pyclass]
pub struct Consumer {
    inner: RustConsumer,
}
