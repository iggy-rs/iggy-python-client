mod client;
mod message;

use client::IggyClient;
use message::Message;
use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn iggy_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<IggyClient>()?;
    Ok(())
}
