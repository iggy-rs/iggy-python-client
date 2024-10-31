use iggy::models::topic::TopicDetails as RustTopicDetails;
use pyo3::prelude::*;

#[pyclass]
pub struct TopicDetails {
    pub(crate) inner: RustTopicDetails,
}

impl From<RustTopicDetails> for TopicDetails {
    fn from(topic_details: RustTopicDetails) -> Self {
        Self {
            inner: topic_details,
        }
    }
}

#[pymethods]
impl TopicDetails {
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.partitions_count
    }
}
