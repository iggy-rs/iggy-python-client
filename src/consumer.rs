use pyo3;
use crate::pyclass;
use crate::pymethods;
use crate::identifier;

/// Maps to iggy::consumer::ConsumerKind
#[pyclass]
#[derive(Default, Clone)]
pub enum ConsumerKind {
  #[default]
  Consumer,
  ConsumerGroup,
}
impl Into<iggy::consumer::ConsumerKind> for ConsumerKind {
  fn into(self) -> iggy::consumer::ConsumerKind {
    match self {
      ConsumerKind::Consumer => iggy::consumer::ConsumerKind::Consumer,
      ConsumerKind::ConsumerGroup => iggy::consumer::ConsumerKind::ConsumerGroup,
    }
  }
}
/// Maps to iggy::consumer::Consumer
#[pyclass]
pub struct Consumer {
    pub kind: ConsumerKind,
    pub id: crate::identifier::Identifier,
}
impl Into<iggy::consumer::Consumer> for Consumer {
  fn into(self) -> iggy::consumer::Consumer {
    iggy::consumer::Consumer {
      kind: self.kind.into(),
      id: self.id.into(),
    }
  }
}
#[pymethods]
impl Consumer {
    #[new]
    pub fn new(id: pyo3::PyRef<identifier::Identifier>) -> Self {
        Self {
            kind: ConsumerKind::Consumer,
            id: id.clone(),
        }
    }
}
