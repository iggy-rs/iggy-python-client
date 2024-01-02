use crate::pyclass;
use crate::pymethods;

/// Maps to iggy::identifier::IdKind
#[pyclass]
#[derive(Default, Clone)]
pub enum IdKind {
  #[default]
  Numeric,
  String,
}
impl Into<iggy::identifier::IdKind> for IdKind {
  fn into(self) -> iggy::identifier::IdKind {
    match self {
      IdKind::Numeric => iggy::identifier::IdKind::Numeric,
      IdKind::String => iggy::identifier::IdKind::String,
    }
  }
}

/// Maps to iggy::identifier::Identifier
#[pyclass]
#[derive(Default, Clone)]
pub struct Identifier {
  pub kind: IdKind,
  pub length: u8,
  pub value: Vec<u8>,
}
impl Into<iggy::identifier::Identifier> for Identifier {
  fn into(self) -> iggy::identifier::Identifier {
    iggy::identifier::Identifier {
      kind: self.kind.into(),
      length: self.length,
      value: self.value,
    }
  }
}
#[pymethods]
impl Identifier {
  #[new]
  pub fn new(value: u32) -> Self {
    Self {
      kind: IdKind::Numeric,
      length: 4,
      value: value.to_le_bytes().to_vec(),
    }
  }
}