use iggy::client_provider::{self, get_raw_client};
use pyo3::PyResult;

use crate::client::Client;

#[pyclass]
pub struct ClientProviderConfig {
    client_provider_config: client_provider::ClientProviderConfig,
}

#[pymethod]
pub async fn get_client(config: &ClientProviderConfig) -> PyResult<Client> {
    let client = get_raw_client(config.client_provider_config.into()).await?;
    let python_client = Client { client };
    Ok(python_client)
}
