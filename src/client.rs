use iggy::client::Client as RustClient;
use iggy::client_provider::ClientProviderConfig as RustClientProviderConfig;

#[pyclass]
pub struct Client {
    client: Box<dyn RustClient>,
}
