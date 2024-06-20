/*
Service Module
Contains traits and other utilities for defining services
*/

/// Startable
/// Implement this trait to allow the node to start the handler
pub trait Startable {
    fn start(&self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}
