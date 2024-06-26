use crate::network::NetworkMessage;
use anyhow::Result;

/// Handler
/// Implement this trait as part of the handling logic for incoming network messages
/// This is meant to be used as a "middleware" for the node to consume and create functionality
pub trait Handler {
    fn handle_message(
        &self,
        message: NetworkMessage,
    ) -> impl std::future::Future<Output = Result<NetworkMessage>> + Send;
}

