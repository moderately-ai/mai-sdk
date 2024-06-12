use std::sync::Arc;

use crate::{
    handler::Startable,
    network::{HandlerEvent, NetworkMessage},
    storage::{GetEvent, SetEvent},
};
use anyhow::Result;
use async_channel::{Receiver, Sender};
use slog::{error, info, Logger};
use tokio::select;
use tokio::sync::RwLock;

type EventBridgeChannel<T> = (Sender<T>, Receiver<T>);
type EventBridgeSubscribers<T> = Arc<RwLock<Vec<(Sender<T>, Receiver<T>)>>>;

/// EventBridge
/// Handles routing events of different types between their publishers and subscribers
#[derive(Clone, Debug)]
pub struct EventBridge {
    logger: Logger,

    network_channel: EventBridgeChannel<NetworkMessage>,
    network_subscribers: EventBridgeSubscribers<NetworkMessage>,

    handler_channel: EventBridgeChannel<HandlerEvent>,
    handler_subscribers: EventBridgeSubscribers<HandlerEvent>,

    kv_get_channel: EventBridgeChannel<GetEvent>,
    kv_get_subscribers: EventBridgeSubscribers<GetEvent>,

    kv_set_channel: EventBridgeChannel<SetEvent>,
    kv_set_subscribers: EventBridgeSubscribers<SetEvent>,
}

pub enum PublishEvents {
    NetworkMessage(NetworkMessage),
    HandlerEvent(HandlerEvent),
    GetEvent(GetEvent),
    SetEvent(SetEvent),
}

impl EventBridge {
    pub fn new(logger: &Logger) -> Self {
        let (network_tx, network_rx) = async_channel::unbounded();
        let (handler_tx, handler_rx) = async_channel::unbounded();
        let (kv_get_tx, kv_get_rx) = async_channel::unbounded();
        let (kv_set_tx, kv_set_rx) = async_channel::unbounded();
        Self {
            logger: logger.clone(),
            network_channel: (network_tx, network_rx),
            network_subscribers: Arc::new(RwLock::new(vec![])),
            handler_channel: (handler_tx, handler_rx),
            handler_subscribers: Arc::new(RwLock::new(vec![])),
            kv_get_channel: (kv_get_tx, kv_get_rx),
            kv_get_subscribers: Arc::new(RwLock::new(vec![])),
            kv_set_channel: (kv_set_tx, kv_set_rx),
            kv_set_subscribers: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn publish(&self, event: PublishEvents) -> Result<()> {
        match event {
            PublishEvents::NetworkMessage(message) => {
                if let Err(e) = self.network_channel.0.send(message).await {
                    return Err(anyhow::anyhow!("Failed to publish message: {:?}", e));
                };
                Ok(())
            }
            PublishEvents::HandlerEvent(event) => {
                if let Err(e) = self.handler_channel.0.send(event).await {
                    return Err(anyhow::anyhow!("Failed to publish event: {:?}", e));
                };
                Ok(())
            }
            PublishEvents::GetEvent(event) => {
                if let Err(e) = self.kv_get_channel.0.send(event).await {
                    return Err(anyhow::anyhow!("Failed to publish get event: {:?}", e));
                };
                Ok(())
            }
            PublishEvents::SetEvent(event) => {
                if let Err(e) = self.kv_set_channel.0.send(event).await {
                    return Err(anyhow::anyhow!("Failed to publish set event: {:?}", e));
                };
                Ok(())
            }
        }
    }

    pub async fn subscribe_to_network(&self) -> Receiver<NetworkMessage> {
        let (tx, rx) = async_channel::unbounded();
        self.network_subscribers
            .write()
            .await
            .push((tx, rx.clone()));
        rx
    }

    pub async fn subscribe_to_handler(&self) -> Receiver<HandlerEvent> {
        let (tx, rx) = async_channel::unbounded();
        self.handler_subscribers
            .write()
            .await
            .push((tx, rx.clone()));
        rx
    }

    pub async fn subscribe_to_kv_get(&self) -> Receiver<GetEvent> {
        let (tx, rx) = async_channel::unbounded();
        self.kv_get_subscribers.write().await.push((tx, rx.clone()));
        rx
    }

    pub async fn subscribe_to_kv_set(&self) -> Receiver<SetEvent> {
        let (tx, rx) = async_channel::unbounded();
        self.kv_set_subscribers.write().await.push((tx, rx.clone()));
        rx
    }
}

impl Startable for EventBridge {
    async fn start(&self) -> Result<()> {
        info!(self.logger, "starting event bridge");
        loop {
            select! {
                network_message = self.network_channel.1.recv() => {
                    info!(self.logger, "event bridge handling network message");
                    if let Ok(message) = network_message {
                        for (tx, _) in self.network_subscribers.read().await.iter() {
                            if let Err(e) = tx.send(message.clone()).await {
                                error!(self.logger, "Failed to send message to subscriber: {:?}", e);
                            }
                        }
                    }
                },
                handler_event = self.handler_channel.1.recv() => {
                    info!(self.logger, "event bridge handling handler message");
                    if let Ok(event) = handler_event {
                        for (tx, _) in self.handler_subscribers.read().await.iter() {
                            if let Err(e) = tx.send(event.clone()).await {
                                error!(self.logger, "Failed to send event to subscriber: {:?}", e);
                            }
                        }
                    }
                },
                get_event = self.kv_get_channel.1.recv() => {
                    info!(self.logger, "event bridge handling get event");
                    if let Ok(event) = get_event {
                        for (tx, _) in self.kv_get_subscribers.read().await.iter() {
                            if let Err(e) = tx.send(event.clone()).await {
                                error!(self.logger, "Failed to send get event to subscriber: {:?}", e);
                            }
                        }
                    }
                },
                set_event = self.kv_set_channel.1.recv() => {
                    info!(self.logger, "event bridge handling set event");
                    if let Ok(event) = set_event {
                        for (tx, _) in self.kv_set_subscribers.read().await.iter() {
                            if let Err(e) = tx.send(event.clone()).await {
                                error!(self.logger, "Failed to send set event to subscriber: {:?}", e);
                            }
                        }
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use slog::o;

    use super::*;

    #[tokio::test]
    async fn test_handler_event() -> Result<()> {
        let logger = slog::Logger::root(slog::Discard, o!());
        let event_bridge = EventBridge::new(&logger);
        {
            let event_bridge = event_bridge.clone();
            tokio::spawn(async move {
                event_bridge.start().await.unwrap();
            });
        }

        let handler_event = HandlerEvent::new(
            NetworkMessage::new("test".to_string(), vec![]),
            Some("peer_id".to_string()),
            Some("topic".to_string()),
        );

        let first_subscriber = event_bridge.subscribe_to_handler().await;
        let second_subscriber = event_bridge.subscribe_to_handler().await;

        event_bridge
            .publish(PublishEvents::HandlerEvent(handler_event.clone()))
            .await?;

        assert_eq!(first_subscriber.recv().await.unwrap(), handler_event);
        assert_eq!(second_subscriber.recv().await.unwrap(), handler_event);

        Ok(())
    }
}
