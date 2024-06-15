use anyhow::{bail, Result};
use async_channel::{Receiver, Sender};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use either::Either;
use libp2p::pnet::PreSharedKey;
use libp2p::Transport;
use libp2p::{
    autonat,
    futures::StreamExt,
    gossipsub, identify,
    kad::{self, store::MemoryStore, QueryId},
    mdns, noise, ping, pnet, relay,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr,
};
use slog::{debug, error, info, warn, Logger};
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};
use tokio::{select, sync::RwLock};

use crate::{
    event_bridge::{EventBridge, PublishEvents},
    handler::Startable,
    distributed_kv_store::{GetEvent, SetEvent},
};

use serde::{Deserialize, Serialize};

pub type PeerId = String;

pub type Topic = String;

pub struct PeerInquiryResponse {
    peer_ids: Vec<PeerId>,
}

pub struct PeerInquiry {
    response_tx: Sender<PeerInquiryResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkMessage {
    /// Type of message
    pub message_type: String,

    /// Payload of the message
    pub payload: Vec<u8>,
}

impl NetworkMessage {
    pub fn new(message_type: String, payload: Vec<u8>) -> Self {
        Self {
            message_type,
            payload,
        }
    }
}

pub trait Network {
    /// Returns the local peer id
    fn peer_id(&self) -> String;

    /// Returns a list of peers connected to the network
    fn peers(&self) -> impl std::future::Future<Output = Vec<PeerId>> + Send;
}

/// Contains information about a message received from the network
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandlerEvent {
    message: NetworkMessage,
    peer_id: Option<PeerId>,
    topic: Option<Topic>,
}

impl HandlerEvent {
    pub fn new(message: NetworkMessage, peer_id: Option<PeerId>, topic: Option<Topic>) -> Self {
        Self {
            peer_id,
            message,
            topic,
        }
    }

    pub fn peer_id(&self) -> Option<String> {
        self.peer_id.clone()
    }

    pub fn message(&self) -> NetworkMessage {
        self.message.clone()
    }

    pub fn topic(&self) -> Option<String> {
        self.topic.as_ref().map(|topic| topic.to_string())
    }
}

#[derive(NetworkBehaviour)]
struct P2PNetworkBehaviour {
    gossipsub: gossipsub::Behaviour,

    ping: ping::Behaviour,

    mdns: mdns::tokio::Behaviour,

    autonat: autonat::Behaviour,

    kad: kad::Behaviour<MemoryStore>,

    relay: relay::Behaviour,

    identify: identify::Behaviour,
}

#[derive(Debug, Clone)]
pub struct P2PNetwork {
    /// module logger
    logger: Logger,

    /// Listener addresses for the libp2p network
    listen_addrs: Vec<Multiaddr>,

    /// Bootstrap addresses for the libp2p network
    bootstrap_addrs: Vec<Multiaddr>,

    /// Ping interval
    ping_interval: std::time::Duration,

    /// Gossipsub heartbeat interval
    gossipsub_heartbeat_interval: std::time::Duration,

    /// Peer inquiry channel
    peer_inquiry_tx: Sender<PeerInquiry>,

    /// Peer inquiry channel
    peer_inquiry_rx: Receiver<PeerInquiry>,

    // Bridge
    bridge: EventBridge,

    /// Keypair for the broker network
    keypair: libp2p::identity::Keypair,

    /// KV get requests
    kv_get_store: Arc<RwLock<HashMap<QueryId, GetEvent>>>,

    /// Pre shared key for the network
    psk: Option<pnet::PreSharedKey>,
}

pub struct P2PNetworkConfig {
    pub listen_addrs: Vec<Multiaddr>,
    pub bootstrap_addrs: Vec<Multiaddr>,
    pub ping_interval: std::time::Duration,
    pub gossipsub_heartbeat_interval: std::time::Duration,
    pub logger: Logger,
    pub bridge: EventBridge,
    pub psk: Option<String>,
}

impl P2PNetwork {
    pub fn new(cfg: P2PNetworkConfig) -> Self {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let (peer_inquiry_tx, peer_inquiry_rx) = async_channel::unbounded();
        let kv_get_store = Arc::new(RwLock::new(HashMap::new()));
        let psk = if let Some(psk) = cfg.psk {
            let psk_bytes: [u8; 32] = BASE64_STANDARD
                .decode(psk.as_bytes())
                .unwrap()
                .try_into()
                .unwrap();
            let psk = PreSharedKey::new(psk_bytes);
            Some(psk)
        } else {
            None
        };
        Self {
            logger: cfg.logger,
            kv_get_store,
            listen_addrs: cfg.listen_addrs,
            ping_interval: cfg.ping_interval,
            gossipsub_heartbeat_interval: cfg.gossipsub_heartbeat_interval,
            keypair,
            peer_inquiry_rx,
            peer_inquiry_tx,
            bridge: cfg.bridge,
            bootstrap_addrs: cfg.bootstrap_addrs,
            psk,
        }
    }
}

async fn handle_set_event(kad: &mut kad::Behaviour<MemoryStore>, event: SetEvent) -> Result<()> {
    let key = kad::RecordKey::new(&event.key.as_bytes());
    kad.start_providing(key.clone())?;
    let value = event.value.clone();
    let record = kad::Record {
        key,
        value,
        publisher: None,
        expires: None,
    };
    if let Err(e) = kad.put_record(record, kad::Quorum::One) {
        event.result.send(Err(e.into())).await?;
    } else {
        event.result.send(Ok(())).await?;
    };
    Ok(())
}

impl Startable for P2PNetwork {
    async fn start(&self) -> Result<()> {
        info!(self.logger, "building p2p network");

        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(self.keypair.clone())
            .with_tokio()
            .with_other_transport(|key| {
                let noise_config = noise::Config::new(key).unwrap();
                let yamux_config = yamux::Config::default();

                let base_transport =
                    tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
                let maybe_encrypted =
                    match self.psk {
                        Some(psk) => Either::Left(base_transport.and_then(move |socket, _| {
                            pnet::PnetConfig::new(psk).handshake(socket)
                        })),
                        None => Either::Right(base_transport),
                    };
                maybe_encrypted
                    .upgrade(libp2p::core::transport::upgrade::Version::V1Lazy)
                    .authenticate(noise_config)
                    .multiplex(yamux_config)
            })?
            .with_behaviour(|key| {
                let local_peer_id = key.public().to_peer_id();
                let message_id_fn = |message: &gossipsub::Message| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    gossipsub::MessageId::from(s.finish().to_string())
                };
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(self.gossipsub_heartbeat_interval)
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .message_id_fn(message_id_fn)
                    .build()?;
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;
                let ping =
                    ping::Behaviour::new(ping::Config::new().with_interval(self.ping_interval));
                let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;
                let autonat = autonat::Behaviour::new(local_peer_id, Default::default());
                let kad = kad::Behaviour::new(local_peer_id, MemoryStore::new(local_peer_id));
                let relay = relay::Behaviour::new(local_peer_id, Default::default());
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/TODO/0.0.1".to_string(),
                    key.public(),
                ));
                Ok(P2PNetworkBehaviour {
                    gossipsub,
                    ping,
                    mdns,
                    autonat,
                    kad,
                    relay,
                    identify,
                })
            })?
            .build();

        swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server));

        // Dial bootstrap nodes
        for bootstrap_addr in self.bootstrap_addrs.iter() {
            swarm.dial(bootstrap_addr.clone())?;
        }

        // Setup local listeners
        for addr in self.listen_addrs.iter() {
            swarm.listen_on(addr.clone())?;
        }

        // Subscribe to a common topic where broadcast messages will be sent
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&gossipsub::IdentTopic::new("broadcast".to_string()))?;

        // Setup channel subscriptions
        let network_rx: Receiver<NetworkMessage> = self.bridge.subscribe_to_network().await;
        let kv_get_rx: Receiver<GetEvent> = self.bridge.subscribe_to_kv_get().await;
        let kv_set_rx: Receiver<SetEvent> = self.bridge.subscribe_to_kv_set().await;

        // Start event loop
        info!(self.logger, "starting broker network");
        loop {
            select! {
                // handle new kv get events
                get_event = kv_get_rx.recv() => match get_event {
                    Ok(event) =>{
                        let key = kad::RecordKey::new(&event.key.as_bytes());
                        let result = swarm.behaviour_mut().kad.get_record(key);
                        self.kv_get_store.write().await.insert(result, event);
                    },
                    Err(e) => {
                        error!(self.logger, "kv get rx channel closed: {e}");
                        bail!("internal channel error")
                    }
                },
                // handle new kv set events
                set_event = kv_set_rx.recv() => match set_event {
                    Ok(event) => {
                        if let Err(e) = handle_set_event(&mut swarm.behaviour_mut().kad, event).await {
                            error!(self.logger, "failed to handle set event: {e}");
                        }
                    },
                    Err(e) => {
                        error!(self.logger, "kv set rx channel closed: {e}");
                        bail!("internal channel error")
                    }
                },
                // handle new peer inquiries
                peer_inquiry = self.peer_inquiry_rx.recv() => match peer_inquiry {
                    Ok(inquiry) => {
                        info!(self.logger, "received peer inquiry");
                        let peer_ids = swarm.connected_peers().map(|peer_id| peer_id.to_string()).collect();
                        let response = PeerInquiryResponse { peer_ids };
                        if let Err(e) = inquiry.response_tx.send(response).await {
                            error!(self.logger, "failed to send peer inquiry response: {e}");
                        }
                    },
                    Err(e) => {
                        error!(self.logger, "peer inquiry rx channel closed: {e}");
                        bail!("internal channel error")
                    }
                },
                // handle new events from the publish rx channel
                publish_event = network_rx.recv() => match publish_event {
                    Ok(event) => {
                        let message = bincode::serialize(&event)?;

                        // Publish the network to the gossipsub network
                        if let Err(e) = swarm.behaviour_mut().gossipsub.publish(
                            gossipsub::IdentTopic::new("broadcast"),
                            message,
                        ) {
                            // If the error is due to insufficient peers, we can safely ignore since we propagate the message to the local handler
                            match e {
                                gossipsub::PublishError::InsufficientPeers => {
                                    info!(self.logger, "no peers to publish message to, message will only propagate locally");
                                },
                                e => {
                                    warn!(self.logger, "failed to publish message: {e}");
                                },
                            }
                        };

                        // Publish the network to the local handler
                        // NOTE: we do this to allow the local system to handle any jobs it has capacity for
                        let local_handler_event = HandlerEvent {
                            peer_id: Some(self.peer_id()),
                            topic: None,
                            message: event,
                        };
                        if let Err(e) = self.bridge.publish(crate::event_bridge::PublishEvents::HandlerEvent(local_handler_event)).await {
                            error!(self.logger, "failed to send message to handler: {e}");
                        } else {
                            info!(self.logger, "notified local");
                        };
                    },
                    Err(e) => {
                        error!(self.logger, "publish rx channel closed: {e}");
                        bail!("internal channel error")
                    }
                },
                // handle new events from the swarm network
                event = swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(P2PNetworkBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, multiaddr) in list {
                            info!(self.logger, "mDNS discovered a new peer: {peer_id}");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            swarm.behaviour_mut().kad.add_address(&peer_id, multiaddr);
                        }
                    },
                    SwarmEvent::Behaviour(P2PNetworkBehaviourEvent::Identify(identify::Event::Received {
                        peer_id,
                        info: identify::Info { observed_addr, .. },
                    })) => {
                        info!(self.logger, "received identify info from {peer_id}");
                        swarm.add_external_address(observed_addr.clone());
                    },
                    SwarmEvent::Behaviour(P2PNetworkBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _multiaddr) in list {
                            info!(self.logger, "mDNS discover peer has expired: {peer_id}");
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    },
                    SwarmEvent::Behaviour(P2PNetworkBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message_id: id,
                        message,
                    })) => {
                        let topic = message.topic.clone();
                        info!(self.logger, "received message {id} from {peer_id} on topic {topic}");
                        let message = bincode::deserialize(&message.data)?;
                        if let Err(e) = self.bridge.publish(PublishEvents::HandlerEvent(message)).await {
                            error!(self.logger, "failed to send message to handler: {e}");
                        };
                    },
                    SwarmEvent::NewExternalAddrOfPeer { peer_id, .. }  => {
                        info!(
                            self.logger,
                            "discovered new address for peer";
                            "peer_id" => peer_id.to_string()
                        );
                    },
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!(self.logger, "local node is listening on {address}");
                    }
                    SwarmEvent::Behaviour(P2PNetworkBehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {id: query_id, result, ..})) => {
                        match result {
                            kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders { key, providers, .. })) => {
                                for peer in providers {
                                    info!(
                                        self.logger,
                                        "Peer {peer:?} provides key {:?}",
                                        std::str::from_utf8(key.as_ref()).unwrap()
                                    );
                                }
                            }
                            kad::QueryResult::GetProviders(Err(err)) => {
                                error!(self.logger, "Failed to get providers: {err:?}");
                            }
                            kad::QueryResult::GetRecord(Ok(
                                kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                                    record: kad::Record { key, value, .. },
                                    ..
                                })
                            )) => {
                                info!(
                                    self.logger,
                                    "got record";
                                    "key" => std::str::from_utf8(key.as_ref()).unwrap(),
                                );
                                let event = self.kv_get_store.write().await.remove(&query_id);
                                if let Some(event) = event {
                                    event.result.send(Ok(Some(value))).await?;
                                }
                            }
                            kad::QueryResult::GetRecord(Ok(_)) => {}
                            kad::QueryResult::GetRecord(Err(err)) => {
                                error!(self.logger, "Failed to get record: {err:?}");
                                let event = self.kv_get_store.write().await.remove(&query_id);
                                if let Some(event) = event {
                                    event.result.send(Err(err.into())).await?;
                                }
                            }
                            kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                                info!(
                                    self.logger,
                                    "Successfully put record {:?}",
                                    std::str::from_utf8(key.as_ref()).unwrap()
                                );
                            }
                            kad::QueryResult::PutRecord(Err(err)) => {
                                error!(self.logger, "Failed to put record: {err:?}");
                            }
                            kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { key })) => {
                                info!(
                                    self.logger,
                                    "Successfully put provider record {:?}",
                                    std::str::from_utf8(key.as_ref()).unwrap()
                                );
                            }
                            kad::QueryResult::StartProviding(Err(err)) => {
                                eprintln!("Failed to put provider record: {err:?}");
                            }
                            _ => {}
                        }
                    }
                    _ => {
                        debug!(self.logger, "unhandled event {:?}", event);
                    }
                }
            }
        }
    }
}

impl Network for P2PNetwork {
    fn peer_id(&self) -> String {
        self.keypair.public().to_peer_id().to_string()
    }

    async fn peers(&self) -> Vec<PeerId> {
        let (tx, rx) = async_channel::bounded(1);
        let inquiry = PeerInquiry { response_tx: tx };
        if let Err(e) = self.peer_inquiry_tx.send(inquiry).await {
            error!(self.logger, "failed to send peer inquiry: {e}");
            return vec![];
        }
        match rx.recv().await {
            Ok(response) => response.peer_ids,
            Err(e) => {
                error!(self.logger, "failed to receive peer inquiry response: {e}");
                vec![]
            }
        }
    }
}
