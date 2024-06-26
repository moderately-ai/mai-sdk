use anyhow::Result;

pub type TaskId = String;

/// A task that can be run by the task queue
pub trait Runnable<RunnableOutput, RunnableState> {
    /// a unique identifier for the task, should be unique across all tasks, nodes and plugins
    fn id(&self) -> TaskId;

    /// run the task given the parameters of the task
    fn run(
        &self,
        state: RunnableState,
    ) -> impl std::future::Future<Output = Result<RunnableOutput>> + Send;
}

/// A lifecycle trait that can be implemented by tasks to perform actions before and after the task is executed
pub trait Lifecycle<RunnableState> {
    /// Called before the task is submitted to the task queue
    fn pre_submit(
        &self,
        _state: &RunnableState,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

use std::{collections::HashMap, sync::Arc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use slog::{debug, error, info, Logger};
use tokio::sync::RwLock;

use crate::{
    distributed_kv_store::{DistributedKVStore, OwnedTasks},
    event_bridge::{EventBridge, PublishEvents},
    network::{NetworkMessage, PeerId},
    service::Startable,
};

const BID_ACCEPTANCE: &str = "bid_acceptance";
const REQUEST_FOR_BIDS: &str = "request_for_bids";
const TASK_ERROR: &str = "task_error";
const TASK_COMPLETE: &str = "task_complete";
const BID: &str = "bid";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Initialize {
    pub task: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestForBid {
    task_id: TaskId,
    peer_id: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SolicitForBid {
    task_id: TaskId,
    peer_ids: Vec<PeerId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Bid {
    task_id: TaskId,
    peer_id: PeerId,
    bid: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BidAcceptance {
    task_id: TaskId,
    peer_id: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskComplete {
    task_id: TaskId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskProgress {
    task_id: TaskId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskError {
    task_id: TaskId,
    peer_id: PeerId,
    error: String,
}

/// A distributed task queue.
/// Executes tasks across 1 or more nodes, the behaviour of this implementation is at least once.
#[derive(Debug, Clone)]
pub struct DistributedTaskQueue<TTask, TTaskOutput, RunnableState> {
    /// Logger
    logger: Logger,

    /// An identifier for the local node
    local_peer_id: PeerId,

    /// Tasks that are owned by the local node, these are tasks that have been submitted to the queue
    owned_tasks: OwnedTasks<TTask, TTaskOutput>,

    /// A map of task ids to bids for the task
    task_bids: Arc<RwLock<HashMap<TaskId, Vec<Bid>>>>,

    /// State that is passed to the runnable tasks
    runnable_state: RunnableState,

    /// Event bridge
    event_bridge: EventBridge,

    /// Distributed kv store
    distributed_kv_store: DistributedKVStore,
}

impl<
        TTask: Clone + Runnable<TTaskOutput, TRunnableState> + Serialize + Lifecycle<TRunnableState>,
        TTaskOutput: Clone,
        TRunnableState: Clone,
    > DistributedTaskQueue<TTask, TTaskOutput, TRunnableState>
{
    pub fn new(
        logger: &Logger,
        local_peer_id: &PeerId,
        runnable_state: &TRunnableState,
        bridge: &EventBridge,
        distributed_kv_store: &DistributedKVStore,
    ) -> Self {
        Self {
            logger: logger.clone(),
            local_peer_id: local_peer_id.clone(),
            owned_tasks: Arc::new(RwLock::new(HashMap::new())),
            runnable_state: runnable_state.clone(),
            event_bridge: bridge.clone(),
            distributed_kv_store: distributed_kv_store.clone(),
            task_bids: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn execute_task(&self, task: TTask) -> Result<TTaskOutput> {
        // Setup the communication channel
        let (tx, rx) = async_channel::bounded(1);

        // Call the pre_submit lifecycle hook
        task.pre_submit(&self.runnable_state).await?;

        // Store the task in the owned_tasks map
        {
            let mut owned_tasks = self.owned_tasks.write().await;
            owned_tasks.insert(task.id(), (task.clone(), tx.clone()));
        }

        // Initialize the task in the task_bids map
        {
            let mut task_bids = self.task_bids.write().await;
            task_bids.insert(task.id(), Vec::new());
        }

        // Store the task in the distributed kv store
        let serialized_task = serde_json::to_vec(&task)?;
        self.distributed_kv_store
            .set(format!("task/{}", task.id()), serialized_task)
            .await?;

        let request_for_bids = RequestForBid {
            task_id: task.id(),
            peer_id: self.local_peer_id.clone(),
        };
        self.event_bridge
            .publish(PublishEvents::NetworkMessage(NetworkMessage {
                message_type: REQUEST_FOR_BIDS.to_string(),
                payload: serde_json::to_vec(&request_for_bids)?,
            }))
            .await?;

        // Collect bids for the task until we have a bid from at least one other node OR a timeout is reached
        // TODO: rewrite this logic with a more robust algorithm later
        let start = std::time::Instant::now();
        while start.elapsed().as_secs() < 1 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let task_bids = self.task_bids.read().await;
            if task_bids.get(&task.id()).unwrap().len() > 1 {
                break;
            }
        }
        let mut bids = {
            let mut task_bids = self.task_bids.write().await;
            task_bids.remove(&task.id()).unwrap()
        };

        // Sort the bids by bid value and select the first
        bids.sort_by(|a, b| a.bid.partial_cmp(&b.bid).unwrap());
        let selected_bid = bids.first().cloned();
        info!(self.logger, "received bids"; "bids" => format!("{:?}", bids), "selected_bid" => format!("{:?}", selected_bid));

        // If we have a selected bid, emit a bid acceptance message else run the task locally
        if let Some(selected_bid) = selected_bid {
            // Emit bid acceptance message
            let bid_acceptance = BidAcceptance {
                task_id: task.id(),
                peer_id: selected_bid.peer_id.clone(),
            };
            self.event_bridge
                .publish(PublishEvents::NetworkMessage(NetworkMessage {
                    message_type: BID_ACCEPTANCE.to_string(),
                    payload: serde_json::to_vec(&bid_acceptance)?,
                }))
                .await?;

            // Wait for the task to complete
            return rx.recv().await.map_err(|e| e.into());
        } else {
            return task.run(self.runnable_state.clone()).await;
        }
    }
}

impl<
        TTask: Runnable<TTaskOutput, TRunnableState>
            + Send
            + Sync
            + DeserializeOwned
            + Serialize
            + Clone
            + 'static,
        TTaskOutput: std::fmt::Debug + Clone + Send + Sync + DeserializeOwned + Serialize + 'static,
        TRunnableState: Clone + Send + Sync + 'static,
    > Startable for DistributedTaskQueue<TTask, TTaskOutput, TRunnableState>
{
    async fn start(&self) -> Result<()> {
        info!(self.logger, "starting distributed task queue");
        let handler_rx = self.event_bridge.subscribe_to_handler().await;
        loop {
            // Wait for the next handler event
            let event = handler_rx.recv().await;
            match event {
                Ok(event) => {
                    let message = event.message();
                    let logger = self.logger.clone();
                    let bridge = self.event_bridge.clone();
                    let local_peer_id = self.local_peer_id.clone();
                    let owned_tasks = self.owned_tasks.clone();
                    let runnable_state = self.runnable_state.clone();
                    let distributed_kv_store = self.distributed_kv_store.clone();
                    let task_bids = self.task_bids.clone();
                    tokio::spawn(async move {
                        /*
                         * The happy path flow for a job is the following:
                         * 1. A node sends a request for bids to all other nodes
                         * 2. All other nodes that can execute the task respond with a bid
                         * 3. The node that requested the bid selects the best bid and sends a bid acceptance to the winning node
                         * 4. The winning node executes the task and sends a task complete message to the requesting node
                         * 5. The requesting node receives the task complete message and stores the output
                         */
                        let message_type = message.message_type.as_str();
                        info!(logger, "received message"; "message_type" => message_type);
                        match message_type {
                            REQUEST_FOR_BIDS => {
                                // Deserialize the request
                                let request_for_bid: RequestForBid =
                                    serde_json::from_slice(&message.payload)?;

                                // Respond with a bid for the task
                                let bid = Bid {
                                    task_id: request_for_bid.task_id.clone(),
                                    peer_id: local_peer_id.clone(),
                                    bid: 1.0,
                                };
                                bridge
                                    .publish(crate::event_bridge::PublishEvents::NetworkMessage(
                                        NetworkMessage {
                                            message_type: BID.to_string(),
                                            payload: serde_json::to_vec(&bid)?,
                                        },
                                    ))
                                    .await?;

                                Ok::<_, anyhow::Error>(())
                            }
                            BID => {
                                // We want to ensure there is a chance that non-owning nodes can bid on tasks, as such
                                // we will wait for a period of time before claiming the task for ourselves

                                // Deserialize the bid
                                let bid: Bid = serde_json::from_slice(&message.payload)?;

                                // Check if we have knowledge of the task
                                task_bids
                                    .write()
                                    .await
                                    .entry(bid.task_id.clone())
                                    .or_insert_with(Vec::new)
                                    .push(bid);

                                Ok(())
                            }
                            BID_ACCEPTANCE => {
                                let bid_acceptance: BidAcceptance =
                                    serde_json::from_slice(&message.payload)?;

                                // Only act on the event if we are the accepted node
                                if bid_acceptance.peer_id != local_peer_id {
                                    debug!(logger, "received bid acceptance for another node"; "peer_id" => bid_acceptance.peer_id);
                                    return Ok(());
                                }

                                // Get the task from the remote tasks
                                let task = distributed_kv_store
                                    .get(format!("task/{}", bid_acceptance.task_id))
                                    .await?;

                                // TODO: emit error if task is not found and we were assigned
                                if task.is_none() {
                                    error!(logger, "task not found for bid acceptance"; "task_id" => bid_acceptance.task_id);
                                    return Ok(());
                                }
                                let task: TTask = serde_json::from_slice(&task.unwrap())?;

                                // Execute the task and store the output in the distributed kv store
                                let output = task.run(runnable_state.clone()).await?;
                                info!(logger, "task complete"; "task_id" => task.id());

                                // Store the output in the distributed kv store
                                let serialized_output = serde_json::to_vec(&output)?;
                                distributed_kv_store
                                    .set(format!("taskOutput/{}", task.id()), serialized_output)
                                    .await?;

                                // Handle output
                                bridge
                                    .publish(crate::event_bridge::PublishEvents::NetworkMessage(
                                        NetworkMessage {
                                            message_type: TASK_COMPLETE.to_string(),
                                            payload: serde_json::to_vec(&TaskComplete {
                                                task_id: task.id(),
                                            })?,
                                        },
                                    ))
                                    .await?;

                                Ok(())
                            }
                            TASK_COMPLETE => {
                                // Deserialize the task complete event
                                let task_complete: TaskComplete =
                                    serde_json::from_slice(&message.payload)?;

                                // Lookup the task from the owned tasks map
                                let task = {
                                    let mut owned_tasks = owned_tasks.write().await;
                                    owned_tasks.remove(&task_complete.task_id)
                                };
                                if task.is_none() {
                                    debug!(logger, "task not found for task complete"; "task_id" => task_complete.task_id);
                                    return Ok(());
                                };
                                let (task, tx) = task.unwrap();

                                // Get the output from the distributed kv store
                                for _ in 0..5 {
                                    if let Some(output) = distributed_kv_store
                                        .get(format!("taskOutput/{}", task.id()))
                                        .await?
                                    {
                                        let output =
                                            serde_json::from_slice::<TTaskOutput>(&output)?;
                                        tx.send(output).await?;
                                        break;
                                    } else {
                                        tokio::time::sleep(std::time::Duration::from_millis(100))
                                            .await;
                                    }
                                }
                                Ok(())
                            }
                            TASK_ERROR => {
                                let task_error =
                                    serde_json::from_slice::<TaskError>(&message.payload)?;
                                if owned_tasks.read().await.get(&task_error.task_id).is_none() {
                                    debug!(logger, "received task error for unknown task"; "task_id" => task_error.task_id);
                                    return Ok(());
                                }
                                Ok(())
                            }
                            _ => {
                                debug!(logger, "received unknown message type"; "message_type" => message_type);
                                Ok(())
                            }
                        }
                    })
                }
                Err(e) => {
                    error!(self.logger, "failed to receive handler event"; "error" => e.to_string());
                    return Ok(());
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;
    use crate::{
        event_bridge::EventBridge,
        network::{Network, P2PNetwork, P2PNetworkConfig},
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestTask {
        id: TaskId,
    }

    impl TestTask {
        fn new() -> Self {
            Self {
                id: "test".to_string(),
            }
        }
    }

    impl Runnable<String, ()> for TestTask {
        fn id(&self) -> TaskId {
            self.id.clone()
        }

        fn run(&self, _: ()) -> impl std::future::Future<Output = Result<String>> + Send {
            async move { Ok(self.id.clone()) }
        }
    }

    impl Lifecycle<()> for TestTask {}

    #[tokio::test]
    async fn test_distributed_task_queue() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());

        // Setup bridge
        let event_bridge = EventBridge::new(&logger);
        {
            let event_bridge = event_bridge.clone();
            tokio::spawn(async move {
                event_bridge.start().await.unwrap();
                panic!("event bridge stopped");
            });
        }

        // Setup network
        let network = P2PNetwork::new(P2PNetworkConfig {
            bootstrap_addrs: vec![],
            listen_addrs: vec![],
            ping_interval: std::time::Duration::from_secs(1),
            gossipsub_heartbeat_interval: std::time::Duration::from_secs(1),
            bridge: event_bridge.clone(),
            logger: logger.clone(),
            psk: None,
        });
        {
            let network = network.clone();
            tokio::spawn(async move {
                network.start().await.unwrap();
                panic!("network stopped");
            });
        }

        // Setup distributed kv store
        let distributed_kv_store =
            DistributedKVStore::new(&logger, &event_bridge, ":memory:").await;

        // Setup the task queue
        let runnable_state = ();
        let task_queue = DistributedTaskQueue::new(
            &logger,
            &network.peer_id(),
            &runnable_state,
            &event_bridge,
            &distributed_kv_store,
        );
        {
            let task_queue = task_queue.clone();
            tokio::spawn(async move {
                task_queue.start().await.unwrap();
                panic!("task queue stopped");
            });
        }

        // TODO: obviously we don't want this, but for now this will do (in before I see this one year from now...)
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let task = TestTask::new();
        let output = task_queue.execute_task(task).await.unwrap();

        assert_eq!(output, "test".to_string());
    }
}
