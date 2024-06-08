use anyhow::Result;

pub type TaskId = String;

/// A task that can be run and return an output
pub trait Runnable<Event, State> {
    /// a unique identifier for the task, should be unique across all tasks, nodes and plugins
    fn id(&self) -> TaskId;

    /// run the task and return the output
    /// if this method is called multiple times, it should return the same output
    fn run(&self, state: State) -> impl std::future::Future<Output = Result<Event>> + Send;
}

use std::{collections::HashMap, sync::Arc};

use async_channel::Sender;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use slog::{debug, error, info, warn, Logger};
use tokio::sync::RwLock;

use crate::{
    bridge::{EventBridge, PublishEvents},
    handler::Startable,
    network::{NetworkMessage, PeerId},
    storage::{OwnedTasks, RemoteTasks, TaskAssignments},
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
    task: Vec<u8>,
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
    peer_id: PeerId,
    output: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskProgress {
    task_id: TaskId,
    peer_id: PeerId,
    output: Vec<u8>,
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

    /// Tasks that are owned by remote nodes, these are tasks that have been submitted to the queue
    remote_tasks: RemoteTasks<TTask>,

    /// A map of task assignments, this is used to track which node is responsible for executing a task
    task_assignments: TaskAssignments,

    /// State that is passed to the runnable tasks
    runnable_state: RunnableState,

    /// Event bridge
    bridge: EventBridge,
}

impl<
        TTask: Clone + Runnable<TTaskOutput, TRunnableState> + Serialize,
        TTaskOutput: Clone,
        TRunnableState: Clone,
    > DistributedTaskQueue<TTask, TTaskOutput, TRunnableState>
{
    pub fn new(
        logger: Logger,
        local_peer_id: PeerId,
        runnable_state: TRunnableState,
        bridge: EventBridge,
    ) -> Self {
        Self {
            logger,
            local_peer_id,
            owned_tasks: Arc::new(RwLock::new(HashMap::new())),
            remote_tasks: Arc::new(RwLock::new(HashMap::new())),
            task_assignments: Arc::new(RwLock::new(HashMap::new())),
            runnable_state,
            bridge,
        }
    }

    pub async fn submit_task(&self, task: TTask, tx: Sender<TTaskOutput>) -> Result<()> {
        // Store the task in the owned_tasks map
        let mut owned_tasks = self.owned_tasks.write().await;
        owned_tasks.insert(task.id(), (task.clone(), tx.clone()));

        // Emit a request for bids event
        let request_for_bids = RequestForBid {
            task_id: task.id(),
            peer_id: self.local_peer_id.clone(),
            task: bincode::serialize(&task)?,
        };
        self.bridge
            .publish(PublishEvents::NetworkMessage(NetworkMessage {
                message_type: REQUEST_FOR_BIDS.to_string(),
                payload: bincode::serialize(&request_for_bids)?,
            }))
            .await?;

        Ok::<_, anyhow::Error>(())
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
        let handler_rx = self.bridge.subscribe_to_handler().await;
        loop {
            // Wait for the next handler event
            let event = handler_rx.recv().await;
            match event {
                Ok(event) => {
                    let message = event.message();
                    let logger = self.logger.clone();
                    let bridge = self.bridge.clone();
                    let local_peer_id = self.local_peer_id.clone();
                    let remote_tasks = self.remote_tasks.clone();
                    let owned_tasks = self.owned_tasks.clone();
                    let task_assignments = self.task_assignments.clone();
                    let runnable_state = self.runnable_state.clone();
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
                                    bincode::deserialize(&message.payload)?;
                                let task = bincode::deserialize::<TTask>(&request_for_bid.task)?;

                                // Store the task in the remote_tasks map for future retrieval
                                {
                                    let mut remote_tasks = remote_tasks.write().await;
                                    remote_tasks
                                        .insert(request_for_bid.task_id.clone(), task.clone());
                                }

                                // Respond with a bid for the task
                                let bid = Bid {
                                    task_id: request_for_bid.task_id.clone(),
                                    peer_id: local_peer_id.clone(),
                                    bid: 1.0,
                                };
                                bridge
                                    .publish(crate::bridge::PublishEvents::NetworkMessage(
                                        NetworkMessage {
                                            message_type: BID.to_string(),
                                            payload: bincode::serialize(&bid)?,
                                        },
                                    ))
                                    .await?;

                                Ok::<_, anyhow::Error>(())
                            }
                            BID => {
                                // Deserialize the bid
                                let bid: Bid = bincode::deserialize(&message.payload)?;

                                // Check if we have knowledge of the task
                                if owned_tasks.read().await.get(&bid.task_id).is_none() {
                                    debug!(logger, "received bid for unknown task"; "task_id" => bid.task_id);
                                    return Ok(());
                                }

                                // Check if we have already assigned the task
                                if task_assignments.read().await.contains_key(&bid.task_id) {
                                    debug!(logger, "received bid for already assigned task"; "task_id" => bid.task_id);
                                    return Ok(());
                                }

                                // Store the task assignment
                                {
                                    let mut task_assignments = task_assignments.write().await;
                                    task_assignments
                                        .insert(bid.task_id.clone(), bid.peer_id.clone());
                                }

                                // Respond with a bid acceptance event
                                if let Err(e) = bridge
                                    .publish(crate::bridge::PublishEvents::NetworkMessage(
                                        NetworkMessage {
                                            message_type: BID_ACCEPTANCE.to_string(),
                                            payload: bincode::serialize(&BidAcceptance {
                                                task_id: bid.task_id.clone(),
                                                peer_id: bid.peer_id.clone(),
                                            })?,
                                        },
                                    ))
                                    .await
                                {
                                    warn!(logger, "failed to emit bid acceptance event"; "error" => e.to_string());
                                    return Ok(());
                                };

                                Ok(())
                            }
                            BID_ACCEPTANCE => {
                                let bid_acceptance: BidAcceptance =
                                    bincode::deserialize(&message.payload)?;

                                // Only act on the event if we are the accepted node
                                if bid_acceptance.peer_id != local_peer_id {
                                    debug!(logger, "received bid acceptance for another node"; "peer_id" => bid_acceptance.peer_id);
                                    return Ok(());
                                }

                                // Get the task from the remote tasks
                                let task = {
                                    let mut remote_tasks = remote_tasks.write().await;
                                    remote_tasks.remove(&bid_acceptance.task_id)
                                };

                                // TODO: emit error if task is not found and we were assigned
                                if task.is_none() {
                                    error!(logger, "task not found for bid acceptance"; "task_id" => bid_acceptance.task_id);
                                    return Ok(());
                                }
                                let task = task.unwrap();

                                // Execute the task & the sender thread to intercept any progress messages
                                let output = task.run(runnable_state.clone()).await?;
                                info!(logger, "task complete"; "task_id" => task.id());

                                // Handle output
                                bridge
                                    .publish(crate::bridge::PublishEvents::NetworkMessage(
                                        NetworkMessage {
                                            message_type: TASK_COMPLETE.to_string(),
                                            payload: bincode::serialize(&TaskComplete {
                                                task_id: task.id(),
                                                peer_id: local_peer_id.clone(),
                                                output: bincode::serialize(&output)?,
                                            })?,
                                        },
                                    ))
                                    .await?;

                                Ok(())
                            }
                            TASK_COMPLETE => {
                                // Deserialize the task complete event
                                let task_complete: TaskComplete =
                                    bincode::deserialize(&message.payload)?;

                                // Lookup the task from the owned tasks map
                                let task = {
                                    let mut owned_tasks = owned_tasks.write().await;
                                    owned_tasks.remove(&task_complete.task_id)
                                };
                                if task.is_none() {
                                    debug!(logger, "task not found for task complete"; "task_id" => task_complete.task_id);
                                    return Ok(());
                                }

                                // Notify the initializer of the output
                                let (_, tx) = task.unwrap();
                                tx.send(bincode::deserialize(&task_complete.output)?)
                                    .await?;

                                // Remove the task assignment
                                {
                                    let mut task_assignments = task_assignments.write().await;
                                    task_assignments.remove(&task_complete.task_id);
                                }

                                Ok(())
                            }
                            TASK_ERROR => {
                                let task_error =
                                    bincode::deserialize::<TaskError>(&message.payload)?;
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
