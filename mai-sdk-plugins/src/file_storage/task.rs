use mai_sdk_core::task_queue::Runnable;

use super::state::FileStorageState;
use anyhow::Result;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileStoragePluginTaskGet {
    pub task_id: String,
    pub key: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileStoragePluginTaskPut {
    pub task_id: String,
    pub key: String,
    pub path: String,
}

/// FileStoragePluginTask
/// Leverages the distributed kv store to make files available across the network
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum FileStoragePluginTask {
    Get(FileStoragePluginTaskGet),
    Put(FileStoragePluginTaskPut),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileStoragePluginTaskOutput {
    Get(Option<Vec<u8>>),
    Put(),
}

impl Runnable<FileStoragePluginTaskOutput, FileStorageState> for FileStoragePluginTask {
    fn id(&self) -> mai_sdk_core::task_queue::TaskId {
        match self {
            FileStoragePluginTask::Get(task) => task.task_id.clone(),
            FileStoragePluginTask::Put(task) => task.task_id.clone(),
        }
    }

    async fn run(&self, state: FileStorageState) -> Result<FileStoragePluginTaskOutput> {
        match self {
            FileStoragePluginTask::Get(task) => {
                let key = task.key.clone();
                let value = state.distributed_kv_store.get(key).await?;
                Ok(FileStoragePluginTaskOutput::Get(value))
            }
            FileStoragePluginTask::Put(task) => {
                let key = task.key.clone();
                let path = task.path.clone();
                let file_buf = tokio::fs::read(path).await?;
                state.distributed_kv_store.set(key, file_buf).await?;
                Ok(FileStoragePluginTaskOutput::Put())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_storage_plugin_task_get() {
        let project_root = env!("CARGO_MANIFEST_DIR");
        println!("Project root: {}", project_root);
        let path = format!("{}/src/file_storage/test_data/sample.txt", project_root);

        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let bridge = mai_sdk_core::event_bridge::EventBridge::new(&logger);

        let distributed_kv_store =
            mai_sdk_core::distributed_kv_store::DistributedKVStore::new(&logger, &bridge, false)
                .await;

        let state = FileStorageState {
            distributed_kv_store,
        };

        // Set the data via task
        let task = FileStoragePluginTask::Put(FileStoragePluginTaskPut {
            task_id: "test".to_string(),
            key: "test".to_string(),
            path: path.to_string(),
        });
        task.run(state.clone()).await.unwrap();

        // Get the data via task
        let task = FileStoragePluginTask::Get(FileStoragePluginTaskGet {
            task_id: "test".to_string(),
            key: "test".to_string(),
        });
        let output = task.run(state).await.unwrap();

        let expected_output = std::fs::read(path).unwrap();
        assert_eq!(
            output,
            FileStoragePluginTaskOutput::Get(Some(expected_output))
        );
    }
}
