use anyhow::Result;
use mai_sdk_core::task_queue::{Lifecycle, Runnable, TaskId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::state::TranscriptionPluginState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptionPluginTaskTranscribe {
    id: TaskId,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TranscriptionPluginTaskTranscribeOutputSegment {
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TranscriptionPluginTaskTranscribeOutput {
    pub segments: Vec<TranscriptionPluginTaskTranscribeOutputSegment>,
}

impl Default for TranscriptionPluginTaskTranscribe {
    fn default() -> Self {
        Self::new()
    }
}

impl TranscriptionPluginTaskTranscribe {
    pub fn new() -> Self {
        Self {
            id: nanoid::nanoid!(),
        }
    }

    pub async fn load_model(&self) -> Result<PathBuf> {
        let api = hf_hub::api::tokio::Api::new()?;
        let model_path = api
            .model("ggerganov/whisper.cpp".to_string())
            .get("ggml-base.en.bin")
            .await?;
        Ok(model_path)
    }

    /// This task requires the audio data to be fetched from the distributed KV store.
    async fn fetch_data(&self, state: TranscriptionPluginState) -> Result<Option<Vec<u8>>> {
        if let Some(data) = state
            .distributed_kv_store
            .get(format!("taskData/{}", self.id()))
            .await?
        {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

impl Lifecycle<TranscriptionPluginState> for TranscriptionPluginTaskTranscribe {
    async fn pre_submit(&self, state: &TranscriptionPluginState) -> Result<()> {
        state
            .distributed_kv_store
            .set(format!("taskData/{}", self.id()), vec![])
            .await?;
        Ok(())
    }
}

impl Runnable<TranscriptionPluginTaskTranscribeOutput, TranscriptionPluginState>
    for TranscriptionPluginTaskTranscribe
{
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    async fn run(
        &self,
        state: TranscriptionPluginState,
    ) -> Result<TranscriptionPluginTaskTranscribeOutput> {
        let model_path = self.load_model().await?;
        let data = self
            .fetch_data(state)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to fetch data from the distributed KV store"))?;

        let segments = super::whisper_rs::transcribe(&data, model_path)?;
        Ok(TranscriptionPluginTaskTranscribeOutput { segments })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_transcription_plugin_task_transcribe() -> Result<()> {
        let data = include_bytes!("./test_data/jfk.wav").to_vec();
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let event_bridge = mai_sdk_core::event_bridge::EventBridge::new(&logger);
        let distributed_kv_store = mai_sdk_core::distributed_kv_store::DistributedKVStore::new(
            &logger,
            &event_bridge,
            ":memory:",
        )
        .await;
        let task = TranscriptionPluginTaskTranscribe::new();
        let state = TranscriptionPluginState {
            logger,
            distributed_kv_store,
        };

        // store the data
        state
            .distributed_kv_store
            .set(format!("taskData/{}", task.id()), data)
            .await?;

        let output = task.run(state).await?;
        assert_eq!(output, TranscriptionPluginTaskTranscribeOutput {
            segments: vec![TranscriptionPluginTaskTranscribeOutputSegment { start_timestamp: 0, end_timestamp: 1100, text: " And so my fellow Americans, ask not what your country can do for you, ask what you can do for your country.".to_string() }],
        });

        Ok(())
    }
}
