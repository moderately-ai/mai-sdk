use mai_sdk_core::task_queue::{Lifecycle, Runnable, TaskId};
use mai_sdk_plugins::{
    text_generation::{TextGenerationPluginTask, TextGenerationPluginTaskOutput},
    transcription::{TranscriptionPluginTaskTranscribe, TranscriptionPluginTaskTranscribeOutput},
    web_scraping::{WebScrapingPluginTaskScrape, WebScrapingPluginTaskScrapeOutput},
};
use slog::info;

use crate::RunnableState;

/// Collection of the variants of tasks that can be executed
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Task {
    TextGeneration(TextGenerationPluginTask),
    Transcribe(TranscriptionPluginTaskTranscribe),
    Scrape(WebScrapingPluginTaskScrape),
}

/// Collection of the variants of task outputs
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum TaskOutput {
    TextGeneration(TextGenerationPluginTaskOutput),
    Transcription(TranscriptionPluginTaskTranscribeOutput),
    Scrape(WebScrapingPluginTaskScrapeOutput),
}

impl Runnable<TaskOutput, RunnableState> for Task {
    fn id(&self) -> TaskId {
        match self {
            Task::TextGeneration(task) => task.id(),
            Task::Transcribe(task) => task.id(),
            Task::Scrape(task) => task.id(),
        }
    }

    async fn run(&self, state: RunnableState) -> anyhow::Result<TaskOutput> {
        info!(state.logger, "running task"; "task_id" => format!("{:?}", self.id()));
        match self {
            Task::TextGeneration(ollama_task) => Ok(TaskOutput::TextGeneration(
                ollama_task.run(state.into()).await?,
            )),
            Task::Transcribe(transcription_task) => transcription_task
                .run(state.into())
                .await
                .map(TaskOutput::Transcription),
            Task::Scrape(web_scraping_task) => Ok(TaskOutput::Scrape(
                web_scraping_task.run(state.into()).await?,
            )),
        }
    }
}

impl Lifecycle<crate::state::RunnableState> for Task {
    async fn pre_submit(&self, state: &crate::state::RunnableState) -> anyhow::Result<()> {
        let state = state.clone();
        match self {
            Task::TextGeneration(ollama_task) => ollama_task.pre_submit(&state.into()).await,
            Task::Transcribe(transcription_task) => {
                transcription_task.pre_submit(&state.into()).await
            }
            Task::Scrape(web_scraping_task) => web_scraping_task.pre_submit(&state.into()).await,
        }
    }
}
