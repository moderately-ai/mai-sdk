use mai_sdk_core::task_queue::{Runnable, TaskId};
use serde::{Deserialize, Serialize};
use slog::info;

use crate::text_generation::ollama::{OllamaChatCompletionRequest, OllamaChatCompletionResponse};

use super::{state::TextGenerationPluginState, ChatRequestMessage};

/// TextGenerationPluginTask
/// This task implements the ability to call an LLM model to generate text
/// The only method of generating text for now is through chat completion as it is the most common use case
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextGenerationPluginTask {
    pub(crate) id: TaskId,

    /// The model to use for generating text
    /// Models should use the following format:
    /// - for hugging face hub: "username/model_name"
    /// - for local models: "path/to/model"
    /// - for ollama models: "ollama/model_name"
    pub(crate) model: String,

    /// The messages to use for generating text
    /// The consumer is responsible for ensuring that the messages fit within the model's context window
    pub(crate) messages: Vec<ChatRequestMessage>,
}

impl TextGenerationPluginTask {
    pub fn new(model: String, messages: Vec<ChatRequestMessage>) -> Self {
        Self {
            id: nanoid::nanoid!(),
            model,
            messages,
        }
    }
}

/// TextGenerationPluginTaskOutput
/// The output of the TextGenerationPluginTask, this will contain only the role and the generated text of the model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextGenerationPluginTaskOutput {
    pub role: String,
    pub content: String,
}

impl Runnable<TextGenerationPluginTaskOutput, TextGenerationPluginState>
    for TextGenerationPluginTask
{
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    async fn run(
        &self,
        state: TextGenerationPluginState,
    ) -> anyhow::Result<TextGenerationPluginTaskOutput> {
        // Send request
        let body = OllamaChatCompletionRequest::new(self.model.clone(), self.messages.clone());
        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:11434/api/chat")
            .json(&body)
            .send()
            .await?;

        // Parse response
        let response_body = resp.json::<OllamaChatCompletionResponse>().await?;
        let output = TextGenerationPluginTaskOutput {
            content: response_body.content(),
            role: response_body.role(),
        };
        info!(state.logger, "OllamaPluginTask::ChatCompletion completed"; "output" => format!("{:?}", output));

        // Return result
        Ok(output)
    }
}
