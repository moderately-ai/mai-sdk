use mai_sdk_core::task_queue::{Runnable, TaskId};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};

#[derive(Debug, Clone)]
pub struct TextGenerationPluginState {
    logger: Logger,
}

impl TextGenerationPluginState {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextGenerationPluginTask {
    id: TaskId,
    model: String,
    messages: Vec<ChatRequestMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequestMessage {
    content: String,
    role: String,
}

impl ChatRequestMessage {
    pub fn new(content: String, role: String) -> Self {
        Self { content, role }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextGenerationPluginTaskOutput {
    role: String,
    content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaChatCompletionRequest {
    model: String,
    messages: Vec<ChatRequestMessage>,
    stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaChatCompletionResponse {
    model: String,
    created_at: String,
    message: OllamaChatCompletionResponseMessage,
    done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaChatCompletionResponseMessage {
    content: String,
    role: String,
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
        let body = OllamaChatCompletionRequest {
            model: self.model.clone(),
            messages: self.messages.clone(),
            stream: false,
        };
        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:11434/api/chat")
            .json(&body)
            .send()
            .await?;

        // Parse response
        let body = resp.json::<OllamaChatCompletionResponse>().await?.message;
        let output = TextGenerationPluginTaskOutput {
            content: body.content,
            role: body.role,
        };
        info!(state.logger, "OllamaPluginTask::ChatCompletion completed"; "output" => format!("{:?}", output));

        // Return result
        Ok(output)
    }
}
