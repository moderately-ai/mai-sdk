mod ollama;
mod state;
mod task;

pub use ollama::{OllamaChatCompletionRequest, OllamaChatCompletionResponse};
pub use state::TextGenerationPluginState;
pub use task::{TextGenerationPluginTask, TextGenerationPluginTaskOutput};

/// ChatRequestMessage
/// Is a dto that represents a chat request message agnostic of model or platform
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatRequestMessage {
    content: String,
    role: String,
}

impl ChatRequestMessage {
    /// Create a new ChatRequestMessage
    /// role should be one of the following:
    /// - "user"
    /// - "assistant"
    /// - "system"
    pub fn new(content: String, role: String) -> Self {
        if role != "user" && role != "assistant" && role != "system" {
            panic!("role should be one of the following: 'user', 'assistant', 'system'");
        }
        Self { content, role }
    }
}
