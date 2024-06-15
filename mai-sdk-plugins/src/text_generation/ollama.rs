use super::ChatRequestMessage;

/// See https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-chat-completion for more information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OllamaChatCompletionRequest {
    model: String,
    messages: Vec<ChatRequestMessage>,
    stream: bool,
}

impl OllamaChatCompletionRequest {
    pub fn new(model: String, messages: Vec<ChatRequestMessage>) -> Self {
        Self {
            model,
            messages,
            stream: false,
        }
    }
}

/// See https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-chat-completion for more information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OllamaChatCompletionResponse {
    model: String,
    created_at: String,
    message: OllamaChatCompletionResponseMessage,
    done: bool,
}

impl OllamaChatCompletionResponse {
    pub fn new(
        model: String,
        created_at: String,
        content: String,
        role: String,
        done: bool,
    ) -> Self {
        Self {
            model,
            created_at,
            message: OllamaChatCompletionResponseMessage { content, role },
            done,
        }
    }

    pub fn content(&self) -> String {
        self.message.content.clone()
    }

    pub fn role(&self) -> String {
        self.message.role.clone()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OllamaChatCompletionResponseMessage {
    content: String,
    role: String,
}
