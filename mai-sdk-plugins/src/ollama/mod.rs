use mai_sdk_core::task_queue::{Runnable, TaskId};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use slog::{info, Logger};

#[derive(Debug, Clone)]
pub struct OllamaPluginState {
    logger: Logger,
}

impl OllamaPluginState {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OllamaPluginTaskGenerate {
    id: TaskId,

    #[serde(alias = "prompt")]
    prompt: String,

    #[serde(alias = "model")]
    model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaApiRequestGenerate {
    prompt: String,
    model: String,
    stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaApiResponseGenerate {
    response: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OllamaPluginTaskChatCompletion {
    id: TaskId,
    model: String,
    messages: Vec<ChatRequestMessage>,
}

impl OllamaPluginTaskChatCompletion {
    pub fn new(messages: Vec<ChatRequestMessage>, model: String) -> Self {
        Self {
            id: nanoid!(),
            model,
            messages,
        }
    }
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
pub struct OllamaPluginTaskOutputChatCompletion {
    role: String,
    content: String,
}

impl OllamaPluginTaskOutputChatCompletion {
    pub fn new(role: String, content: String) -> Self {
        Self { role, content }
    }

    pub fn role(&self) -> String {
        self.role.clone()
    }

    pub fn content(&self) -> String {
        self.content.clone()
    }
}

impl OllamaPluginTaskGenerate {
    pub fn new(prompt: String, model: String) -> Self {
        Self {
            id: nanoid!(),
            prompt,
            model,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id.clone()
    }

    pub fn prompt(&self) -> String {
        self.prompt.clone()
    }

    pub fn model(&self) -> String {
        self.model.clone()
    }
}

/// OllamaPluginTask
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OllamaPluginTask {
    Generate(OllamaPluginTaskGenerate),
    ChatCompletion(OllamaPluginTaskChatCompletion),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OllamaPluginTaskOutputGenerate {
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OllamaPluginTaskOutput {
    Generate(OllamaPluginTaskOutputGenerate),
    ChatCompletion(OllamaPluginTaskOutputChatCompletion),
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

impl Runnable<OllamaPluginTaskOutput, OllamaPluginState> for OllamaPluginTask {
    fn id(&self) -> TaskId {
        match self {
            OllamaPluginTask::Generate(task) => task.id.clone(),
            OllamaPluginTask::ChatCompletion(task) => task.id.clone(),
        }
    }

    async fn run(&self, state: OllamaPluginState) -> anyhow::Result<OllamaPluginTaskOutput> {
        match self {
            OllamaPluginTask::Generate(task) => {
                info!(state.logger, "Running OllamaPluginTask::Generate"; "task" => format!("{:?}", task));

                // Send request
                let body = OllamaApiRequestGenerate {
                    prompt: task.prompt.clone(),
                    model: task.model.clone(),
                    stream: false,
                };
                let client = reqwest::Client::new();
                let resp = client
                    .post("http://localhost:11434/api/generate")
                    .json(&body)
                    .send()
                    .await?;

                // Prase response
                let output = OllamaPluginTaskOutputGenerate {
                    output: resp.json::<OllamaApiResponseGenerate>().await?.response,
                };
                info!(state.logger, "OllamaPluginTask::Generate completed"; "output" => format!("{:?}", output));

                // Return result
                Ok(OllamaPluginTaskOutput::Generate(output))
            }
            OllamaPluginTask::ChatCompletion(task) => {
                info!(state.logger, "Running OllamaPluginTask::ChatCompletion"; "task" => format!("{:?}", task));

                // Send request
                let body = OllamaChatCompletionRequest {
                    model: task.model.clone(),
                    messages: task.messages.clone(),
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
                let output = OllamaPluginTaskOutputChatCompletion {
                    content: body.content,
                    role: body.role,
                };
                info!(state.logger, "OllamaPluginTask::ChatCompletion completed"; "output" => format!("{:?}", output));

                // Return result
                Ok(OllamaPluginTaskOutput::ChatCompletion(output))
            }
        }
    }
}
