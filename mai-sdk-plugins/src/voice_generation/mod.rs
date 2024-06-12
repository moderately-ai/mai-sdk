use anyhow::Result;
use mai_sdk_core::task_queue::{Runnable, TaskId};
use slog::Logger;

pub struct VoiceGenerationPluginState {
    pub logger: Logger,
}

impl VoiceGenerationPluginState {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}

pub enum VoicePresets {
    Default,
}

pub struct VoiceGenerationPluginTask {
    pub id: TaskId,
    pub text: String,
    pub voice: VoicePresets,
}

impl VoiceGenerationPluginTask {
    pub fn new(text: String, voice: VoicePresets) -> Self {
        Self {
            id: TaskId::new(),
            text,
            voice,
        }
    }
}

pub struct VoiceGenerationPluginTaskOutput {
    pub audio: Vec<u8>,
}

impl Runnable<VoiceGenerationPluginTaskOutput, VoiceGenerationPluginState>
    for VoiceGenerationPluginTask
{
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    async fn run(
        &self,
        _state: VoiceGenerationPluginState,
    ) -> Result<VoiceGenerationPluginTaskOutput> {
        // TODO: i'd like to implement this with the bark model, will need to port to candle,
        Ok(VoiceGenerationPluginTaskOutput { audio: vec![] })
    }
}
