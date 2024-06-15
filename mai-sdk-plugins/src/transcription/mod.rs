mod state;
mod task;
mod whisper_rs;

pub use state::TranscriptionPluginState;
pub use task::{
    TranscriptionPluginTaskTranscribe, TranscriptionPluginTaskTranscribeOutput,
    TranscriptionPluginTaskTranscribeOutputSegment,
};
