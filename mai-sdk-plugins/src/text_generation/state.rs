use slog::Logger;

#[derive(Debug, Clone)]
pub struct TextGenerationPluginState {
    pub logger: Logger,
}

impl TextGenerationPluginState {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}
