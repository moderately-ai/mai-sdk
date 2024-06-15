use slog::Logger;

#[derive(Debug, Clone)]
pub struct WebScrapingPluginState {
    pub logger: Logger,
}

impl WebScrapingPluginState {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}
