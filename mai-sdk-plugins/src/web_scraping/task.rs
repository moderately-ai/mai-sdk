use anyhow::Result;
use mai_sdk_core::task_queue::{Lifecycle, Runnable, TaskId};
use serde::{Deserialize, Serialize};
use slog::info;

use super::state::WebScrapingPluginState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebScrapingPluginTaskScrape {
    pub id: TaskId,
    pub url: String,
    pub enable_js: bool,
    pub headless: bool,
}

impl WebScrapingPluginTaskScrape {
    pub fn new(url: String, enable_js: bool, headless: bool) -> Self {
        Self {
            id: nanoid::nanoid!(),
            url,
            enable_js,
            headless,
        }
    }
}

impl Lifecycle<WebScrapingPluginState> for WebScrapingPluginTaskScrape {}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct WebScrapingPluginTaskScrapeOutput {
    pub content: String,
}

impl Runnable<WebScrapingPluginTaskScrapeOutput, WebScrapingPluginState>
    for WebScrapingPluginTaskScrape
{
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    async fn run(
        &self,
        state: WebScrapingPluginState,
    ) -> Result<WebScrapingPluginTaskScrapeOutput> {
        let content = match self.enable_js {
            true => {
                info!(state.logger, "running with browser");
                super::headless_chrome::get_website_content(&self.url, self.headless).await?
            }
            false => {
                info!(state.logger, "running without browser");
                super::reqwest::get_website_content(&self.url).await?
            }
        };

        Ok(WebScrapingPluginTaskScrapeOutput { content })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_web_scraping_plugin_task_scrape() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let state = WebScrapingPluginState::new(&logger);

        let task = WebScrapingPluginTaskScrape {
            id: "test".into(),
            url: "https://www.google.com".to_string(),
            enable_js: false,
            headless: true,
        };
        let output = task.run(state).await.unwrap();
        assert!(!output.content.is_empty());
    }

    #[tokio::test]
    async fn test_web_scraping_plugin_task_scrape_with_js() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let state = WebScrapingPluginState::new(&logger);

        let task = WebScrapingPluginTaskScrape {
            id: "test".into(),
            url: "https://www.google.com".to_string(),
            enable_js: true,
            headless: true,
        };
        let output = task.run(state).await.unwrap();
        assert!(!output.content.is_empty());
    }
}
