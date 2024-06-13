use anyhow::Result;
use headless_chrome::LaunchOptions;
use mai_sdk_core::task_queue::{Runnable, TaskId};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebScrapingPluginTaskScrape {
    pub id: TaskId,
    pub url: String,
    pub enable_js: bool,
}

impl WebScrapingPluginTaskScrape {
    pub fn new(url: String, enable_js: bool) -> Self {
        Self {
            id: nanoid::nanoid!(),
            url,
            enable_js,
        }
    }
}

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
        let content = if self.enable_js {
            info!(state.logger, "fetching website content with javascript"; "url" => &self.url);
            let browser = headless_chrome::Browser::new(LaunchOptions {
                headless: false,
                ..Default::default()
            })
            .unwrap();
            let tab = browser.new_tab()?;
            tab.navigate_to(&self.url)?;
            tab.wait_until_navigated()?;
            tab.get_content()?
        } else {
            info!(state.logger, "fetching website content without javascript"; "url" => &self.url);
            reqwest::get(&self.url).await?.text().await?
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
            id: TaskId::new(),
            url: "https://www.google.com".to_string(),
            enable_js: false,
        };
        let output = task.run(state).await.unwrap();
        assert!(!output.content.is_empty());
    }

    #[tokio::test]
    async fn test_web_scraping_plugin_task_scrape_with_js() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let state = WebScrapingPluginState::new(&logger);

        let task = WebScrapingPluginTaskScrape {
            id: TaskId::new(),
            url: "https://www.google.com".to_string(),
            enable_js: true,
        };
        let output = task.run(state).await.unwrap();
        assert!(!output.content.is_empty());
    }
}
