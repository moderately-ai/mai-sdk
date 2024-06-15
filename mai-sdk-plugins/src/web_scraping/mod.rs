mod headless_chrome;
mod reqwest;
mod state;
mod task;

pub use state::WebScrapingPluginState;
pub use task::{WebScrapingPluginTaskScrape, WebScrapingPluginTaskScrapeOutput};
