use headless_chrome::LaunchOptions;

pub async fn get_website_content(url: &str, headless: bool) -> anyhow::Result<String> {
    let browser = headless_chrome::Browser::new(LaunchOptions {
        headless,
        ..Default::default()
    })
    .unwrap();
    let tab = browser.new_tab()?;
    tab.navigate_to(url)?;
    tab.wait_until_navigated()?;
    tab.get_content()
}
