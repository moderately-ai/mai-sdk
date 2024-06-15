pub async fn get_website_content(url: &str) -> anyhow::Result<String> {
    reqwest::get(url).await?.text().await.map_err(Into::into)
}
