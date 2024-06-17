#[cfg(feature = "text_generation")]
pub mod text_generation;

#[cfg(feature = "transcription")]
pub mod transcription;

#[cfg(feature = "web_scraping")]
pub mod web_scraping;

#[cfg(feature = "voice_generation")]
pub mod voice_generation;

#[cfg(feature = "file_storage")]
pub mod file_storage;
