use anyhow::Result;
use hound;
use mai_sdk_core::task_queue::{Runnable, TaskId};
use serde::{Deserialize, Serialize};
use slog::{info, Logger};
use std::path::PathBuf;
use whisper_rs::{FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters};

#[derive(Debug, Clone)]
pub struct TranscriptionPluginState {
    pub logger: Logger,
}

impl TranscriptionPluginState {
    pub fn new(logger: &Logger) -> Self {
        Self {
            logger: logger.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptionPluginTaskTranscribe {
    id: TaskId,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TranscriptionPluginTaskTranscribeOutputSegment {
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TranscriptionPluginTaskTranscribeOutput {
    pub segments: Vec<TranscriptionPluginTaskTranscribeOutputSegment>,
}

impl TranscriptionPluginTaskTranscribe {
    pub async fn load_model(&self) -> Result<PathBuf> {
        let api = hf_hub::api::tokio::Api::new()?;
        // TODO: parameterize model path
        let model_path = api
            .model("ggerganov/whisper.cpp".to_string())
            .get("ggml-base.en.bin")
            .await?;
        Ok(model_path)
    }
}

impl Runnable<TranscriptionPluginTaskTranscribeOutput, TranscriptionPluginState>
    for TranscriptionPluginTaskTranscribe
{
    fn id(&self) -> TaskId {
        self.id.clone()
    }

    async fn run(
        &self,
        state: TranscriptionPluginState,
    ) -> Result<TranscriptionPluginTaskTranscribeOutput> {
        let model_path = self.load_model().await?;
        info!(state.logger, "Model loaded"; "path" => model_path.to_str().unwrap());

        let mut context_param = WhisperContextParameters::default();

        // Enable DTW token level timestamp for known model by using model preset
        context_param.dtw_parameters.mode = whisper_rs::DtwMode::ModelPreset {
            model_preset: whisper_rs::DtwModelPreset::BaseEn,
        };

        let custom_aheads = [
            (3, 1),
            (4, 2),
            (4, 3),
            (4, 7),
            (5, 1),
            (5, 2),
            (5, 4),
            (5, 6),
        ]
        .map(|(n_text_layer, n_head)| whisper_rs::DtwAhead {
            n_text_layer,
            n_head,
        });
        context_param.dtw_parameters.mode = whisper_rs::DtwMode::Custom {
            aheads: &custom_aheads,
        };

        let ctx = WhisperContext::new_with_params(model_path.to_str().unwrap(), context_param)?;

        let mut state = ctx.create_state()?;

        // Create a params object for running the model.
        // The number of past samples to consider defaults to 0.
        let mut params = FullParams::new(SamplingStrategy::Greedy { best_of: 0 });

        // Edit params as needed.
        // Set the number of threads to use to 1.
        params.set_n_threads(1);
        // Enable translation.
        params.set_translate(true);
        // Set the language to translate to to English.
        params.set_language(Some("en"));
        // Disable anything that prints to stdout.
        params.set_print_special(false);
        params.set_print_progress(false);
        params.set_print_realtime(false);
        params.set_print_timestamps(false);
        // Enable token level timestamps
        params.set_token_timestamps(true);

        // Open the audio file.
        let reader = hound::WavReader::new(self.data.as_slice())?;

        #[allow(unused_variables)]
        let hound::WavSpec {
            channels,
            sample_rate,
            bits_per_sample,
            ..
        } = reader.spec();

        // Convert the audio to floating point samples.
        let samples: Vec<i16> = reader
            .into_samples::<i16>()
            .map(|x| x.expect("Invalid sample"))
            .collect();
        let mut audio = vec![0.0f32; samples.len()];
        whisper_rs::convert_integer_to_float_audio(&samples, &mut audio).expect("Conversion error");

        // Convert audio to 16KHz mono f32 samples, as required by the model.
        // These utilities are provided for convenience, but can be replaced with custom conversion logic.
        // SIMD variants of these functions are also available on nightly Rust (see the docs).
        if channels == 2 {
            audio = whisper_rs::convert_stereo_to_mono_audio(&audio).expect("Conversion error");
        } else if channels != 1 {
            panic!(">2 channels unsupported");
        }

        if sample_rate != 16000 {
            panic!("sample rate must be 16KHz");
        }

        // Run the model.
        state.full(params, &audio[..]).expect("failed to run model");

        // Iterate through the segments of the transcript.
        let num_segments = state
            .full_n_segments()
            .expect("failed to get number of segments");

        // Setup the output vector
        let mut segments = vec![];

        for i in 0..num_segments {
            // Get the transcribed text and timestamps for the current segment.
            let segment = state
                .full_get_segment_text(i)
                .expect("failed to get segment");
            let start_timestamp = state
                .full_get_segment_t0(i)
                .expect("failed to get start timestamp");
            let end_timestamp = state
                .full_get_segment_t1(i)
                .expect("failed to get end timestamp");
            // Format the segment information as a string.
            segments.push(TranscriptionPluginTaskTranscribeOutputSegment {
                start_timestamp,
                end_timestamp,
                text: segment,
            });
        }

        Ok(TranscriptionPluginTaskTranscribeOutput { segments })
    }
}

#[cfg(test)]
mod tests {
    use slog::o;

    use super::*;

    #[tokio::test]
    async fn test_transcription_plugin_task_transcribe() -> Result<()> {
        let data = include_bytes!("./test_data/jfk.wav").to_vec();
        let task = TranscriptionPluginTaskTranscribe {
            id: TaskId::new(),
            data,
        };
        let state = TranscriptionPluginState {
            logger: slog::Logger::root(slog::Discard, o!()),
        };
        let output = task.run(state).await?;
        assert_eq!(output, TranscriptionPluginTaskTranscribeOutput {
            segments: vec![TranscriptionPluginTaskTranscribeOutputSegment { start_timestamp: 0, end_timestamp: 1100, text: " And so my fellow Americans, ask not what your country can do for you, ask what you can do for your country.".to_string() }],
        });

        Ok(())
    }
}
