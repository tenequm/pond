//! The e5 embedding backend: `intfloat/multilingual-e5-base` (XLM-RoBERTa) run
//! through `candle-transformers` on the Metal GPU on macOS (spec.md#search).
//! One message produces one vector - there is no chunking.

use anyhow::{Context, Result, anyhow};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::xlm_roberta::{Config, XLMRobertaModel};
use tokenizers::Tokenizer;

use super::{EmbedBackend, MODEL_ID};

/// e5's training context. The tokenizer truncates input past it before
/// inference - one message, one vector, bounded embed cost.
const MAX_TOKENS: usize = 512;

/// The e5 backend: XLM-RoBERTa weights on the GPU (Metal on macOS, CUDA on a
/// `cuda`-feature non-macOS build, CPU otherwise). `forward` is `&self`, so -
/// unlike the previous ONNX backend - no interior mutability is needed.
pub struct E5Embedder {
    model: XLMRobertaModel,
    tokenizer: Tokenizer,
    device: Device,
}

impl E5Embedder {
    /// Load `intfloat/multilingual-e5-base` from HuggingFace (cached after the
    /// first download) onto the best available device.
    pub fn load() -> Result<Self> {
        let device = select_device();
        let api = hf_hub::api::sync::Api::new().context("init HuggingFace hub client")?;
        let repo = api.model(MODEL_ID.to_owned());
        let fetch = |file: &str| {
            repo.get(file)
                .with_context(|| format!("fetch {file} for {MODEL_ID}"))
        };

        let config: Config =
            serde_json::from_str(&std::fs::read_to_string(fetch("config.json")?)?)?;
        // `candle_core::safetensors::load` encapsulates the mmap behind a safe
        // API - pond forbids `unsafe`, so this is the entry point rather than
        // `VarBuilder::from_mmaped_safetensors`.
        let tensors = candle_core::safetensors::load(fetch("model.safetensors")?, &device)?;
        let vb = VarBuilder::from_tensors(tensors, DType::F32, &device);
        let model = XLMRobertaModel::new(&config, vb)
            .map_err(|error| anyhow!("load {MODEL_ID} weights: {error}"))?;

        let mut tokenizer = Tokenizer::from_file(fetch("tokenizer.json")?)
            .map_err(|error| anyhow!("load e5 tokenizer: {error}"))?;
        // Pad each batch to its longest member (the model masks the padding) and
        // truncate at e5's context window.
        tokenizer.with_padding(Some(tokenizers::PaddingParams {
            strategy: tokenizers::PaddingStrategy::BatchLongest,
            pad_id: config.pad_token_id,
            ..Default::default()
        }));
        tokenizer
            .with_truncation(Some(tokenizers::TruncationParams {
                max_length: MAX_TOKENS,
                ..Default::default()
            }))
            .map_err(|error| anyhow!("configure e5 tokenizer: {error}"))?;

        tracing::info!(model = %MODEL_ID, device = device_label(&device), "loaded embedding model");
        Ok(Self {
            model,
            tokenizer,
            device,
        })
    }

    /// The device the weights are on - `"metal"`, `"cuda"`, or `"cpu"`.
    pub fn device(&self) -> &'static str {
        device_label(&self.device)
    }
}

impl EmbedBackend for E5Embedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|error| anyhow!("tokenize embedding batch: {error}"))?;
        let mut ids = Vec::with_capacity(encodings.len());
        let mut masks = Vec::with_capacity(encodings.len());
        for encoding in &encodings {
            ids.push(Tensor::new(encoding.get_ids(), &self.device)?);
            masks.push(Tensor::new(encoding.get_attention_mask(), &self.device)?);
        }
        let input_ids = Tensor::stack(&ids, 0)?;
        let attention_mask = Tensor::stack(&masks, 0)?;
        let token_type_ids = input_ids.zeros_like()?;
        // XLM-RoBERTa last hidden state: [batch, seq, hidden].
        let hidden = self.model.forward(
            &input_ids,
            &attention_mask,
            &token_type_ids,
            None,
            None,
            None,
        )?;
        // Masked mean-pool, then L2-normalize - the sentence-transformers
        // pooling e5 was trained with (padding tokens excluded by the mask).
        let mask = attention_mask.to_dtype(DType::F32)?.unsqueeze(2)?;
        let summed = hidden.broadcast_mul(&mask)?.sum(1)?;
        let counts = mask.sum(1)?;
        let mean = summed.broadcast_div(&counts)?;
        let norm = mean.sqr()?.sum_keepdim(1)?.sqrt()?;
        mean.broadcast_div(&norm)?
            .to_vec2::<f32>()
            .map_err(|error| anyhow!("read embedding vectors: {error}"))
    }
}

/// Pick the embedding device: Metal on macOS, CUDA on a `cuda`-feature non-mac
/// build, CPU otherwise. candle's `*_if_available` returns `Cpu` when the
/// backend feature is not compiled in; a runtime `Err` (no GPU or driver) also
/// falls back to `Cpu`.
fn select_device() -> Device {
    #[cfg(target_os = "macos")]
    let device = Device::metal_if_available(0);
    #[cfg(not(target_os = "macos"))]
    let device = Device::cuda_if_available(0);
    device.unwrap_or_else(|error| {
        tracing::warn!(%error, "GPU device unavailable, falling back to CPU");
        Device::Cpu
    })
}

fn device_label(device: &Device) -> &'static str {
    match device {
        Device::Cpu => "cpu",
        Device::Cuda(_) => "cuda",
        Device::Metal(_) => "metal",
    }
}
