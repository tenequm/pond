//! The e5 embedding backend: `intfloat/multilingual-e5-base` (XLM-RoBERTa) run
//! through `candle-transformers` on the Metal GPU on macOS (spec.md#search).
//! One message produces one vector - there is no chunking.

use anyhow::{Context, Result, anyhow};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::xlm_roberta::{Config, XLMRobertaModel};
use tokenizers::Tokenizer;

use super::{EmbedBackend, model_id};
use crate::sessions::embedding_dim;

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
    /// Load the configured XLM-RoBERTa model from HuggingFace (cached after
    /// the first download) onto the best available device. The model id comes
    /// from [`model_id`]; the configured `[embeddings].dim` must match the
    /// model's `hidden_size`, otherwise writes would fail at the schema
    /// boundary anyway.
    pub fn load() -> Result<Self> {
        let device = select_device();
        let id = model_id();
        let api = hf_hub::api::sync::Api::new().context("init HuggingFace hub client")?;
        let repo = api.model(id.to_owned());
        let fetch = |file: &str| {
            repo.get(file)
                .with_context(|| format!("fetch {file} for {id}"))
        };

        let config: Config =
            serde_json::from_str(&std::fs::read_to_string(fetch("config.json")?)?)?;
        if config.hidden_size != embedding_dim() {
            return Err(anyhow!(
                "[embeddings].dim = {} but model {id:?} reports hidden_size = {}; \
                 set [embeddings].dim to match the model's output width.",
                embedding_dim(),
                config.hidden_size,
            ));
        }
        // `candle_core::safetensors::load` encapsulates the mmap behind a safe
        // API - pond forbids `unsafe`, so this is the entry point rather than
        // `VarBuilder::from_mmaped_safetensors`.
        let tensors = candle_core::safetensors::load(fetch("model.safetensors")?, &device)?;
        // Cast weights to F16 to halve resident model RSS in `pond mcp` and
        // shrink per-query activations. Final mean-pool casts back to F32.
        let tensors = tensors
            .into_iter()
            .map(|(name, tensor)| Ok((name, tensor.to_dtype(DType::F16)?)))
            .collect::<Result<std::collections::HashMap<_, _>>>()?;
        let vb = VarBuilder::from_tensors(tensors, DType::F16, &device);
        let model = XLMRobertaModel::new(&config, vb)
            .map_err(|error| anyhow!("load {id} weights: {error}"))?;

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

        tracing::info!(model = %id, device = device_label(&device), "loaded embedding model");
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
        // XLM-RoBERTa last hidden state: [batch, seq, hidden]. F16 weights ->
        // F16 hidden; cast to F32 for the pool so the output vector keeps
        // sentence-transformers' expected precision.
        let hidden = self
            .model
            .forward(
                &input_ids,
                &attention_mask,
                &token_type_ids,
                None,
                None,
                None,
            )?
            .to_dtype(DType::F32)?;
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
