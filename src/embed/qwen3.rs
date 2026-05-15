use anyhow::{Result, anyhow};

use crate::config::EmbeddingModel;

use super::EmbedBackend;

/// The Qwen3 candle backend, loaded via `fastembed`'s `Qwen3TextEmbedding`.
pub struct Qwen3Embedder {
    inner: fastembed::Qwen3TextEmbedding,
    dim: usize,
    model_id: String,
    max_embed_tokens: i32,
}

impl Qwen3Embedder {
    /// Load the model weights from HuggingFace (cached after first download)
    /// onto the Metal device on macOS, CPU elsewhere. The selected device is
    /// logged at startup.
    pub fn load(model: &EmbeddingModel) -> Result<Self> {
        let device = select_device();
        let label = device_label(&device);
        // The Qwen3-Embedding weights ship as bf16; loading them as bf16 (rather
        // than upconverting to f32) halves resident memory at no quality cost
        // and keeps the full f32 exponent range, so no overflow risk.
        //
        // `max_embed_tokens` is the tokenizer `max_length`: input past it is
        // truncated before inference, which is exactly the per-message cap - one
        // message, one vector, bounded embed cost (plan.md Stage 2).
        let inner = fastembed::Qwen3TextEmbedding::from_hf(
            model.load_repo(),
            &device,
            candle_core::DType::BF16,
            model.max_embed_tokens,
        )
        .map_err(|error| {
            anyhow!(
                "failed to load embedding model {}: {error}",
                model.load_repo()
            )
        })?;
        tracing::info!(model = %model.id, device = label, "loaded embedding model");
        Ok(Self {
            inner,
            dim: model.dim as usize,
            model_id: model.id.clone(),
            max_embed_tokens: model.max_embed_tokens as i32,
        })
    }

    /// The device the weights were loaded onto (`"metal"`, `"cuda"`, or `"cpu"`).
    pub fn device(&self) -> &'static str {
        device_label(self.inner.device())
    }
}

impl EmbedBackend for Qwen3Embedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self.inner
            .embed(texts)
            .map_err(|error| anyhow!("embedding inference failed: {error}"))
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn max_embed_tokens(&self) -> i32 {
        self.max_embed_tokens
    }
}

/// Select the embedding device: Metal on macOS, CUDA on a non-macOS build with
/// the `cuda` feature, CPU otherwise. candle's `*_if_available` helpers return
/// `Cpu` when the matching backend feature is not compiled into `candle-core`;
/// `new_metal` / `new_cuda` can still fail at runtime (no GPU or driver), so an
/// `Err` falls back to `Cpu` too. The chosen device is logged in [`Qwen3Embedder::load`].
fn select_device() -> candle_core::Device {
    #[cfg(target_os = "macos")]
    let device = candle_core::Device::metal_if_available(0);
    #[cfg(not(target_os = "macos"))]
    let device = candle_core::Device::cuda_if_available(0);
    device.unwrap_or_else(|error| {
        tracing::warn!(%error, "GPU device unavailable, falling back to CPU");
        candle_core::Device::Cpu
    })
}

fn device_label(device: &candle_core::Device) -> &'static str {
    match device {
        candle_core::Device::Cpu => "cpu",
        candle_core::Device::Cuda(_) => "cuda",
        candle_core::Device::Metal(_) => "metal",
    }
}

#[cfg(test)]
mod tests {
    use super::{device_label, select_device};

    // plan.md Stage 2 done-when: the embedding worker runs on the Metal device
    // on macOS (real Apple hardware), never the CPU fallback; a default
    // non-macOS build runs on CPU. `select_device` is the device-selection path
    // the worker takes; exercising it needs no model weights. A `--features cuda`
    // build can select a GPU at runtime, so the CPU assertion is scoped to the
    // default (no-`cuda`) build.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_selects_the_metal_device() {
        assert_eq!(device_label(&select_device()), "metal");
    }

    #[cfg(all(not(target_os = "macos"), not(feature = "cuda")))]
    #[test]
    fn non_macos_selects_cpu() {
        assert_eq!(device_label(&select_device()), "cpu");
    }
}
