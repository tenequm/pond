#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModel {
    pub id: String,
    pub dimensions: u16,
}

impl EmbeddingModel {
    pub fn qwen3_default() -> Self {
        Self {
            id: "Qwen/Qwen3-Embedding-0.6B".to_owned(),
            dimensions: 1024,
        }
    }
}
