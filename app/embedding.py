# это import & GPU embedding

# import_service/embedding.py
from pathlib import Path
from sentence_transformers import SentenceTransformer
from loguru import logger
import torch

class ImportEmbedding:
    def __init__(self, cache_dir: str = "/app/cache"):
        self.cache_dir = Path(cache_dir)
        self._model = None
        self._model_path = self._find_model()
        # Проверяем доступность GPU
        if torch.cuda.is_available():
            self.device = 'cuda'
            logger.info(f"✅ GPU detected: {torch.cuda.get_device_name(0)}")
        else:
            self.device = 'cpu'
            logger.warning("⚠️ GPU not available, using CPU")

    def _find_model(self) -> Path:
        # Ищем локальную модель в кэше
        for model_dir in self.cache_dir.glob("models--intfloat--multilingual-e5-small*/snapshots/*"):
            if (model_dir / "model.safetensors").exists():
                logger.info(f"Found local model at {model_dir}")
                return model_dir
        raise RuntimeError("Full model not found. Please run setup_models.py first.")

    def _load(self):
        if self._model is None:
            logger.info(f"Loading full model from {self._model_path} on {self.device}...")
            self._model = SentenceTransformer(
                str(self._model_path),
                device=self.device
            )
            if self.device == 'cuda':
                self._model.half()  # экономия VRAM
                logger.info("Model converted to half precision (FP16)")

    def encode(self, texts: list[str]) -> list[list[float]]:
        self._load()
        # Используем batch_size=256 для эффективности на GPU
        return self._model.encode(
            texts,
            normalize_embeddings=True,
            batch_size=1024,
            show_progress_bar=False
        ).tolist()

    def unload(self):
        if self._model:
            del self._model
            self._model = None
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            logger.info("GPU model unloaded, VRAM freed")