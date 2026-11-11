"""Carico ed eseguo i due modelli ONNX (sentiment ed embedding).
Uso onnxruntime al posto di PyTorch cosi' l'immagine Docker resta leggera.
Lo usano sia Spark che Streamlit.
"""
from __future__ import annotations

import json
import os

import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

SENTIMENT_DIR = os.getenv("SENTIMENT_MODEL_DIR", "/models/sentiment")
EMBEDDING_DIR = os.getenv("EMBEDDING_MODEL_DIR", "/models/embedding")


# parte comune ai due modelli: tokenizer + modello onnx, lavora a batch
class OnnxTextModel:

    def __init__(self, model_dir: str, max_length: int = 128, batch_size: int = 32):
        self.tokenizer = Tokenizer.from_file(os.path.join(model_dir, "tokenizer.json"))
        # le frasi piu' lunghe di 128 token vengono tagliate
        self.tokenizer.enable_truncation(max_length=max_length)
        # padding: tutte le frasi del batch devono avere la stessa lunghezza (matrice rettangolare)
        self.tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = int(os.getenv("ORT_THREADS", "1"))  # un thread solo, il parallelismo lo fa gia' spark
        self.session = ort.InferenceSession(
            os.path.join(model_dir, "model.onnx"), opts, providers=["CPUExecutionProvider"]
        )
        self.input_names = {i.name for i in self.session.get_inputs()}
        self.batch_size = batch_size
        config_path = os.path.join(model_dir, "config.json")
        self.config = json.load(open(config_path)) if os.path.exists(config_path) else {}

    # frasi -> numeri (token) -> modello. restituisce l'output e la maschera
    def _run(self, texts: list[str]):
        enc = self.tokenizer.encode_batch(texts)
        ids = np.array([e.ids for e in enc], dtype=np.int64)
        mask = np.array([e.attention_mask for e in enc], dtype=np.int64)  # 1 = token vero, 0 = padding
        feed = {"input_ids": ids, "attention_mask": mask}
        # distilbert non lo vuole, minilm si'
        if "token_type_ids" in self.input_names:
            feed["token_type_ids"] = np.zeros_like(ids)
        return self.session.run(None, feed)[0], mask

    def batches(self, texts: list[str]):
        for start in range(0, len(texts), self.batch_size):
            yield self._run(texts[start:start + self.batch_size])


# distilbert sa dire solo positivo o negativo: se non e' abbastanza sicuro
# (sotto la soglia, 0.75 di default) il post diventa neutral
class SentimentModel(OnnxTextModel):

    def __init__(self, model_dir: str = SENTIMENT_DIR, neutral_threshold: float | None = None):
        super().__init__(model_dir)
        id2label = self.config.get("id2label", {"0": "NEGATIVE", "1": "POSITIVE"})
        self.labels = [id2label[str(i)].lower() for i in range(len(id2label))]
        self.pos_idx = self.labels.index("positive")
        self.neg_idx = self.labels.index("negative")
        self.threshold = neutral_threshold if neutral_threshold is not None else float(
            os.getenv("NEUTRAL_THRESHOLD", "0.75"))

    # per ogni post: etichetta, quanto e' sicuro, e un valore da -1 a 1
    def predict(self, texts: list[str]) -> list[tuple[str, float, float]]:
        out = []
        for logits, _ in self.batches(texts):
            # softmax. tolgo il massimo per non andare in overflow con exp
            probs = np.exp(logits - logits.max(axis=1, keepdims=True))
            probs /= probs.sum(axis=1, keepdims=True)
            for p in probs:
                best = int(p.argmax())
                label = self.labels[best] if p[best] >= self.threshold else "neutral"
                out.append((label, float(p[best]), float(p[self.pos_idx] - p[self.neg_idx])))
        return out


# minilm: trasforma una frase in un vettore di 384 numeri che ne rappresenta il significato
class EmbeddingModel(OnnxTextModel):

    def __init__(self, model_dir: str = EMBEDDING_DIR):
        super().__init__(model_dir)

    def encode(self, texts: list[str]) -> np.ndarray:
        chunks = []
        for hidden, mask in self.batches(texts):
            m = mask[..., None].astype(np.float32)  # aggiungo un asse per poter moltiplicare con hidden
            # media dei vettori dei token senza contare il padding
            pooled = (hidden * m).sum(axis=1) / np.clip(m.sum(axis=1), 1e-9, None)
            # normalizzo: con norma 1 il prodotto scalare e' gia' il coseno
            pooled /= np.clip(np.linalg.norm(pooled, axis=1, keepdims=True), 1e-9, None)
            chunks.append(pooled.astype(np.float32))
        return np.vstack(chunks) if chunks else np.zeros((0, 0), dtype=np.float32)