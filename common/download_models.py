"""Scarica dall'Hugging Face Hub i modelli gia' esportati in ONNX (eseguito al build dell'immagine).

- sentiment: DistilBERT fine-tuned su SST-2 (classificazione POSITIVE/NEGATIVE)
- embedding: all-MiniLM-L6-v2 (vettori a 384 dimensioni per la ricerca semantica)

Le versioni "quantized" (int8) sono ~4 volte piu' piccole e piu' veloci su CPU.
"""
import os
import shutil
import sys

from huggingface_hub import hf_hub_download

# per ogni modello: il repository su hugging face e i 3 file che mi servono
MODELS = {
    "sentiment": ("Xenova/distilbert-base-uncased-finetuned-sst-2-english",
                  ["onnx/model_quantized.onnx", "tokenizer.json", "config.json"]),
    "embedding": ("Xenova/all-MiniLM-L6-v2",
                  ["onnx/model_quantized.onnx", "tokenizer.json", "config.json"]),
}


def main(target: str, which: list[str]) -> None:
    for name in which:
        repo, files = MODELS[name]
        out_dir = os.path.join(target, name)
        os.makedirs(out_dir, exist_ok=True)
        for f in files:
            path = hf_hub_download(repo_id=repo, filename=f)
            # lo rinomino model.onnx, cosi' onnx_models.py lo trova sempre con lo stesso nome
            dest = "model.onnx" if f.endswith(".onnx") else os.path.basename(f)
            shutil.copy(path, os.path.join(out_dir, dest))
        print(f"{name}: {repo} -> {out_dir}")


# es. "python download_models.py /models embedding" scarica solo l'embedding (streamlit),
# senza argomenti li scarica tutti e due (spark)
if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/models",
         sys.argv[2:] or list(MODELS))