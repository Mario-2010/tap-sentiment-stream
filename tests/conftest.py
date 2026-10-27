"""Fixture: modelli ONNX "giocattolo" con la stessa interfaccia di quelli reali.

Cosi' i test girano offline e in pochi secondi: verificano tokenizzazione, batching,
pooling e l'integrazione con Spark, non la qualita' del modello.
"""
import glob
import json
import os
import sys

import numpy as np
import onnx
import pytest
from onnx import TensorProto, helper, numpy_helper
from tokenizers import Tokenizer, models, pre_tokenizers

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATHS = [os.path.join(ROOT, "common"), os.path.join(ROOT, "spark", "app")]
sys.path[:0] = PATHS
# anche i worker Python di Spark devono trovare i moduli (nel container: --py-files)
os.environ["PYTHONPATH"] = os.pathsep.join(PATHS + [os.environ.get("PYTHONPATH", "")])

VOCAB = ["[PAD]", "[UNK]", "good", "great", "love", "amazing", "bad", "terrible", "hate", "broken"]
POSITIVE, NEGATIVE = {"good", "great", "love", "amazing"}, {"bad", "terrible", "hate", "broken"}


def _tokenizer(path):
    tok = Tokenizer(models.WordLevel({w: i for i, w in enumerate(VOCAB)}, unk_token="[UNK]"))
    tok.pre_tokenizer = pre_tokenizers.Whitespace()
    tok.save(path)


def _save(graph, path):
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
    model.ir_version = 8
    onnx.save(model, path)


def _sentiment_model(path):
    # logits = somma sui token di una tabella [vocab, 2]: parole positive -> colonna 1
    table = np.zeros((len(VOCAB), 2), dtype=np.float32)
    for i, w in enumerate(VOCAB):
        table[i] = [0, 3] if w in POSITIVE else [3, 0] if w in NEGATIVE else [0, 0]
    g = helper.make_graph(
        [helper.make_node("Gather", ["table", "input_ids"], ["emb"]),
         helper.make_node("Cast", ["attention_mask"], ["maskf"], to=TensorProto.FLOAT),
         helper.make_node("Unsqueeze", ["maskf", "axis"], ["mask3"]),
         helper.make_node("Mul", ["emb", "mask3"], ["masked"]),
         helper.make_node("ReduceSum", ["masked", "axis1"], ["logits"], keepdims=0)],
        "sentiment",
        [helper.make_tensor_value_info("input_ids", TensorProto.INT64, ["b", "s"]),
         helper.make_tensor_value_info("attention_mask", TensorProto.INT64, ["b", "s"])],
        [helper.make_tensor_value_info("logits", TensorProto.FLOAT, ["b", 2])],
        [numpy_helper.from_array(table, "table"),
         numpy_helper.from_array(np.array([2], dtype=np.int64), "axis"),
         numpy_helper.from_array(np.array([1], dtype=np.int64), "axis1")])
    _save(g, path)


def _embedding_model(path, dims=384):
    table = np.random.default_rng(0).normal(size=(len(VOCAB), dims)).astype(np.float32)
    g = helper.make_graph(
        [helper.make_node("Gather", ["table", "input_ids"], ["last_hidden_state"])],
        "embedding",
        [helper.make_tensor_value_info("input_ids", TensorProto.INT64, ["b", "s"]),
         helper.make_tensor_value_info("attention_mask", TensorProto.INT64, ["b", "s"]),
         helper.make_tensor_value_info("token_type_ids", TensorProto.INT64, ["b", "s"])],
        [helper.make_tensor_value_info("last_hidden_state", TensorProto.FLOAT, ["b", "s", dims])],
        [numpy_helper.from_array(table, "table")])
    _save(g, path)


@pytest.fixture(scope="session", autouse=True)
def fake_models(tmp_path_factory):
    base = tmp_path_factory.mktemp("models")
    for name, build in [("sentiment", _sentiment_model), ("embedding", _embedding_model)]:
        d = base / name
        d.mkdir()
        _tokenizer(str(d / "tokenizer.json"))
        build(str(d / "model.onnx"))
    json.dump({"id2label": {"0": "NEGATIVE", "1": "POSITIVE"}}, open(base / "sentiment" / "config.json", "w"))
    os.environ["SENTIMENT_MODEL_DIR"] = str(base / "sentiment")
    os.environ["EMBEDDING_MODEL_DIR"] = str(base / "embedding")
    return base


def iceberg_jar():
    jars = glob.glob(os.path.join(ROOT, "tests", "jars", "iceberg-spark-runtime-*.jar"))
    return jars[0] if jars else None
