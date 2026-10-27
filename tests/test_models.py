import numpy as np


def test_sentiment_labels_and_neutral_band(fake_models):
    from onnx_models import SentimentModel
    model = SentimentModel(str(fake_models / "sentiment"), neutral_threshold=0.75)
    preds = model.predict(["good great love", "terrible broken hate", "nothing here", ""])
    labels = [p[0] for p in preds]
    assert labels == ["positive", "negative", "neutral", "neutral"]
    for label, score, value in preds:
        assert 0.5 <= score <= 1.0
        assert -1.0 <= value <= 1.0
    assert preds[0][2] > 0 > preds[1][2]


def test_sentiment_batching_keeps_order(fake_models):
    from onnx_models import SentimentModel
    model = SentimentModel(str(fake_models / "sentiment"))
    model.batch_size = 3
    texts = ["good", "bad"] * 10
    assert [p[0] for p in model.predict(texts)] == ["positive", "negative"] * 10


def test_embeddings_are_normalized_and_padding_ignored(fake_models):
    from onnx_models import EmbeddingModel
    model = EmbeddingModel(str(fake_models / "embedding"))
    vecs = model.encode(["good", "good bad terrible love", "good"])
    assert vecs.shape == (3, 384)
    np.testing.assert_allclose(np.linalg.norm(vecs, axis=1), 1.0, rtol=1e-5)
    # "good" da solo e "good" in un batch con frasi piu' lunghe (padding) danno lo stesso vettore
    np.testing.assert_allclose(vecs[0], vecs[2], rtol=1e-5)
    assert np.allclose(vecs[0], model.encode(["good"])[0], atol=1e-6)
