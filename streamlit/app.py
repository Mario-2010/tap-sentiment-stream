"""TAP Sentiment Stream: dashboard live, ricerca semantica e RAG sui post analizzati."""
import os

import pandas as pd
import requests
import streamlit as st
from elasticsearch import Elasticsearch

from onnx_models import EmbeddingModel

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "mastodon-posts")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
COLORS = {"positive": "#2e9e5b", "neutral": "#9aa0a6", "negative": "#d64545"}

st.set_page_config(page_title="TAP Sentiment Stream", page_icon="📡", layout="wide")


@st.cache_resource
def es_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30)


@st.cache_resource
def embedder() -> EmbeddingModel:
    return EmbeddingModel()


def semantic_search(query: str, k: int, sentiment: str | None = None) -> list[dict]:
    """kNN su dense_vector: la domanda viene trasformata nello stesso spazio dei post."""
    vector = embedder().encode([query])[0].tolist()
    knn = {"field": "embedding", "query_vector": vector, "k": k, "num_candidates": max(100, 10 * k)}
    if sentiment:
        knn["filter"] = {"term": {"sentiment": sentiment}}
    res = es_client().search(index=ES_INDEX, knn=knn, size=k, source_excludes=["embedding"])
    return [{**h["_source"], "similarity": h["_score"]} for h in res["hits"]["hits"]]


# ------------------------------------------------------------------ dashboard

def dashboard(minutes: int) -> None:
    interval = "1m" if minutes <= 60 else "10m" if minutes <= 720 else "1h"
    res = es_client().search(
        index=ES_INDEX, size=10, sort=[{"created_at": "desc"}],
        source_excludes=["embedding"],
        query={"range": {"created_at": {"gte": f"now-{minutes}m"}}},
        aggs={
            "labels": {"terms": {"field": "sentiment"}},
            "avg": {"avg": {"field": "sentiment_value"}},
            "timeline": {
                "date_histogram": {"field": "created_at", "fixed_interval": interval},
                "aggs": {"labels": {"terms": {"field": "sentiment"}}},
            },
            "tags": {"terms": {"field": "hashtags", "size": 15},
                     "aggs": {"avg": {"avg": {"field": "sentiment_value"}}}},
        },
    )
    aggs = res["aggregations"]
    total = res["hits"]["total"]["value"]
    counts = {b["key"]: b["doc_count"] for b in aggs["labels"]["buckets"]}

    c = st.columns(5)
    c[0].metric("Post analizzati", total)
    for col, label in zip(c[1:4], ["positive", "neutral", "negative"]):
        share = 100 * counts.get(label, 0) / total if total else 0
        col.metric(label.capitalize(), counts.get(label, 0), f"{share:.0f}%", delta_color="off")
    avg = aggs["avg"]["value"]
    c[4].metric("Sentiment medio [-1, 1]", f"{avg:+.2f}" if avg is not None else "n/d")

    if not total:
        st.info("Nessun post nell'intervallo selezionato: la pipeline e' avviata?")
        return

    timeline = pd.DataFrame([
        {"time": pd.to_datetime(b["key_as_string"]), **{l["key"]: l["doc_count"] for l in b["labels"]["buckets"]}}
        for b in aggs["timeline"]["buckets"]
    ]).set_index("time").fillna(0)
    present = [l for l in ["positive", "neutral", "negative"] if l in timeline.columns]

    left, right = st.columns([2, 1])
    left.subheader("Post per sentiment nel tempo")
    left.area_chart(timeline[present], color=[COLORS[l] for l in present])
    right.subheader("Hashtag piu' frequenti")
    tags = pd.DataFrame([{"hashtag": f"#{b['key']}", "post": b["doc_count"],
                          "sentiment medio": round(b["avg"]["value"] or 0, 2)}
                         for b in aggs["tags"]["buckets"]])
    if not tags.empty:
        right.dataframe(tags, hide_index=True, use_container_width=True)

    st.subheader("Ultimi post")
    latest = pd.DataFrame([h["_source"] for h in res["hits"]["hits"]])
    st.dataframe(latest[["created_at", "author", "sentiment", "sentiment_score", "text"]],
                 hide_index=True, use_container_width=True)


# ------------------------------------------------------------------ RAG

def ask_llm(question: str, posts: list[dict]) -> str | None:
    context = "\n".join(f"[{i}] ({p['sentiment']}) {p['text']}" for i, p in enumerate(posts, 1))
    prompt = (
        "Sei un analista di social media. Rispondi in italiano alla domanda usando SOLO i post "
        "qui sotto, cita i post con [numero] e indica il sentiment prevalente. "
        "Se i post non bastano per rispondere, dillo.\n\n"
        f"POST:\n{context}\n\nDOMANDA: {question}"
    )
    try:
        r = requests.post(f"{OLLAMA_URL}/api/chat", timeout=120, json={
            "model": OLLAMA_MODEL, "stream": False,
            "messages": [{"role": "user", "content": prompt}],
        })
        r.raise_for_status()
        return r.json()["message"]["content"]
    except (requests.RequestException, KeyError):
        return None


def show_posts(posts: list[dict]) -> None:
    for i, p in enumerate(posts, 1):
        tags = " ".join(f"#{t}" for t in p.get("hashtags") or [])
        st.markdown(
            f"**[{i}]** :{'green' if p['sentiment'] == 'positive' else 'red' if p['sentiment'] == 'negative' else 'gray'}"
            f"[{p['sentiment']}] · similarita' {p['similarity']:.2f} · @{p['author']}  \n"
            f"{p['text']}  \n<small>{tags} · [link]({p['url']})</small>",
            unsafe_allow_html=True,
        )


# ------------------------------------------------------------------ layout

st.title("📡 TAP Sentiment Stream")
st.caption("Mastodon → Logstash → Kafka → Spark (ONNX) → Iceberg + Elasticsearch → Streamlit")

tab_dash, tab_search, tab_rag = st.tabs(["Dashboard live", "Ricerca semantica", "Chiedi ai dati (RAG)"])

with tab_dash:
    minutes = st.select_slider("Intervallo", options=[15, 60, 360, 1440, 10080], value=60,
                               format_func=lambda m: f"ultimi {m} min" if m < 1440 else f"ultimi {m // 1440} giorni")

    @st.fragment(run_every=10)
    def live():
        dashboard(minutes)

    live()

with tab_search:
    q = st.text_input("Cerca per significato, non per parole esatte", "people complaining about the weather")
    col1, col2 = st.columns(2)
    k = col1.slider("Risultati", 3, 20, 8)
    filt = col2.selectbox("Filtra per sentiment", ["tutti", "positive", "neutral", "negative"])
    if q:
        show_posts(semantic_search(q, k, None if filt == "tutti" else filt))

with tab_rag:
    st.write("Retrieval Augmented Generation: recupero i post piu' simili alla domanda "
             "con la ricerca vettoriale e li passo come contesto a un LLM locale (Ollama).")
    question = st.text_input("Domanda", "What do people think about AI today?")
    if st.button("Chiedi", type="primary") and question:
        posts = semantic_search(question, 8)
        if not posts:
            st.warning("Nessun post indicizzato.")
        else:
            with st.spinner("Genero la risposta..."):
                answer = ask_llm(question, posts)
            if answer:
                st.markdown(answer)
            else:
                share = pd.Series([p["sentiment"] for p in posts]).value_counts(normalize=True)
                st.info(f"LLM non raggiungibile ({OLLAMA_URL}): mostro solo il retrieval. "
                        f"Sentiment dei post recuperati: "
                        + ", ".join(f"{l} {v:.0%}" for l, v in share.items()))
            with st.expander("Fonti usate come contesto", expanded=not answer):
                show_posts(posts)
