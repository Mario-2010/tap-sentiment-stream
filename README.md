# TAP Sentiment Stream

Pipeline end-to-end per l'analisi del **sentiment in tempo reale** dei post pubblici di Mastodon,
con **ricerca semantica** e un piccolo sistema **RAG** per interrogare i dati in linguaggio naturale.

Progetto per l'esame di *Technologies for Advanced Programming* (A.A. 2026/2027, prof. Salvatore Nicotra).
Autore: Mario Gabriele.

```mermaid
flowchart LR
    M[Mastodon API<br/>timeline pubblica] -->|http_poller| L[Logstash]
    R[Replayer<br/>post di esempio] -->|file tail| L
    L -->|topic mastodon-posts<br/>3 partizioni| K[(Kafka)]
    K --> S[Spark Structured Streaming<br/>pulizia + dedup<br/>Pandas UDF ONNX:<br/>sentiment + embedding]
    S -->|MERGE| I[(Apache Iceberg<br/>Lakehouse)]
    S -->|Bulk API| E[(Elasticsearch<br/>testo + dense_vector)]
    E --> KB[Kibana]
    E --> ST[Streamlit<br/>dashboard, ricerca, RAG]
    O[Ollama LLM] -.-> ST
```

| Layer | Tecnologia | Ruolo |
|---|---|---|
| Ingestion | Logstash 8.15 | Polling dell'API Mastodon (o lettura del file di replay), filtro e normalizzazione |
| Streaming | Apache Kafka 3.8 (KRaft) | Backbone: disaccoppia produttori e consumatori, buffer persistente, replay |
| Processing | Spark 3.5 Structured Streaming | Parsing, pulizia, deduplica, micro-batch con checkpoint |
| AI Inference | ONNX Runtime (Pandas UDF) | DistilBERT SST-2 per il sentiment, MiniLM per gli embedding (384 dim.) |
| Storage | Apache Iceberg 1.6 | Storico analitico: ACID, time travel, schema evolution |
| Indexing / Search | Elasticsearch 8.15 | Full-text + ricerca vettoriale kNN |
| Consumption | Kibana, Streamlit, Ollama | Dashboard, ricerca semantica, RAG |
| Deploy | Docker Compose | Tutto in container, avvio con un comando |

## Requisiti

- Docker Desktop (o Docker Engine + Compose v2), almeno **8 GB di RAM** assegnati a Docker
- Internet al primo build (immagini, connettori Maven, modelli Hugging Face) e per la modalita' live

## Avvio

```bash
cp .env.example .env          # opzionale: personalizza sorgente, soglie, modello LLM

make up                       # live da Mastodon   (= SOURCE=mastodon docker compose up -d --build)
make replay                   # offline, post di esempio (= SOURCE=replay docker compose --profile replay up -d --build)
make llm                      # opzionale: avvia Ollama e scarica il modello per il RAG
```

Il primo build richiede qualche minuto. Poi:

| Servizio | URL |
|---|---|
| Streamlit (dashboard, ricerca, RAG) | http://localhost:8501 |
| Kibana | http://localhost:5601 |
| Kafka UI (topic, partizioni, messaggi) | http://localhost:8080 |
| Spark UI (micro-batch, tempi) | http://localhost:4040 |
| Elasticsearch | http://localhost:9200 |

Comandi utili:

```bash
make logs                     # log di Spark e Logstash (si vede ogni micro-batch con le sue metriche)
make demo                     # demo Iceberg: query SQL, snapshot, time travel
make evolve                   # come sopra + schema evolution (ALTER TABLE ADD COLUMN)
make down                     # ferma tutto (i dati restano nei volumi)
make clean                    # ferma tutto e cancella i dati
curl 'localhost:9200/mastodon-posts/_count'
```

> **All'esame:** se la rete dell'aula non e' affidabile usa `make replay`: la pipeline e' identica,
> cambia solo l'input di Logstash. Se un'istanza Mastodon non espone la timeline pubblica senza
> login, cambia `MASTODON_URL` nel file `.env` (es. `https://fosstodon.org`).

### Dashboard Kibana

`kibana-init` crea in automatico le data view `mastodon-posts` e `pipeline-metrics`.
Per una dashboard in 3 minuti: *Analytics → Dashboards → Create → Create visualization* e aggiungi

1. **Bar vertical stacked**: asse X `created_at` (date histogram), breakdown `sentiment`;
2. **Donut**: slice by `sentiment`;
3. **Metric**: average di `sentiment_value`;
4. **Table**: top values di `hashtags` con average di `sentiment_value`;
5. sulla data view `pipeline-metrics`: **Line** di `avg_latency_s` e `rows_in` per `processed_at`.

## Struttura

```
docker-compose.yml          tutti i servizi
logstash/pipelines/         input (mastodon | replay) + filtro e output comuni
setup/                      mapping Elasticsearch, init di Kibana, script del replayer
data/sample_posts.jsonl     40 post di esempio (scritti a mano, non reali) per la modalita' offline
common/onnx_models.py       wrapper ONNX Runtime per sentiment ed embedding (usato da Spark e Streamlit)
common/download_models.py   scarica i modelli ONNX da Hugging Face al build
spark/app/streaming_job.py  job Spark Structured Streaming
spark/app/iceberg_demo.py   demo Lakehouse (time travel, schema evolution)
streamlit/app.py            dashboard live, ricerca semantica, RAG
tests/                      test con modelli ONNX "giocattolo" e Iceberg reale
docs/RELAZIONE.md           relazione del progetto
```

## Test

```bash
pip install -r tests/requirements.txt
# serve Java 17 (Spark 3.5 non supporta Java 21) e il jar iceberg-spark-runtime-3.5_2.12-1.6.1 in tests/jars/
python -m pytest -q tests
```

## Note e limiti

- Il modello di sentiment e' addestrato su recensioni in inglese: Logstash tiene solo i post `language = en`.
  Per l'italiano basta sostituire il modello con uno multilingue (vedi relazione, sezione sviluppi futuri).
- Il modello SST-2 e' binario: la classe `neutral` e' ottenuta con una soglia sulla confidenza (`NEUTRAL_THRESHOLD`).
- Configurazione da sviluppo: un solo broker Kafka, un nodo Elasticsearch senza sicurezza, Spark in `local[*]`.
