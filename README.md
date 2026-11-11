# TAP Sentiment Stream

Pipeline end-to-end per l'analisi del **sentiment in tempo reale** dei post pubblici di Mastodon,
con **ricerca semantica** e un piccolo sistema **RAG** per interrogare i dati in linguaggio naturale.

Progetto per l'esame di *Technologies for Advanced Programming* (A.A. 2026/2027, prof. Salvatore Nicotra).
Autore: Mario Gabriele Vittoria .

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

## Avvio

```bash
cp .env.example .env          # opzionale: personalizza sorgente, soglie, modello LLM

make up                       # live da Mastodon   (= SOURCE=mastodon docker compose up -d --build)
make replay                   # offline, post di esempio (= SOURCE=replay docker compose --profile replay up -d --build)
make extra                    # opzionale: Kibana e Kafka UI (servono almeno 12-16 GB di RAM nel PC)
make llm                      # opzionale: avvia Ollama e scarica il modello per il RAG
```

| Layer | Tecnologia | Ruolo |
|---|---|---|
| Ingestion | Logstash 9.5 | Polling dell'API Mastodon (o lettura del file di replay), filtro e normalizzazione |
| Streaming | Apache Kafka 4.1 (KRaft) | Backbone: disaccoppia produttori e consumatori, buffer persistente, replay |
| Processing | Spark 4.0 Structured Streaming | Parsing, pulizia, deduplica, micro-batch con checkpoint |
| AI Inference | ONNX Runtime (Pandas UDF) | DistilBERT SST-2 per il sentiment, MiniLM per gli embedding (384 dim.) |
| Storage | Apache Iceberg 1.10 | Storico analitico: ACID, time travel, schema evolution |
| Indexing / Search | Elasticsearch 9.5 | Full-text + ricerca vettoriale kNN |
| Consumption | Kibana, Streamlit, Ollama | Dashboard, ricerca semantica, RAG |
| Deploy | Docker Compose | Tutto in container, avvio con un comando |


Dopo l'avvio i servizi sono raggiungibili qui:

| Servizio | URL |
|---|---|
| Streamlit (dashboard, ricerca, RAG) | http://localhost:8501 |
| Kibana (con `--profile extra`) | http://localhost:5601 |
| Kafka UI (con `--profile extra`) | http://localhost:8080 |
| Spark UI (micro-batch, tempi) | http://localhost:4040 |
| Elasticsearch | http://localhost:9200 |

In PowerShell, dalla cartella del progetto:

```powershell
$env:SOURCE="replay"
docker compose --profile replay up -d --build      # avvio offline
docker compose logs -f spark                        # log dei micro-batch (Ctrl+C per uscire)
docker compose exec spark /opt/spark/bin/spark-submit /opt/app/iceberg_demo.py --evolve
docker compose --profile replay down                # stop
```
Su un PC con 8 GB di RAM conviene limitare la memoria di WSL2 , altrimenti Windows si blocca. Crea il file `%USERPROFILE%\.wslconfig` con: 
```ini
[wsl2]
memory=4GB
processors=4
swap=4GB
```
poi chiudi Docker Desktop, esegui `wsl --shutdown` e riaprilo.

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
# serve Java 17 o 21 e il jar iceberg-spark-runtime-4.0_2.13-1.10.1 in tests/jars/
python -m pytest -q tests
```
