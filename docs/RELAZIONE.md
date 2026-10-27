# TAP Sentiment Stream
## Analisi del sentiment in tempo reale su Mastodon con ricerca semantica e RAG

**Corso:** Technologies for Advanced Programming, A.A. 2026/2027, prof. Salvatore Nicotra
**Studente:** Mario Gabriele, Corso di Laurea in Matematica e Informatica (L-31)

---

## 1. Obiettivo

Il progetto realizza una pipeline dati *end-to-end*, completamente containerizzata, che:

1. **acquisisce** in continuo i post pubblici del social network Mastodon;
2. li **trasporta** attraverso un sistema di streaming (Kafka);
3. li **elabora** in tempo reale con Spark, applicando due modelli di Intelligenza Artificiale:
   uno di **classificazione** (sentiment positivo, negativo, neutro) e uno di **embedding** (vettore semantico);
4. li **memorizza** in un Lakehouse (Apache Iceberg) per l'analisi storica e in Elasticsearch per la ricerca;
5. li **rende consultabili** con una dashboard in tempo reale, una ricerca semantica e un assistente
   RAG (Retrieval Augmented Generation) che risponde a domande in linguaggio naturale citando i post.

La domanda a cui il sistema risponde e' semplice: *"cosa sta dicendo la gente, in questo momento,
e con quale tono?"*. Lo stesso schema si applica a recensioni di prodotti, ticket di assistenza o log applicativi.

Ho scelto Mastodon perche' espone un'API pubblica gratuita, senza account e senza chiavi, con dati reali
e continui; e' anche la sorgente usata negli esempi del corso (istanza `mastodon.cloud`). Le versioni degli
strumenti sono le stesse usate a lezione: Spark 4.0.1, Logstash 9.5.5, Kafka in modalita' KRaft. Per le demo senza Internet il progetto include una modalita' *replay* che riproduce dei post di esempio.

---

## 2. Architettura

```
 Mastodon API ──http_poller──┐
                             ├──> Logstash ──> Kafka ──> Spark Structured Streaming ──┬──> Iceberg (Lakehouse)
 Replayer (file) ──tail──────┘    (filtro)    (topic      (pulizia, dedup,             │
                                               3 part.)    Pandas UDF ONNX)            └──> Elasticsearch ──> Kibana
                                                                                                  │
                                                                                                  └──> Streamlit ──> Ollama (LLM)
```

L'architettura segue i layer visti a lezione:

| Layer | Componente | Cosa fa nel progetto |
|---|---|---|
| Ingestion | Logstash | Interroga l'API ogni 20 s, scarta reblog e post non in inglese, normalizza lo schema |
| Streaming | Kafka | Topic `mastodon-posts` con 3 partizioni, chiave = id del post |
| Processing + AI | Spark Structured Streaming | Micro-batch ogni 10 s: parsing, pulizia HTML, deduplica, sentiment, embedding |
| Storage | Apache Iceberg | Tabella `lake.tap.posts`, partizionata per giorno, con storico degli snapshot |
| Indexing / Search | Elasticsearch | Indice con campo `text` (full-text) e `embedding` (`dense_vector` a 384 dimensioni) |
| Consumption | Kibana, Streamlit, Ollama | Dashboard, ricerca per significato, domande in linguaggio naturale |
| Deploy | Docker Compose | 12 servizi, avvio con un solo comando |

Si tratta di un'architettura **Kappa**: c'e' un unico percorso, quello in streaming, e lo storico
non viene ricalcolato da un layer batch separato come nella Lambda Architecture. Se serve rielaborare
tutto (ad esempio con un modello migliore) basta rileggere il topic Kafka dall'inizio, perche' Kafka
conserva i messaggi (retention di 7 giorni), oppure rileggere la tabella Iceberg.

### 2.1 Il viaggio di un post

1. Logstash scarica gli ultimi 40 post della timeline pubblica. L'API restituisce un array JSON e il
   codec `json` genera un evento per ogni elemento.
2. Il filtro scarta i reblog e i post con `language != "en"`, poi un piccolo script Ruby tiene solo i
   campi utili (`id`, `created_at`, `content_html`, `author`, `hashtags`, contatori).
3. L'evento viene pubblicato sul topic Kafka con **chiave = id del post**: lo stesso post finisce sempre
   nella stessa partizione.
4. Spark legge il topic, deserializza il JSON con uno **schema esplicito** (il "contratto" del dato),
   pulisce il testo (tag HTML, entita' come `&amp;`, link) e marca come non validi i record senza id,
   senza data o con testo vuoto.
5. Siccome il polling restituisce spesso post gia' visti, Spark **elimina i duplicati** con
   `dropDuplicatesWithinWatermark(["id"])` su una finestra di 15 minuti.
6. Per ogni micro-batch due **Pandas UDF** eseguono i modelli ONNX: il primo restituisce
   `(etichetta, confidenza, valore in [-1, 1])`, il secondo un vettore di 384 numeri.
7. Il risultato viene scritto su **Iceberg** con un `MERGE INTO` e su **Elasticsearch** con la Bulk API,
   usando l'id del post come id del documento. Infine viene scritto un documento di **metriche** del batch.
8. Streamlit e Kibana interrogano Elasticsearch; il RAG trasforma la domanda in un vettore, recupera i post
   piu' simili e li passa a un LLM locale.

---

## 3. Scelte tecnologiche e motivazioni

### 3.1 Ingestion: Logstash

Logstash ha gia' i plugin necessari (`http_poller` per l'API, `file` per il replay, `kafka` in output) e
permette di configurare la pipeline in modo dichiarativo, senza scrivere un producer.
L'input e' **intercambiabile**: il container monta un input diverso (`mastodon.conf` o `replay.conf`) e
lo stesso filtro e output comuni, quindi il resto del sistema non si accorge della differenza.

*Alternative:* **Fluent Bit** e' piu' leggero ed e' ideale per raccogliere log da file o da container, ma
ha meno strumenti per interrogare API REST; **Apache NiFi** offre un'interfaccia grafica e il tracciamento
della provenienza di ogni record, ma per un solo flusso sarebbe sovradimensionato.

### 3.2 Streaming backbone: Apache Kafka

Kafka e' un **log distribuito, partizionato e persistente**. Rispetto a una coda tradizionale
(RabbitMQ, ActiveMQ) la differenza chiave e' che il messaggio **non viene cancellato quando viene letto**:
ogni consumatore tiene il proprio *offset*. Questo permette di:

- **disaccoppiare** produttore e consumatore: se Spark si ferma, Logstash continua a scrivere e Spark
  recupera quando riparte;
- avere **piu' consumatori indipendenti** sullo stesso flusso (es. un secondo job per un altro modello);
- fare **replay**: rielaborare i dati passati riportando indietro l'offset;
- **scalare** in orizzontale: le 3 partizioni possono essere lette da 3 task Spark in parallelo.

Uso Kafka 4.1 in modalita' **KRaft**, cioe' senza ZooKeeper: i metadati del cluster sono gestiti da Kafka
stesso tramite il protocollo di consenso Raft.

### 3.3 Processing: Spark Structured Streaming

Structured Streaming tratta lo stream come una **tabella illimitata** a cui si aggiungono righe: si usa la
stessa API dei DataFrame e dello SQL. Il modello e' a **micro-batch** (un trigger ogni 10 secondi).

Punti importanti del job:

- **Schema esplicito** in `from_json`: un campo inatteso non rompe il job e lo schema e' documentato nel codice.
- **Watermark + deduplica:** lo stato della deduplica e' limitato a 15 minuti, quindi la memoria non cresce all'infinito.
- **`maxOffsetsPerTrigger = 200`:** limita i messaggi per batch, una forma di *backpressure* che evita
  batch enormi dopo un fermo.
- **Checkpoint:** offset Kafka e stato della deduplica vengono salvati su disco. Dopo un crash il job
  riparte dall'ultimo batch non completato.
- **`foreachBatch`:** permette di scrivere lo stesso micro-batch su due destinazioni diverse e di calcolarne le metriche.
- **`persist()`:** l'inferenza e' l'operazione piu' costosa; senza cache Spark la rieseguirebbe per ogni
  sink (Iceberg, Elasticsearch, metriche).

### 3.4 AI Inference: ONNX Runtime dentro una Pandas UDF

**Perche' ONNX.** ONNX e' un formato standard per i modelli di machine learning. I modelli Hugging Face sono
stati esportati in ONNX e **quantizzati** (pesi a 8 bit): niente PyTorch nell'immagine (centinaia di MB in meno)
e inferenza su CPU 2-4 volte piu' veloce, con una perdita di accuratezza trascurabile.
Il tokenizer usa la libreria `tokenizers` (scritta in Rust), la stessa usata da `transformers`.

**Perche' una Pandas UDF.** Una UDF Python normale viene chiamata **una riga alla volta**, serializzando ogni
riga tra la JVM e Python. Una **Pandas UDF** riceve invece blocchi di righe come `pandas.Series` trasferiti con
**Apache Arrow** (formato colonnare, senza copie inutili): il modello lavora su batch, che e' il modo efficiente
di usare una rete neurale.

Uso la variante **"Iterator of Series → Iterator of Series"**:

```python
@F.pandas_udf(SENTIMENT_TYPE)
def sentiment_udf(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    model = SentimentModel()          # caricato UNA volta per task
    for texts in batches:
        yield pd.DataFrame(model.predict(texts.tolist()), columns=["label", "score", "value"])
```

Il modello viene caricato una sola volta per task e poi riusato per tutti i blocchi, invece di essere ricaricato
a ogni chiamata. Il modello viaggia con l'immagine Docker, quindi ogni executor lo ha in locale.

**I due modelli:**

| | Sentiment | Embedding |
|---|---|---|
| Modello | DistilBERT fine-tuned su SST-2 | all-MiniLM-L6-v2 (Sentence-Transformers) |
| Compito | Classificazione | Rappresentazione semantica |
| Output | 2 logit → softmax → etichetta | 384 numeri (mean pooling + normalizzazione L2) |
| Uso | Grafici, filtri, metriche | Ricerca kNN, RAG |

La classe **neutral** non esiste nel modello SST-2 (che e' binario): la ricavo con una **soglia sulla confidenza**.
Se la classe vincente ha probabilita' sotto 0,75 il post e' "neutral". Il valore continuo
`P(positivo) − P(negativo)` in [-1, 1] serve per le medie nel tempo.

### 3.5 Storage: Apache Iceberg (Lakehouse)

Salvare direttamente file Parquet in una cartella (data lake "semplice") ha diversi problemi:
nessuna transazione (un job che fallisce a meta' lascia file parziali), nessun modo sicuro di aggiornare o
cancellare righe, schema non gestito, e per sapere quali file leggere bisogna elencare le cartelle.

**Iceberg** e' un *table format*: i dati restano in Parquet, ma sopra c'e' uno strato di **metadati**
(metadata file → manifest list → manifest → data file) che porta:

- **transazioni ACID:** ogni micro-batch e' un commit atomico, un lettore non vede mai dati a meta';
- **snapshot e Time Travel:** ogni commit crea uno snapshot; si puo' interrogare la tabella com'era
  (`VERSION AS OF <id>` o `TIMESTAMP AS OF '...'`);
- **Schema Evolution:** `ALTER TABLE ADD COLUMN` modifica solo i metadati; le colonne sono identificate da un id,
  non dal nome o dalla posizione, quindi rinominare o aggiungere colonne e' sicuro;
- **partizionamento nascosto:** la tabella e' partizionata con `days(created_at)`; le query filtrano su
  `created_at` e Iceberg salta da solo le partizioni inutili;
- **`MERGE INTO`:** permette scritture idempotenti (vedi sezione 5).

Lo script `iceberg_demo.py` mostra tutto questo durante l'esame: query SQL, elenco snapshot, time travel e
aggiunta di una colonna mentre lo streaming continua a scrivere.

Uso un catalogo di tipo **Hadoop** (metadati su file system): e' il piu' semplice per un progetto locale.
In produzione si userebbe un catalogo REST, Hive o Nessie e uno storage a oggetti come S3 o MinIO.

### 3.6 Indexing e Search: Elasticsearch

Elasticsearch fa due cose nel progetto:

- **ricerca full-text** sul campo `text`, con analyzer inglese (stemming: "running" trova "run");
- **ricerca vettoriale** sul campo `embedding` di tipo `dense_vector` con similarita' coseno, indicizzato con
  **HNSW** (un grafo per la ricerca approssimata dei vicini, molto piu' veloce di confrontare tutti i vettori).

Il mapping e' **esplicito e `strict`**: un campo non previsto viene rifiutato invece di essere aggiunto
silenziosamente con un tipo sbagliato. E' una piccola forma di *data contract*.

### 3.7 Data Consumption: Kibana, Streamlit e RAG

**Kibana** serve per l'esplorazione libera dei dati e per monitorare la pipeline (indice `pipeline-metrics`).
**Streamlit** e' l'applicazione "per l'utente finale", con tre schede:

1. **Dashboard live** (si aggiorna ogni 10 s): numero di post, percentuali per sentiment, sentiment medio,
   andamento nel tempo, hashtag piu' usati con il loro sentiment medio, ultimi post;
2. **Ricerca semantica:** la frase cercata viene trasformata in vettore con lo **stesso** modello usato da Spark e
   si cercano i post piu' vicini. Cercando *"people complaining about trains"* si trova un post che dice
   *"Train cancelled again"* anche senza parole in comune;
3. **RAG:** vedi sezione 4.

---

## 4. Classificazione vs Embedding, e il RAG

Entrambi i modelli sono **Transformer encoder** (famiglia BERT), ma sono usati in modo diverso:

- nella **classificazione** sopra l'encoder c'e' una "testa" (un livello lineare) addestrata a produrre
  un punteggio per ogni classe. L'output e' una **decisione** (positivo/negativo) e il modello e' stato
  addestrato con esempi etichettati (supervisionato su SST-2);
- nell'**embedding** non c'e' testa: si prendono i vettori di tutti i token dell'ultimo livello e se ne fa la media
  (*mean pooling*), ottenendo un vettore che **rappresenta il significato** della frase. Il modello e' stato
  addestrato (contrastive learning) perche' frasi simili abbiano vettori vicini. L'output non e' una risposta ma
  una **posizione in uno spazio** in cui si misura la similarita' (coseno).

Il **RAG** (Retrieval Augmented Generation) unisce le due cose a un LLM:

1. **Retrieval:** la domanda dell'utente diventa un vettore → ricerca kNN in Elasticsearch → 8 post piu' simili;
2. **Augmentation:** i post vengono inseriti nel prompt con la loro etichetta di sentiment;
3. **Generation:** un LLM locale (Ollama, modello `qwen2.5:1.5b`) risponde **solo** in base a quei post e li cita con [n].

Vantaggi: l'LLM risponde su dati freschi (di pochi secondi fa) che non ha mai visto in addestramento, le risposte
sono **verificabili** (le fonti sono mostrate) e si riducono le "allucinazioni". Se Ollama non e' attivo l'app
mostra comunque i post recuperati e la distribuzione del loro sentiment.

---

## 5. Affidabilita' e tolleranza ai guasti

| Problema | Soluzione nel progetto |
|---|---|
| Spark si ferma o va in crash | Kafka conserva i messaggi; al riavvio il **checkpoint** fa ripartire dall'ultimo offset |
| Un batch viene rieseguito dopo un crash | Scritture **idempotenti**: `MERGE INTO ... WHEN NOT MATCHED` su Iceberg, `_id = id del post` su Elasticsearch |
| Il polling restituisce gli stessi post | Deduplica con watermark in Spark + idempotenza dei sink |
| Picco di dati dopo un fermo | `maxOffsetsPerTrigger` limita la dimensione dei batch |
| Record malformati | Schema esplicito; i record non validi sono scartati e **contati** nelle metriche |
| Cambia la struttura della tabella | Iceberg schema evolution; il `MERGE` elenca le colonne in modo esplicito |
| Avvio nell'ordine sbagliato | `depends_on` con `healthcheck` e container di init in Docker Compose |
| Rete assente all'esame | Modalita' `replay`; modelli e connettori sono gia' dentro le immagini |

La garanzia complessiva e' **at-least-once** in lettura (Spark puo' rileggere un batch) piu' **sink idempotenti**:
il risultato finale e' equivalente a un *exactly-once*, cioe' ogni post compare una sola volta in Iceberg ed Elasticsearch.

---

## 6. Data Governance e Observability

**Qualita' del dato.** Ogni micro-batch produce un documento nell'indice `pipeline-metrics` con: righe in ingresso,
valide e scartate, distribuzione del sentiment, **latenza media** (tempo tra l'arrivo in Kafka e la fine
dell'elaborazione) e durata del batch. In Kibana si vede subito se la pipeline rallenta o se aumentano i record scartati.

**Contratti sullo schema.** Lo schema e' dichiarato in tre punti coerenti: il filtro di Logstash, lo `StructType` di
Spark e il mapping `strict` di Elasticsearch.

**Lineage con OpenMetadata.** Il progetto e' pensato per essere censito in OpenMetadata, che si avvia con il suo
docker compose ufficiale e si collega con i connettori nativi:

| Asset | Connettore OpenMetadata | Cosa raccoglie |
|---|---|---|
| Topic `mastodon-posts` | Kafka (messaging) | Topic, partizioni, schema dei messaggi |
| Tabella `lake.tap.posts` | Iceberg (database) | Colonne, tipi, partizioni, snapshot |
| Indice `mastodon-posts` | Elasticsearch (search) | Mapping dei campi |
| Job Spark | Pipeline + lineage | Collega topic → tabella e topic → indice |

Il **lineage** risultante e': `Mastodon API → Logstash → mastodon-posts (Kafka) → tap-sentiment-stream (Spark) →
lake.tap.posts (Iceberg) + mastodon-posts (Elasticsearch) → Streamlit/Kibana`. Su ogni asset si possono definire
owner, descrizione, tag (es. `PII` sul campo `author`) e test di qualita' (es. `sentiment` in un insieme di valori
ammessi, `id` non nullo e unico). Non ho incluso OpenMetadata nel compose principale perche' da solo richiede
diversi GB di RAM (server, MySQL, Elasticsearch e Airflow propri).

---

## 7. Scalabilita'

- **Kafka:** si aumentano le partizioni e i broker; il parallelismo di lettura di Spark segue il numero di partizioni.
- **Spark:** nel compose gira in `local[*]`, ma lo stesso codice gira senza modifiche su un cluster
  (standalone, YARN o **Kubernetes** con `spark-submit --master k8s://...`): ogni executor carica il proprio modello ONNX.
- **Elasticsearch:** piu' shard e repliche su piu' nodi.
- **Iceberg:** lo storage a oggetti (S3/MinIO) scala in modo indipendente dal calcolo; piu' motori (Spark, Trino,
  Flink) possono leggere la stessa tabella.
- **Kubernetes:** ogni servizio del compose corrisponde a un Deployment o StatefulSet; esistono operatori per
  Kafka (Strimzi), Spark (Spark Operator) ed Elasticsearch (ECK).

---

## 8. Test

La cartella `tests/` contiene 7 test automatici (pytest), tutti verdi, che girano offline in circa 20 secondi:

- **modelli:** con modelli ONNX "giocattolo" costruiti al volo (stessa interfaccia dei veri) verificano etichette,
  fascia neutral, ordine dei risultati con batch multipli, normalizzazione degli embedding e che il padding non
  cambi il risultato;
- **pipeline Spark:** pulizia dell'HTML, deduplica, record non validi, esecuzione delle Pandas UDF;
- **Iceberg:** scrivendo due volte lo stesso batch le righe non raddoppiano (idempotenza del `MERGE`) e
  dopo un `ALTER TABLE ADD COLUMN` le scritture continuano a funzionare;
- **streaming end-to-end:** uno stream reale (sorgente file al posto di Kafka) passa per watermark, deduplica e
  `foreachBatch`, e scrive su Iceberg 3 righe a partire da 7 messaggi (duplicati e un record non valido).

---

## 9. Limiti e sviluppi futuri

- Solo post in inglese: si puo' passare a un modello multilingue (es. XLM-RoBERTa per il sentiment e
  `paraphrase-multilingual-MiniLM-L12-v2` per gli embedding) per analizzare anche l'italiano.
- Il neutro e' ricavato da una soglia: un modello a 3 classi addestrato su tweet sarebbe piu' preciso.
- Un broker Kafka e un nodo Elasticsearch senza autenticazione: in produzione servono replica, TLS e utenti.
- Il RAG usa un LLM piccolo per girare su un portatile; un modello piu' grande darebbe risposte migliori.
- Possibili estensioni: allarmi quando il sentiment medio di un hashtag crolla, Schema Registry su Kafka,
  catalogo Iceberg REST con MinIO, deploy su Kubernetes.

---

## 10. Scaletta per la presentazione (10-12 minuti)

1. **Problema e idea** (1 min): sentiment in tempo reale + domande in linguaggio naturale.
2. **Architettura** (2 min): il diagramma, layer per layer, e il "viaggio di un post".
3. **Demo live** (4 min):
   - Kafka UI: topic, 3 partizioni, messaggi in arrivo;
   - `make logs`: un micro-batch con le sue metriche;
   - Streamlit: dashboard che si aggiorna, ricerca semantica (una frase senza parole in comune), domanda al RAG;
   - `make evolve`: snapshot, time travel e schema evolution su Iceberg.
4. **Scelte tecniche** (3 min): Kafka vs code, Pandas UDF + ONNX, Iceberg vs Parquet, idempotenza.
5. **Limiti e sviluppi** (1 min).

---

## Appendice: domande probabili e risposte brevi

**Perche' Kafka e non RabbitMQ?** RabbitMQ consegna e cancella: e' pensato per distribuire compiti.
Kafka e' un log persistente: ogni consumatore ha il suo offset, i dati si possono rileggere e piu' sistemi leggono lo
stesso flusso. Per una pipeline dati, dove servono replay e piu' consumatori, Kafka e' la scelta naturale.

**A cosa servono le partizioni e la chiave?** Le partizioni sono l'unita' di parallelismo e di ordinamento.
Con chiave = id del post, tutte le versioni dello stesso post vanno nella stessa partizione e restano in ordine.

**Cos'e' il watermark?** Una soglia sul tempo che dice a Spark quanto aspettare dati in ritardo. Serve per
eliminare lo stato vecchio: qui la deduplica ricorda gli id solo per 15 minuti.

**Pandas UDF vs UDF normale?** La UDF normale lavora riga per riga con serializzazione pickle; la Pandas UDF lavora su
blocchi colonnari trasferiti con Arrow e permette l'inferenza in batch. La variante a iteratore carica il modello una
volta per task.

**Perche' ONNX e non PyTorch?** Formato standard e portabile, runtime leggero e ottimizzato per l'inferenza su CPU,
modelli quantizzati piu' piccoli e veloci, immagine Docker senza PyTorch.

**Iceberg vs Parquet "nudo"?** Parquet e' un formato di file; Iceberg e' un formato di tabella sopra i file Parquet:
aggiunge transazioni ACID, snapshot e time travel, schema evolution sicura, partizionamento nascosto, MERGE/UPDATE/DELETE.

**Cos'e' un embedding e come funziona la ricerca vettoriale?** Un vettore che rappresenta il significato di un testo.
Testi simili hanno vettori vicini (coseno alto). Elasticsearch indicizza i vettori con HNSW e trova i k vicini
approssimati in tempi logaritmici invece di confrontare tutti i documenti.

**Classificazione vs embedding?** Vedi sezione 4: stessa architettura Transformer, ma la classificazione ha una testa
addestrata che produce una decisione, l'embedding restituisce una rappresentazione da confrontare con altre.

**Cosa succede se Spark va in crash?** Riparte dal checkpoint; i messaggi non elaborati sono ancora in Kafka; un batch
rieseguito non crea duplicati grazie a MERGE e agli id fissi in Elasticsearch.

**Come aggiungeresti la Data Governance?** Con OpenMetadata: connettori per Kafka, Iceberg ed Elasticsearch, lineage
del job Spark, owner e tag sui campi, test di qualita' (sezione 6).
