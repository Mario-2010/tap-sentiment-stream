.PHONY: up replay extra llm demo evolve logs ps down clean test

up:          ## Pipeline con dati live da Mastodon
	SOURCE=mastodon docker compose up -d --build

replay:      ## Pipeline con i post di esempio (offline)
	SOURCE=replay docker compose --profile replay up -d --build

extra:       ## Aggiunge Kibana e Kafka UI (circa 2 GB di RAM in piu')
	docker compose --profile extra up -d

llm:         ## Aggiunge Ollama e scarica il modello per il RAG
	docker compose --profile llm up -d ollama
	docker compose exec ollama ollama pull $${OLLAMA_MODEL:-qwen2.5:1.5b}

demo:        ## Demo Iceberg: analytics, snapshot, time travel
	docker compose exec spark /opt/spark/bin/spark-submit /opt/app/iceberg_demo.py

evolve:      ## Demo Iceberg con schema evolution
	docker compose exec spark /opt/spark/bin/spark-submit /opt/app/iceberg_demo.py --evolve

logs:
	docker compose logs -f spark logstash

ps:
	docker compose ps

down:
	docker compose --profile replay --profile extra --profile llm down

clean:       ## Ferma tutto e cancella i dati (volumi)
	docker compose --profile replay --profile extra --profile llm down -v

test:        ## Test locali (serve Java 17 o 21 + pip install -r tests/requirements.txt)
	python -m pytest -q tests
