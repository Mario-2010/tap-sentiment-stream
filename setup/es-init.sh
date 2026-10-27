#!/bin/sh
# Crea gli indici Elasticsearch con mapping esplicito (dense_vector per la ricerca semantica).
ES=http://elasticsearch:9200
until curl -s "$ES/_cluster/health" | grep -Eq '"status":"(green|yellow)"'; do
  echo "Attendo Elasticsearch..."; sleep 5
done
for idx in mastodon-posts pipeline-metrics; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$ES/$idx")
  if [ "$code" = "404" ]; then
    echo "Creo l'indice $idx"
    curl -s -f -X PUT "$ES/$idx" -H 'Content-Type: application/json' \
      --data-binary "@/setup/$idx-mapping.json" || exit 1
    echo
  else
    echo "Indice $idx gia' presente"
  fi
done
