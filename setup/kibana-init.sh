#!/bin/sh
# Attende Kibana e crea le data view per i due indici.
KB=http://kibana:5601
until curl -s "$KB/api/status" | grep -q '"level":"available"'; do
  echo "Attendo Kibana..."; sleep 5
done
# $1 = nome dell'indice, $2 = nome da mostrare, $3 = campo con la data
create_view() {
  curl -s -X POST "$KB/api/data_views/data_view" \
    -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
    -d "{\"override\": true, \"data_view\": {\"id\": \"$1\", \"title\": \"$1\", \"name\": \"$2\", \"timeFieldName\": \"$3\"}}"
  echo
}
create_view mastodon-posts "Mastodon posts" created_at
create_view pipeline-metrics "Pipeline metrics" processed_at