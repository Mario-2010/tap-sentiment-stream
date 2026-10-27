#!/bin/sh
# Riproduce in loop i post di esempio come se arrivassero in tempo reale:
# a ogni riga assegna un id univoco e il timestamp corrente.
OUT=/stream/stream.jsonl
: > "$OUT"
i=0
while true; do
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    i=$((i + 1))
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    echo "$line" | sed -e "s/__ID__/replay-$(date +%s)-$i/" -e "s/__NOW__/$now/" >> "$OUT"
    sleep "${REPLAY_DELAY:-2}"
  done < /data/sample_posts.jsonl
done
