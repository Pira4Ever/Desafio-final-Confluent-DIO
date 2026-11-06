#!/usr/bin/env bash
# Derruba tudo. Rode ao final para parar o custo. Confira as listas vazias no fim.
set -uo pipefail
cd "$(dirname "$0")"
set -a; source .env; set +a

ENV_ID=$(jq -r .id .state/env.json 2>/dev/null || true)
[ -n "${ENV_ID:-}" ] || { echo "Sem .state/env.json: nada para derrubar (ou use confluent environment list)."; exit 1; }
confluent environment use "$ENV_ID"

echo "==> Statements Flink"
for id in $(confluent flink statement list -o json | jq -r '.[].name'); do
  confluent flink statement delete "$id" --force
done

echo "==> Compute pool"
for id in $(confluent flink compute-pool list -o json | jq -r '.[].id'); do
  confluent flink compute-pool delete "$id" --force
done

echo "==> Conectores"
for id in $(confluent connect cluster list -o json | jq -r '.[].id'); do
  confluent connect cluster delete "$id" --force
done

echo "==> Environment (remove cluster, Schema Registry, tópicos, SAs de dados)"
confluent environment delete "$ENV_ID" --force

echo "==> Service accounts"
for f in .state/sa-producer.json .state/sa-consumer.json; do
  [ -f "$f" ] && confluent iam service-account delete "$(jq -r .id "$f")" --force
done

echo "==> Slot de replicação no Postgres (evita reter WAL no Neon)"
PGPASSWORD="$PG_ADMIN_PASSWORD" psql "host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_ADMIN_USER sslmode=require" \
  -c "SELECT pg_drop_replication_slot('desafio_slot') FROM pg_replication_slots WHERE slot_name='desafio_slot';"

echo "==> Verificação (esperado: listas vazias)"
confluent environment list
confluent iam service-account list