#!/usr/bin/env bash
# Sobe o pipeline do zero. Pré-requisitos: confluent CLI, psql, jq, envsubst e .env preenchido.
# ESQUELETO: confira cada comando com `confluent <grupo> --help` na sua versão do CLI.
set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || { echo "Falta .env (copie de .env.example)"; exit 1; }
set -a; source .env; set +a
mkdir -p .state

echo "==> Camada 1: fundação"
confluent login --save   # usa CONFLUENT_CLOUD_EMAIL/PASSWORD do ambiente
confluent environment create "$ENV_NAME" -o json | tee .state/env.json
ENV_ID=$(jq -r .id .state/env.json)
confluent environment use "$ENV_ID"

confluent kafka cluster create "$CLUSTER_NAME" --cloud "$CLOUD" --region "$REGION" --type basic -o json | tee .state/cluster.json
CLUSTER_ID=$(jq -r .id .state/cluster.json)
confluent kafka cluster use "$CLUSTER_ID"

confluent iam service-account create desafio-producer --description "CDC / escrita" -o json | tee .state/sa-producer.json
confluent iam service-account create desafio-consumer --description "Leitura" -o json | tee .state/sa-consumer.json
PRODUCER_SA_ID=$(jq -r .id .state/sa-producer.json)
CONSUMER_SA_ID=$(jq -r .id .state/sa-consumer.json)
export PRODUCER_SA_ID

# ACLs de menor privilégio (prefixo desafio-/desafio.*)
confluent kafka acl create --allow --service-account "$PRODUCER_SA_ID" --operations write,describe,create --topic "$TOPIC_PREFIX" --prefix
confluent kafka acl create --allow --service-account "$CONSUMER_SA_ID" --operations read,describe --topic "$TOPIC_PREFIX" --prefix
confluent kafka acl create --allow --service-account "$CONSUMER_SA_ID" --operations read --consumer-group "desafio-" --prefix
confluent iam service-account list
confluent kafka acl list

echo "==> Camada 2: contrato (Schema Registry)"
confluent schema-registry cluster enable --cloud "$CLOUD" --geo us || true
for s in transaction account fraud_alert; do
  echo "Registrando $s"   # registre cada schema no subject do tópico correspondente
done
# Ajuste os subjects ao nome real dos tópicos, p.ex.:
# confluent schema-registry schema create --subject desafio.fraud.detected-value --schema schemas/fraud_alert.avsc --type avro
# confluent schema-registry subject update <subject> --compatibility BACKWARD
# confluent schema-registry schema compatibility validate --subject <subject> --schema schemas/evolution/transaction_add_field.avsc --type avro      # esperado: compatível
# confluent schema-registry schema compatibility validate --subject <subject> --schema schemas/evolution/transaction_remove_required.avsc --type avro  # esperado: incompatível

echo "==> Camada 3: ingestão (CDC)"
PGPASSWORD="$PG_ADMIN_PASSWORD" psql "host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_ADMIN_USER sslmode=require" \
  -v cdc_password="$PG_CDC_PASSWORD" -f db/01_cdc_user.sql
PGPASSWORD="$PG_ADMIN_PASSWORD" psql "host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_ADMIN_USER sslmode=require" \
  -f db/02_seed_check.sql

envsubst < connectors/cdc.json > .state/cdc.rendered.json   # .state/ está no .gitignore
confluent connect cluster create --config-file .state/cdc.rendered.json
confluent connect cluster list

echo "==> Camada 4: Flink SQL"
confluent flink compute-pool create "$POOL_NAME" --cloud "$CLOUD" --region "$REGION" --max-cfu 10
# Execute os statements em ordem:
#   sql/01_enrich_temporal_join.sql
#   sql/02_rule_velocity.sql
# (confluent flink statement create ... ou pelo shell: confluent flink shell)

echo "Pipeline no ar. Colete as evidências e preencha o README."