-- Camada 4b — RULE_VELOCITY: 3+ transações no mesmo cartão em menos de 60 segundos.
-- Saída no tópico desafio.fraud.detected (schema: schemas/fraud_alert.avsc).
-- MATCH_RECOGNIZE exige ordenação por atributo de tempo crescente; ajuste a coluna
-- de tempo e o watermark conforme a tabela criada pelo seu conector.

CREATE TABLE IF NOT EXISTS `desafio.fraud.detected` (
  card_id      STRING,
  rule         STRING,
  tx_count     BIGINT,
  total_amount DECIMAL(15, 2),
  first_tx_at  TIMESTAMP_LTZ(3),
  last_tx_at   TIMESTAMP_LTZ(3)
);

INSERT INTO `desafio.fraud.detected`
SELECT card_id, rule, tx_count, total_amount, first_tx_at, last_tx_at
FROM `desafio.enriched.transactions`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY created_at
  MEASURES
    'RULE_VELOCITY'      AS rule,
    COUNT(T.transaction_id) AS tx_count,
    SUM(T.amount)        AS total_amount,
    FIRST(T.created_at)  AS first_tx_at,
    LAST(T.created_at)   AS last_tx_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS true
);