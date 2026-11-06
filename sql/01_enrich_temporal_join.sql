-- Camada 4a — Enriquecimento: transações ⋈ contas (temporal join).
-- Pré-requisito: tabelas Flink derivadas dos tópicos CDC, com PRIMARY KEY nos dois lados
-- (o tópico de contas precisa ser um changelog upsert para o temporal join).
-- Ajuste os nomes dos tópicos/colunas ao que o conector gerou (confira com `SHOW TABLES`).

SET 'sql.tables.scan.idle-timeout' = '5s';

CREATE TABLE IF NOT EXISTS `desafio.enriched.transactions` (
  transaction_id STRING,
  card_id        STRING,
  account_id     STRING,
  merchant_id    STRING,
  amount         DECIMAL(15, 2),
  currency       STRING,
  created_at     TIMESTAMP_LTZ(3),
  customer_id    STRING,
  risk_tier      STRING,
  credit_limit   DECIMAL(15, 2),
  PRIMARY KEY (transaction_id) NOT ENFORCED
);

INSERT INTO `desafio.enriched.transactions`
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.amount,
  t.currency,
  t.created_at,
  a.customer_id,
  a.risk_tier,
  a.credit_limit
FROM `desafio.public.transactions` AS t
JOIN `desafio.public.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.account_id = a.account_id;