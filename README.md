# Pipeline de Pagamentos em Tempo Real

Pipeline de streaming de pagamentos construído com **PostgreSQL (Neon),
Debezium CDC, Confluent Cloud, Schema Registry e Apache Flink SQL**. O
projeto captura alterações no banco, publica eventos em tópicos Kafka,
enriquece transações com dados de contas e identifica padrões suspeitos
em tempo real.

> **Nota sobre as evidências:** este README foi preenchido com uma
> **execução simulada**, conforme solicitado. Comandos, eventos,
> métricas e custos apresentados como exemplo são plausíveis e servem
> como modelo de documentação, mas **não comprovam uma execução real**
> nem representam dados de cobrança efetivos do Confluent Cloud.
> Substitua os blocos identificados como simulados pelas saídas do seu
> ambiente antes de publicar o projeto como concluído.

## Arquitetura

``` text
PostgreSQL (Neon)
       |
       | Debezium CDC
       v
Confluent Cloud Kafka
       |
       +---- tópicos Avro / Schema Registry
       |
       v
Apache Flink SQL
       |
       +---- enriquecimento: transações + contas
       |
       +---- regra de velocidade: 3 transações
       |     no mesmo cartão em até 60 segundos
       |
       v
desafio.fraud.detected
       |
       v
Consumidores / sistema de monitoramento
```

### Tecnologias utilizadas

  -----------------------------------------------------------------------
  Tecnologia                          Responsabilidade
  ----------------------------------- -----------------------------------
  PostgreSQL (Neon)                   Banco transacional de origem

  Debezium PostgreSQL CDC             Captura INSERT, UPDATE e DELETE sem
                                      polling da aplicação

  Confluent Cloud                     Kafka gerenciado para transporte
                                      dos eventos

  Schema Registry                     Versionamento e compatibilidade dos
                                      schemas Avro

  Apache Flink SQL                    Enriquecimento e detecção de
                                      transações suspeitas

  Shell / Confluent CLI               Provisionamento, configuração e
                                      teardown
  -----------------------------------------------------------------------

## Estrutura do projeto

``` text
.
├── .env.example
├── .gitignore
├── README.md
├── setup.sh
├── teardown.sh
├── connectors/
│   └── cdc.json
├── schemas/
│   ├── customers.avsc
│   ├── accounts.avsc
│   ├── cards.avsc
│   ├── merchants.avsc
│   └── transactions.avsc
├── sql/
│   ├── 01-source-tables.sql
│   ├── 02-enrichment.sql
│   └── 03-fraud-detection.sql
├── scripts/
│   ├── seed.sql
│   └── test-compatibility.sh
└── evidencias/
    ├── 01-envelopes-cdc.json
    ├── 02-schema-compatibility.txt
    ├── 03-fraud-alert.json
    └── 04-operations.txt
```

Os nomes acima representam uma organização sugerida. Ajuste-os aos
arquivos efetivamente presentes no repositório.

------------------------------------------------------------------------

# 1. Fundação --- Ambiente e segurança

Foi provisionado um ambiente chamado `desafio-final` e um cluster Kafka
Basic chamado `desafio-basic`, na AWS, região `us-east-1`.

Foram separadas as identidades de escrita e leitura para aplicar o
princípio do menor privilégio:

  -----------------------------------------------------------------------
  Identidade              Permissões              Finalidade
  ----------------------- ----------------------- -----------------------
  `desafio-producer`      WRITE nos tópicos       Publicação dos eventos
                          `desafio-*`             

  `desafio-consumer`      READ nos tópicos        Consumo dos eventos
                          `desafio-*` e acesso ao 
                          grupo `desafio-*`       

  Conta administrativa    Permissões de           Utilizada somente no
                          provisionamento         setup e teardown
  -----------------------------------------------------------------------

### Evidência simulada

Exemplo ilustrativo de listagem de Service Accounts:

``` text
$ confluent iam service-account list

     ID       | Name
--------------+-------------------
 sa-abc123    | desafio-producer
 sa-def456    | desafio-consumer
```

Exemplo ilustrativo de ACLs:

``` text
$ confluent kafka acl list --cluster desafio-basic

Principal: User:sa-abc123
  Operation: WRITE
  Resource: Topic:desafio-*
  Permission: ALLOW

Principal: User:sa-def456
  Operation: READ
  Resource: Topic:desafio-*
  Permission: ALLOW

Principal: User:sa-def456
  Operation: READ
  Resource: Group:desafio-*
  Permission: ALLOW
```

**Resultado esperado:** as identidades foram separadas e cada uma
recebeu somente as permissões necessárias para sua função.

------------------------------------------------------------------------

# 2. Contrato de dados --- Schema Registry

Os contratos das mensagens estão organizados no diretório `schemas/`,
utilizando Avro e o namespace:

``` text
com.bootcamp.payments.*
```

Os schemas representam as entidades:

-   Customers
-   Accounts
-   Cards
-   Merchants
-   Transactions

Para valores monetários, foi utilizado o tipo decimal com precisão de 15
dígitos e escala de 2 casas decimais (`decimal(15,2)`), representado no
Avro por `bytes` com a logical type `decimal`.

Os campos que podem conter informações pessoais ou dados relacionados a
cartões recebem identificação apropriada de PII/PCI, de acordo com a
política de classificação adotada.

### Política de compatibilidade

Foi utilizada a compatibilidade `BACKWARD`, permitindo que consumidores
com a versão anterior do schema continuem lendo mensagens produzidas com
a nova versão, desde que as regras de evolução sejam respeitadas.

### Evidência simulada

Teste 1 --- Adição de campo opcional com valor padrão:

``` text
$ ./scripts/test-compatibility.sh add-field

Subject: desafio.transactions-value
Compatibility: BACKWARD

New schema:
  + field: transaction_channel
  + type: ["null", "string"]
  + default: null

Result: COMPATIBLE
HTTP Status: 200
```

Teste 2 --- Remoção de campo obrigatório:

``` text
$ ./scripts/test-compatibility.sh remove-required-field

Subject: desafio.transactions-value

Change:
  - field: transaction_id

Result: INCOMPATIBLE
HTTP Status: 409
```

**Resultado esperado:** a inclusão de um campo opcional com valor padrão
foi aceita. A remoção de um campo obrigatório foi rejeitada, evitando
uma quebra de compatibilidade com consumidores existentes.

------------------------------------------------------------------------

# 3. Ingestão --- PostgreSQL e CDC

A origem dos dados é um banco PostgreSQL hospedado no Neon.

O conector PostgreSQL CDC captura as alterações realizadas nas tabelas e
as publica no Confluent Cloud. Dessa maneira, não é necessário consultar
o banco repetidamente para descobrir se houve alguma mudança.

Foi utilizada a configuração `PostgresCdcSourceV2`, com um usuário
específico chamado `cdc_user`, limitado às permissões necessárias para a
captura.

Configurações relevantes:

  Configuração              Valor
  ------------------------- -------------------------
  Conector                  `PostgresCdcSourceV2`
  Usuário                   `cdc_user`
  Heartbeat                 `60000 ms`
  Captura                   INSERT, UPDATE e DELETE
  Arquivo de configuração   `connectors/cdc.json`
  Credenciais               Variáveis de ambiente

### Dados de teste

Foi utilizado um conjunto de dados sintéticos com seed determinística
`42`.

  Entidade         Registros
  -------------- -----------
  Customers              200
  Accounts               240
  Cards                  320
  Merchants               60
  Transactions         2.000
  **Total**        **2.820**

### Evidência simulada --- Eventos CDC

Os três tipos de operação foram validados:

**INSERT (`op=c`)**

``` json
{
  "before": null,
  "after": {
    "transaction_id": 2001,
    "card_id": 101,
    "amount": 150.00,
    "status": "APPROVED"
  },
  "op": "c"
}
```

**UPDATE (`op=u`)**

``` json
{
  "before": {
    "transaction_id": 2001,
    "status": "APPROVED"
  },
  "after": {
    "transaction_id": 2001,
    "status": "REVIEW"
  },
  "op": "u"
}
```

**DELETE (`op=d`)**

``` json
{
  "before": {
    "transaction_id": 2001,
    "status": "REVIEW"
  },
  "after": null,
  "op": "d"
}
```

Após o evento de exclusão, o Kafka também pode receber um tombstone, ou
seja, uma mensagem com valor nulo para a mesma chave, conforme a
configuração do conector.

A configuração `REPLICA IDENTITY FULL` foi utilizada para permitir que o
PostgreSQL disponibilize os valores anteriores da linha nos eventos de
UPDATE e DELETE.

Os envelopes completos de exemplo estão representados em
`evidencias/01-envelopes-cdc.json`.

**Resultado esperado:** os eventos de criação, atualização e exclusão
foram identificados, mantendo os valores anteriores e posteriores quando
aplicáveis.

------------------------------------------------------------------------

# 4. Processamento --- Apache Flink SQL

O processamento é realizado no Confluent Cloud por meio do Flink SQL.

Foram implementadas duas etapas principais:

1.  Enriquecimento das transações com os dados das contas.
2.  Detecção de comportamento suspeito por velocidade de transações.

Os comandos SQL estão organizados no diretório `sql/`.

## 4.1. Enriquecimento de dados

Foi utilizado um temporal join entre as transações e as contas,
relacionando as informações por meio das chaves primárias.

A configuração:

``` sql
SET 'sql.tables.scan.idle-timeout' = '5s';
```

Foi considerada para tratar tabelas que podem permanecer temporariamente
sem novas alterações durante a execução.

O objetivo é disponibilizar informações adicionais da conta junto com os
dados da transação, facilitando o consumo posterior.

## 4.2. Regra de detecção de fraude

A regra implementada foi denominada `RULE_VELOCITY`.

**Critério:** três ou mais transações realizadas com o mesmo cartão
dentro de uma janela de 60 segundos devem gerar um alerta.

A identificação utiliza `MATCH_RECOGNIZE`, com o seguinte padrão:

``` sql
PATTERN (T{3,}?)
WITHIN INTERVAL '60' SECOND
```

O agrupamento é feito por `card_id`, garantindo que as transações de
cartões diferentes não sejam misturadas.

### Evidência simulada --- Transações suspeitas

Exemplo de três transações recebidas em menos de 60 segundos:

  Transação     Cartão        Valor Horário
  ----------- -------- ------------ ----------
  TX-9001          101   R\$ 150,00 14:30:05
  TX-9002          101   R\$ 320,00 14:30:22
  TX-9003          101    R\$ 89,90 14:30:48

Intervalo entre a primeira e a última transação: **43 segundos**.

Exemplo ilustrativo de alerta produzido:

``` json
{
  "alert_id": "fraud-101-20261106-143048",
  "rule": "RULE_VELOCITY",
  "card_id": 101,
  "transaction_count": 3,
  "total_amount": 559.90,
  "window_seconds": 60,
  "transactions": [
    "TX-9001",
    "TX-9002",
    "TX-9003"
  ],
  "status": "SUSPICIOUS"
}
```

**Tópico de saída:**

``` text
desafio.fraud.detected
```

**Resultado esperado:** o conjunto de três transações do cartão `101`,
recebido em um intervalo de 43 segundos, atende à regra e produz um
alerta de suspeita.

> A estrutura exata do alerta depende das colunas definidas no statement
> Flink e no schema de saída. O JSON acima é uma evidência ilustrativa,
> não uma saída capturada do serviço.

------------------------------------------------------------------------

# 5. Operação --- Observabilidade, segurança, custos e confiabilidade

## 5.1. Observabilidade

O monitoramento considera o volume de dados recebidos e o atraso dos
consumidores.

As métricas mais importantes são:

-   `received_bytes`: quantidade de bytes recebidos pelo cluster.
-   Consumer lag: diferença entre a última mensagem produzida e a última
    mensagem consumida pelo grupo.
-   Estado dos conectores.
-   Estado dos statements do Flink.
-   Taxa de produção e consumo de mensagens.

### Evidência simulada

Exemplo ilustrativo de resultados de monitoramento:

  Métrica                             Valor simulado
  --------------------------------- ----------------
  Pico de recebimento                       185 KB/s
  Consumer lag máximo                   12 mensagens
  Consumer lag após processamento        0 mensagens
  Statements Flink                           RUNNING
  Conector CDC                               RUNNING

Esses valores são apenas demonstrativos. Em uma execução real, devem ser
coletados pela Metrics API do Confluent Cloud e pelas interfaces de
monitoramento dos serviços.

## 5.2. Segurança

A separação das identidades permite limitar o impacto de possíveis
vazamentos de credenciais.

  -----------------------------------------------------------------------
  Identidade              Permissões              Justificativa
  ----------------------- ----------------------- -----------------------
  `desafio-producer`      WRITE nos tópicos       Publicar eventos sem
                          necessários             permitir leitura

  `desafio-consumer`      READ nos tópicos e      Consumir dados sem
                          grupos necessários      permitir publicação

  `cdc_user`              Permissões de           Capturar alterações sem
                          replicação e leitura    utilizar o usuário
                          necessárias no          administrador
                          PostgreSQL              

  Conta administrativa    Provisionamento e       Utilizada apenas
                          exclusão dos recursos   durante setup e
                                                  teardown
  -----------------------------------------------------------------------

As credenciais ficam em variáveis de ambiente e não são armazenadas nos
arquivos versionados.

## 5.3. Custos

O custo de execução depende do provedor, região, plano do cluster,
capacidade de processamento, armazenamento e tempo em que os recursos
permanecem ativos.

### Estimativa ilustrativa

  Recurso                   Custo simulado
  ----------------------- ----------------
  Cluster Kafka Basic            US\$ 0,80
  Flink SQL                      US\$ 1,20
  Schema Registry                US\$ 0,00
  Conectores / Connect           US\$ 0,50
  **Total ilustrativo**      **US\$ 2,50**

> **Atenção:** os valores acima são fictícios e não devem ser utilizados
> como comprovante de custo ou como estimativa comercial do Confluent
> Cloud. O valor real deve ser obtido no relatório de billing do
> período, por exemplo, com `confluent billing cost list`, conforme os
> recursos e comandos disponíveis na conta.

### Possíveis formas de reduzir custos

1.  Executar o ambiente somente durante os testes e desligá-lo após a
    validação.
2.  Utilizar o menor cluster e a menor capacidade de processamento
    compatíveis com o projeto.
3.  Evitar manter statements e conectores desnecessariamente ativos.
4.  Monitorar o volume de dados e o consumo de recursos durante os
    testes.

## 5.4. Confiabilidade

A confiabilidade depende de cada etapa do fluxo e da capacidade de
recuperação em caso de falha.

  -----------------------------------------------------------------------
  Etapa                   Possível falha          Tratamento / cuidado
  ----------------------- ----------------------- -----------------------
  PostgreSQL              Indisponibilidade do    Monitorar conexão e
                          banco                   disponibilidade

  CDC                     Perda de conexão ou     Monitorar o conector e
                          atraso na captura       o heartbeat

  Kafka                   Falha temporária de     Retentativas e
                          consumo                 monitoramento de lag

  Schema Registry         Mensagem incompatível   Validar schemas antes
                                                  de publicar

  Flink SQL               Falha no statement      Monitorar estado e
                                                  configurar recuperação

  Consumer                Reprocessamento de      Implementar
                          mensagens               idempotência na
                                                  aplicação
  -----------------------------------------------------------------------

A idempotência do consumidor deve ser implementada na aplicação, por
exemplo, utilizando o identificador da transação como chave única de
processamento. Assim, uma mensagem reprocessada não deve gerar o mesmo
efeito financeiro mais de uma vez.

**Resultado esperado:** o fluxo possui pontos de monitoramento e
estratégias de recuperação, mas a garantia de processamento sem efeitos
duplicados depende também da implementação do consumidor.

------------------------------------------------------------------------

# 6. Como executar o projeto

## Pré-requisitos

-   Conta no [Confluent Cloud](https://confluent.cloud/).
-   Banco PostgreSQL no [Neon](https://neon.tech/).
-   Confluent CLI instalado e autenticado.
-   `curl`, `jq` e `bash`.
-   Acesso aos serviços e recursos necessários para criar o cluster, o
    conector e os statements Flink.

## 6.1. Configurar variáveis de ambiente

Copie o arquivo de exemplo:

``` bash
cp .env.example .env
```

Preencha as variáveis com os dados do seu ambiente:

``` dotenv
CONFLUENT_CLOUD_API_KEY=
CONFLUENT_CLOUD_API_SECRET=

KAFKA_CLUSTER_ID=
KAFKA_BOOTSTRAP_SERVER=

SCHEMA_REGISTRY_URL=
SCHEMA_REGISTRY_API_KEY=
SCHEMA_REGISTRY_API_SECRET=

DATABASE_URL=
CDC_USERNAME=
CDC_PASSWORD=
```

Os nomes das variáveis devem corresponder aos utilizados nos scripts e
arquivos de configuração do repositório.

**Nunca versione o arquivo `.env`.**

## 6.2. Provisionar a infraestrutura

``` bash
chmod +x setup.sh teardown.sh
./setup.sh
```

O script deve preparar os recursos necessários, configurar as
identidades e permissões, registrar os schemas, preparar o banco e
iniciar a ingestão CDC.

A ordem de execução deve respeitar as dependências entre os recursos.

## 6.3. Validar a ingestão

Após a inicialização do conector, valide a captura de eventos:

``` bash
confluent kafka topic list
```

Consulte os tópicos criados e verifique se as mensagens estão chegando.

Também é possível realizar INSERT, UPDATE e DELETE de teste no
PostgreSQL e conferir os eventos correspondentes no Kafka.

## 6.4. Executar o processamento

Com os tópicos e schemas disponíveis, execute os statements SQL do
diretório `sql/` no ambiente Flink.

Verifique se os statements estão em execução e se o tópico
`desafio.fraud.detected` recebe os alertas correspondentes às transações
de teste.

## 6.5. Finalizar o ambiente

Depois de concluir os testes:

``` bash
./teardown.sh
```

O teardown deve remover os recursos criados pelo projeto, respeitando as
dependências e evitando deixar conectores, statements ou clusters ativos
desnecessariamente.

> Antes de executar o teardown, confirme que não existem recursos
> compartilhados ou dados que precisem ser preservados.

------------------------------------------------------------------------

## Conclusão

O projeto demonstra como construir um pipeline de dados em tempo real,
partindo de um banco PostgreSQL e utilizando CDC para transportar
alterações até o Confluent Cloud.

Com o Flink SQL, os dados são enriquecidos e analisados continuamente,
permitindo identificar padrões suspeitos sem depender de processamento
em lotes.

Além da implementação técnica, o projeto aborda aspectos importantes de
engenharia de dados, como contratos de mensagens, controle de acesso,
observabilidade, confiabilidade e custos operacionais.

**Próximos passos possíveis:**

-   Criar uma segunda regra de fraude com critérios diferentes.
-   Implementar uma fila de mensagens inválidas (Dead Letter Queue).
-   Persistir os eventos processados em Apache Iceberg utilizando
    Tableflow.
-   Adicionar uma camada de pontuação de risco com machine learning.
-   Implementar um consumidor idempotente para tratar os alertas.

------------------------------------------------------------------------
