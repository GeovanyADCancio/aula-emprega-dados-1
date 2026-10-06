# Aula: Introdução a Streaming de Dados com Kafka

## Arquivos
- `docker-compose.yml` — sobe Zookeeper + Kafka (Postgres/Kafka Connect só
  entram se vocês também forem revisar a parte de CDC/Debezium da aula
  passada; não são necessários pra este exercício de tópico simples).
- `produtor_transacoes.py` — gera transações de pagamento simuladas e
  publica no tópico `transacoes.pagamentos`.
- `consumidor_a_fraude.py` — consumer group `deteccao-fraude`, lê a
  partir de **agora** (`auto_offset_reset=latest`), alerta valor alto ou
  transação negada.
- `consumidor_b_persistencia.py` — consumer group `persistencia-datalake`,
  lê **desde o início** (`auto_offset_reset=earliest`), grava tudo em
  `datalake_transacoes.jsonl` (simula persistência num data lake).

## Pré-requisitos
```bash
docker compose up -d
pip install kafka-python
```
> Se `kafka-python` der erro de import em Python 3.12+ (problema conhecido
> da lib, não mantida com a mesma frequência), use Python 3.10/3.11 nessa
> aula, ou troque por `pip install kafka-python-ng` (fork compatível, API idêntica).

O tópico `transacoes.pagamentos` não precisa ser criado manualmente — as
imagens da Confluent criam o tópico automaticamente no primeiro `send()`.

## Como rodar (3 terminais)
```bash
# Terminal 1
python3 produtor_transacoes.py

# Terminal 2
python3 consumidor_a_fraude.py

# Terminal 3
python3 consumidor_b_persistencia.py
```
Ordem não importa muito — pode subir os consumidores antes ou depois do
produtor. O importante é os três rodando ao mesmo tempo, lado a lado na
tela, pra turma ver o fluxo em paralelo.

## Roteiro de conceitos pra explicar em aula
1. **Tópico** — `transacoes.pagamentos` é o canal; produtor escreve, sem
   saber quem (ou quantos) vai ler.
2. **Mesma mensagem, dois destinos** — os dois consumidores recebem
   **todas** as transações, cada um do seu jeito, porque estão em
   **consumer groups diferentes** (`deteccao-fraude` vs
   `persistencia-datalake`). Se estivessem no mesmo grupo, o Kafka
   dividiria as mensagens entre eles em vez de duplicar.
3. **Offset: `latest` vs `earliest`** — pare o Consumidor A, deixe o
   produtor rodando uns segundos, religue o A: ele **não** vê o que
   perdeu (`latest`). Faça o mesmo com o B: ele reprocessa tudo desde o
   início (`earliest`), inclusive o que já tinha lido antes — é assim que
   fica claro que offset é por *consumer group*, não por tópico.
4. **Desacoplamento** — pare o produtor: os consumidores continuam
   rodando (só param de receber mensagem nova). Pare os dois
   consumidores: o produtor nem percebe, continua publicando — cada peça
   não sabe da existência da outra, só do tópico.
5. **Persistência real** — abra `datalake_transacoes.jsonl` num editor
   enquanto o Consumidor B roda, pra mostrar o arquivo crescendo linha a
   linha em tempo real.