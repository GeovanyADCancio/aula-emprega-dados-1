# Aula: Camada Silver com PySpark — Rede Hospitalar

## Arquivos
- `gerar_dados_saude.py` — roda **local** (`python3 gerar_dados_saude.py`), gera os 4 CSVs em `./dados_saude/`.
  Escala default: 150k pacientes, 600 médicos, 250 procedimentos, 2,5M atendimentos.
  Ajuste `N_ENCOUNTERS` no topo do script se quiser algo menor para testar rápido primeiro.
- `01_Bronze_para_Silver_Limpeza.py` — notebook Databricks (formato *source*): ingestão,
  regex, parsing de datas/custo, padronização categórica, deduplicação, split OK/quarentena.
- `02_Silver_Joins_Broadcast_Explain.py` — notebook Databricks: inner/left join, broadcast
  join, `explain()`, skew (AQE + salting manual), window functions, escrita particionada.

## Passo a passo
1. `pip install pandas numpy faker`
2. `python3 gerar_dados_saude.py`
3. No Databricks Free Edition: crie um Volume (`Catalog > workspace > default > Create Volume`,
   nome `dados_saude`) e faça upload dos 4 CSVs para lá.
4. Importe os dois `.py` como notebooks: **Workspace > Import > File** (o Databricks reconhece
   o cabeçalho `# Databricks notebook source` e recria as células automaticamente).
5. Rode o notebook 1 até o fim, depois o notebook 2.

## Sujeiras de dados injetadas (gabarito rápido)
- CPF nulo/inválido, telefone e CRM em formatos variados
- Datas em 3+ formatos distintos (inclusive data futura, erro de digitação)
- Pacientes duplicados (recadastro, mesmo CPF)
- FKs órfãs e nulas em `atendimentos` (patient_id, doctor_id, procedure_code)
- IDs "sujos" tipo `" 123"`, `"123.0"`, zero-padding — clássico de export de planilha
- Custo em dois formatos (`"1234.56"` vs `"R$ 1.234,56"`), negativo e outlier
- `encounter_id` duplicado (reprocessamento)
- CID-10, telefone e e-mail embutidos em texto livre (para regex)
- Um médico concentra ~15% dos atendimentos (skew proposital)
