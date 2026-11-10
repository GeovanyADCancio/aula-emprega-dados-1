# Especificação Funcional — Confiabilidade da Frota

| Item | Valor |
|---|---|
| Empresa | TransVale Mobilidade (fictícia), operadora de ônibus urbanos |
| Solicitante | Engenharia de Manutenção |
| Analista | Renata Albuquerque |
| Produto | `gold.vw_confiabilidade_frota_mes` |

---

## 1. Objetivo

A prefeitura exige que cada ônibus fique **disponível pelo menos 95% do tempo**. Precisamos de uma tabela que mostre, **para cada ônibus e cada mês**:

- quantas vezes ele quebrou;
- quantas horas ficou parado;
- qual foi a disponibilidade (%);
- o tempo médio para consertar (MTTR) e o tempo médio entre quebras (MTBF).

## 2. Período e data de referência

- Meses de **janeiro a outubro de 2026**.
- **Data de referência: 31/10/2026.** O resultado deve mostrar a situação **ao final desse dia**. Os dados foram extraídos em 06/11, então existem registros de novembro que devem ser **ignorados**.

## 3. Tabelas

Todos os campos chegam como texto. Datas vêm como `AAAAMMDD` (ex.: `20260905`) e horas como `HHMMSS` (ex.: `080000`).

**equipamento** — cadastro de equipamentos
| Campo | Significado |
|---|---|
| EQUNR | Código do equipamento |
| EQTYP | Tipo: `O` = ônibus, `F` = equipamento de garagem |
| HERST | Fabricante |
| TYPBZ | Modelo |
| INBDT | Data em que entrou em operação |
| SWERK | Código da garagem |

**equipamento_descricao** — nome do equipamento
| Campo | Significado |
|---|---|
| EQUNR | Código do equipamento |
| SPRAS | Idioma: `P` = português, `E` = inglês |
| DESCR | Descrição |

**garagem**
| Campo | Significado |
|---|---|
| WERKS | Código da garagem |
| NAME1 | Nome da garagem |

**nota_manutencao** — registros de problemas nos equipamentos
| Campo | Significado |
|---|---|
| QMNUM | Número da nota |
| QMART | Tipo: `M2` = avaria (quebra), `M1` = solicitação de serviço |
| EQUNR | Equipamento |
| AUSVN / AUZTV | Data / hora em que a avaria começou |
| AUSBS / AUZTB | Data / hora em que a avaria terminou (vazio = ainda não terminou) |
| MSAUS | `X` = o ônibus ficou parado |

## 4. Como ligar as tabelas

```
equipamento.EQUNR = equipamento_descricao.EQUNR
equipamento.SWERK = garagem.WERKS
equipamento.EQUNR = nota_manutencao.EQUNR
```

Atenção: cada equipamento tem **duas descrições**, uma em cada idioma.

## 5. Regras

| # | Regra |
|---|---|
| R1 | Considerar só **ônibus** (`EQTYP = 'O'`). |
| R2 | Todo ônibus aparece em **todos os meses**, mesmo nos meses sem quebra, a partir do mês em que entrou em operação. |
| R3 | **Falha** = nota `M2` com `MSAUS = 'X'`. As demais notas não contam. |
| R4 | Falhas que começaram depois de 31/10/2026 ficam de fora. |
| R5 | Se a falha **não terminou** até o fim de 31/10 (fim vazio ou depois disso), considerar que ela termina em **31/10 às 24:00**. Ela conta como **não encerrada**. |
| R6 | **Horas no mês:** horas do mês em que o ônibus estava na frota. Se ele entrou no meio do mês, contar a partir da data de entrada. |
| R7 | **Horas paradas:** horas de falha dentro do mês. Se a falha atravessa a virada do mês, cada mês fica só com a sua parte. |
| R8 | **Quantidade de falhas:** falhas que **começaram** no mês. |
| R9 | **Disponibilidade (%)** = (horas no mês − horas paradas) ÷ horas no mês × 100. |
| R10 | **MTTR** = média da duração das falhas **encerradas** que começaram no mês. Sem falha encerrada, fica vazio. |
| R11 | **MTBF** = (horas no mês − horas paradas) ÷ quantidade de falhas. Sem falha, fica vazio. |
| R12 | Descrição do ônibus em português; se não houver, em inglês. |

Exemplo da R7: uma falha de 29/09 22:00 a 02/10 10:00 dura 60h, que ficam divididas assim:

```
        setembro            |            outubro
29/09 22:00 ──── 30/09 24:00 | 01/10 00:00 ──── 02/10 10:00
            26h              |             34h
```

## 6. Campos de saída

| Campo | Descrição |
|---|---|
| dt_mes_referencia | Primeiro dia do mês |
| cd_veiculo | Código do ônibus |
| ds_veiculo | Descrição (R12) |
| nm_fabricante | Fabricante |
| ds_modelo | Modelo |
| cd_garagem | Código da garagem |
| nm_garagem | Nome da garagem |
| dt_inicio_operacao | Data de entrada em operação |
| hr_calendario | Horas no mês (R6) |
| hr_parada | Horas paradas (R7) |
| hr_operacao | Horas no mês − horas paradas |
| qt_falhas | Quantidade de falhas (R8) |
| qt_falhas_encerradas | Falhas do mês já encerradas (R5) |
| pc_disponibilidade | Disponibilidade % (R9) |
| hr_mttr | MTTR (R10) |
| hr_mtbf | MTBF (R11) |

Horas e percentuais com 2 casas decimais.

## 7. Casos de validação

**Ônibus 9001** (`000000000010009001`), em operação desde 2025:

| Nota | Tipo | Parou? | Início | Fim | Conta? |
|---|---|---|---|---|---|
| 000309000001 | M2 | X | 05/09 08:00 | 05/09 20:00 | Sim, 12h |
| 000309000002 | M2 | X | 29/09 22:00 | 02/10 10:00 | Sim, 60h (26h set + 34h out) |
| 000309000003 | M1 | | 10/09 09:00 | 10/09 10:00 | Não, é solicitação |
| 000309000004 | M2 | | 15/09 14:00 | 15/09 15:00 | Não, não parou |
| 000309000005 | M2 | X | 30/10 18:00 | 02/11 10:00 | Sim, cortada em 31/10 24:00 = 30h, não encerrada |
| 000309000006 | M2 | X | 03/11 07:00 | 03/11 12:00 | Não, começou depois da referência |

**Ônibus 9002** (`000000000010009002`): entrou em operação em 11/10/2026, sem falhas.

**Resultado esperado:**

| Ônibus | Mês | Horas no mês | Horas paradas | Falhas | Disponib. | MTTR | MTBF |
|---|---|---|---|---|---|---|---|
| 9001 | ago | 744 | 0 | 0 | 100,00% | vazio | vazio |
| 9001 | set | 720 | 38 | 2 | 94,72% | 36,00 | 341,00 |
| 9001 | out | 744 | 64 | 1 | 91,40% | vazio | 680,00 |
| 9002 | out | 504 | 0 | 0 | 100,00% | vazio | vazio |

- **set:** paradas 12 + 26 = 38h; MTTR = (12 + 60) ÷ 2 = 36h; MTBF = (720 − 38) ÷ 2 = 341h.
- **out:** paradas 34 + 30 = 64h; a única falha que começou em outubro não terminou, então o MTTR fica vazio; MTBF = (744 − 64) ÷ 1 = 680h.
- **9002:** 21 dias × 24h = 504h, e não aparece em setembro.

## 8. Glossário

| Termo | Significado |
|---|---|
| Disponibilidade | % do tempo em que o ônibus podia rodar |
| MTTR | Tempo médio para consertar |
| MTBF | Tempo médio entre uma quebra e outra |