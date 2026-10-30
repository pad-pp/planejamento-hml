# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03b2c78f-b1bb-4402-bc8b-0311f700edca",
# META       "default_lakehouse_name": "MonitoramentoRiscos",
# META       "default_lakehouse_workspace_id": "fa89e973-58ac-4f5a-8dee-69512c070b80",
# META       "known_lakehouses": [
# META         {
# META           "id": "03b2c78f-b1bb-4402-bc8b-0311f700edca"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "a2a5c09d-17a0-ab16-44d1-aee01c486d3e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Análise de Riscos de Informação da Apuração com IA
# 
# Script coleta informações de monitoramento por riscos, e faz a pontuação de novas justificativas de monitoramento com uso de IA
# 
# ## Instalação no Fabric:
# 1) Abra o workspace onde está o Datalake onde ficarão as tabelas analises_ia (crie o datalake, se não existir)
# 2) Mude a variável DATASET para o nome do modelo semântico do relatório Monitoramento por Riscos, para que a consulta DAX consiga achar as apurações físico-financeiras (precisa estar no mesmo workspace)
# 3) Para rodar o script, é necessário um ambiente existente que tenha as bibliotecas Python utilizadas pelo script;
# 4) Abra o ambiente criado, adicione uma biblioteca de repositório público, escolha "openai"
# 5) Faça a importação de notebook no workspace, e aponte para o presente arquivo
# 6) No notebook aberto, altere o ambiente para o ambiente criado no item 3
# 7) Em Gerenciador -> Itens de dados, adicione o item de dados do catálogo do OneLake criado no item 1
# 8) Na pasta Files do OneLake, carregue o arquivo RelatorioPendentes.dax, que tem a consulta no modelo semântico que é utilizada por este script
# 9) Crie uma célula de código para os parâmetros marcados com #Fabric notebook parameters, e transforme a célula em célula de parâmetros (Toogle parameter cell) - assim, ao rodar em um pipeline, estas variáveis podem ser injetadas pelo pipeline


# PARAMETERS CELL ********************

#Fabric notebook parameters
BATCH_SIZE = 100 # quantidade de indicadores que serão analisados por execução
MAX_WORKERS = 10 # quantidade de chamadas à IA em paralelo que serão feitas (apenas para modo online, não batch)
ANO = 0 # ano dos indicadores a serem analisados - quando 0, considera todos os anos disponíveis no modelo semântico
EXECUTAR_MODO_ONLINE = False # se deve ser utilizado o modo de acionamento online da IA ou modo batch (remessa de arquivo e verificação posterior dos resultados)

AZURE_OPENAI_API_VERSION = "2024-10-21"
AZURE_OPENAI_API_KEY = None # caso não seja apontado, será utilizada a conexão à qual a identidade do workspace tem acesso no Azure Foundry
AZURE_OPENAI_ENDPOINT = None # caso não seja apontado o link do endpoint no Azure Foundry, será utilizada a conexão à qual a identidade do workspace tem acesso no Azure Foundry
AZURE_OPENAI_MODEL = "gpt-5-mini"
AZURE_OPENAI_MODEL_TEMPERATURE = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Sempre que for feita alguma alteração no prompt que provoque problemas de comparabilidade entre diferentes análises, altere a versão para que seja um número maior que o anterior. Serão regeradas análises para todos os indicadores

# CELL ********************

PROMPT_VERSION = "v4"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8


# =========================
# 1) Imports
# =========================
import os
import json
import time
import datetime as dt
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED


import sempy.fabric as fabric
from sempy.fabric import FabricDataFrame
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType, BooleanType, DoubleType
)


from openai import OpenAI, AzureOpenAI
from synapse.ml.fabric.credentials import get_openai_httpx_sync_client



# =========================
# 2) Configuracao
# =========================
DATASET = "Monitoramento por Riscos"
DAX_FILE_NAME = "RelatorioPendentes.dax"
DAX_FILE = f"/lakehouse/default/Files/{DAX_FILE_NAME}"


SOURCE_ID_COL = "IdCaso"
SOURCE_ANO_COL = "AnoAnalise"
SOURCE_STATUS_COL = "StatusAnalise"
SOURCE_REPORT_COL = "RelatorioTexto"


TARGET_TABLE = "analises_ia"
ERROR_TABLE = "analises_ia_erros"
PERGUNTAS_TABLE = "perguntas_dpe"
PROMPTS_LOG_TABLE = "prompts_log"


MAX_RETRIES = 3
RETRY_SECONDS = 5


# Limites de payload para gpt-5-mini (128k input). DAX já trunca em 15000 chars; guard Python adicional.
MAX_RELATORIO_CHARS = 15000
MAX_PROMPT_CHARS = 20000 # folga para system + json overhead


BATCH_STATUS_ERRO = ["failed", "expired", "cancelled"]
BATCH_STATUS_TERMINAIS = BATCH_STATUS_ERRO.copy() + ["completed"]

# =========================
# 3) JSON schema
# =========================
PERGUNTAS = []

def montar_json_schema() -> dict:
    properties = {}
    required = []
    for p in perguntas():
        chave = p["chave"]
        chave_j = f"{chave}_justificativa"
        properties[chave] = {"type": "boolean"}
        properties[chave_j] = {"type": "string", "maxLength": 300}
        required.extend([chave, chave_j])
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "QuestionarioBooleanoComJustificativa",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False
            }
        }
    }

# =========================
# 4) Bootstrap Lakehouse
# =========================
TARGET_SCHEMA = StructType([
    StructField("id_indicador", LongType(), True),
    StructField("ano", LongType(), True),
    StructField("id_produto", LongType(), True),
    StructField("id_pergunta", LongType(), True),
    StructField("resposta", BooleanType(), True),
    StructField("justificativa", StringType(), True),
    StructField("status_analise", StringType(), True),
    StructField("versao_prompt", StringType(), True),
    StructField("modelo_ia", StringType(), True),
    StructField("dt_analise_utc", TimestampType(), True)
])
ERROR_SCHEMA = StructType([
    StructField("id_indicador", LongType(), True),
    StructField("id_produto", LongType(), True),
    StructField("ano", LongType(), True),
    StructField("mensagem_erro", StringType(), True),
    StructField("payload_json", StringType(), True),
    StructField("dt_erro_utc", TimestampType(), True)
])
PROMPTS_LOG_SCHEMA = StructType([
    StructField("id_indicador", LongType(), True),
    StructField("versao_prompt", StringType(), True),
    StructField("system_prompt", StringType(), True),
    StructField("user_prompt", StringType(), True),
    StructField("duracao_segundos", DoubleType(), True),
    StructField("dt_log_utc", TimestampType(), True)
])

def bootstrap_tables():
    _criar_perguntas_table()
    _criar_analises_ia_table()
    _criar_prompts_log_table()

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ERROR_TABLE} (
        id_indicador BIGINT,
        id_produto BIGINT,
        ano INT,
        mensagem_erro STRING,
        payload_json STRING,
        dt_erro_utc TIMESTAMP
    )
    USING DELTA
    """)

def _criar_perguntas_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PERGUNTAS_TABLE} (
        id_pergunta INT,
        pergunta STRING,
        ordem INT
    )
    USING DELTA
    """)

    n = spark.sql(f"SELECT COUNT(*) AS n FROM {PERGUNTAS_TABLE}").collect()[0]["n"]
    if n == 0:
        spark.sql(f"""
        INSERT INTO {PERGUNTAS_TABLE} (id_pergunta, pergunta, ordem) VALUES
        (1, 'O relatório explica a variação entre Meta Física Esperada (MFE) e a Entrega Realizada (ER)?', 1),
        (2, 'O relatório explica a variação entre Liquidação Esperada (LE) e a Liquidação Realizada (LR)?', 2),
        (3, 'O relatório declara expectativas (física e finaneira) para os próximos meses?', 3),
        (4, 'O relatório apresenta indícios ou evidências que sustentem as expectivativas para os próximos meses?', 4),
        (5, 'O relatório esclarece todos os Pontos de Atenção (PAt) apontados?', 5)
        """)


def _criar_analises_ia_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id_indicador BIGINT,
        ano INT,
        id_produto BIGINT,
        id_pergunta INT,
        resposta BOOLEAN,
        justificativa STRING,
        status_analise STRING,
        versao_prompt STRING,
        modelo_ia STRING,
        dt_analise_utc TIMESTAMP
    )
    USING DELTA
    """)
    # Evoluir tabela existente (Lakehouse já sem hash_entrada) - ADD COLUMNS IF NOT EXISTS
    try:
        spark.sql(f"ALTER TABLE {TARGET_TABLE} ADD COLUMNS (justificativa STRING)")
    except Exception:
        pass


def _criar_prompts_log_table():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PROMPTS_LOG_TABLE} (
        id_indicador BIGINT,
        versao_prompt STRING,
        system_prompt STRING,
        user_prompt STRING,
        duracao_segundos DOUBLE,
        dt_log_utc TIMESTAMP
    )
    USING DELTA
    """)


# =========================
# 5) Leitura do semantic model
# =========================
def run_dax(dax: str) -> pd.DataFrame:
    dax_named_parameters = dax.replace("@UltimaVersaoPrompt", f'"{PROMPT_VERSION}"')
    df = fabric.evaluate_dax(DATASET, dax_named_parameters)
    df.columns = df.columns.str.strip("[]")
    return df

def carregar_dax() -> str:
    with open(DAX_FILE, "r", encoding="utf-8") as f:
        return f.read()

def get_pendentes(ano: int = 0) -> pd.DataFrame:
    dax = carregar_dax()
    df = run_dax(dax)
    return df[df[SOURCE_ANO_COL] == ano] if ano > 0 else df


# =========================
# 6) Utilitarios
# =========================
def _get_ids_from_case(id_caso: str):
    partes = id_caso.split("-")
    id_indicador = int(partes[0]) if len(partes) > 0 else 0
    id_produto = int(partes[1]) if len(partes) > 1 else 0
    ano = int(partes[2]) if len(partes) > 2 else 0
    return id_indicador, id_produto, ano

def _carregar_perguntas() -> list[dict]:
    df = spark.sql(
        f"SELECT id_pergunta, pergunta, ordem FROM {PERGUNTAS_TABLE} ORDER BY ordem"
    ).toPandas()
    global PERGUNTAS
    PERGUNTAS = [
        {"chave": f"q{row['id_pergunta']}", "texto": row["pergunta"]}
        for _, row in df.iterrows()
    ]
    return PERGUNTAS

def perguntas() -> list[dict]:
    if len(PERGUNTAS) == 0:
        return _carregar_perguntas()
    else:
        return PERGUNTAS

def extrair_conteudo_da_resposta_ia(content):
    data = json.loads(content)

    for p in perguntas():
        key = p["chave"]
        key_j = f"{key}_justificativa"
        if key not in data or not isinstance(data[key], bool):
            raise ValueError(f"Campo invalido na resposta: {key}")
        if key_j not in data or not isinstance(data[key_j], str):
            raise ValueError(f"Campo invalido na resposta: {key_j}")
        # truncar para 300 se exceder (garantia adicional ao maxLength do schema)
        if len(data[key_j]) > 300:
            data[key_j] = data[key_j][:300]
        # validacao obrigatoriedade: false exige justificativa nao vazia
        if data[key] is False and not data[key_j].strip():
            raise ValueError(f"Justificativa obrigatoria para {key} quando false e veio vazia")
        # quando true, forcar vazia
        if data[key] is True and data[key_j].strip() != "":
            data[key_j] = ""
    return data

def truncar_relatorio(relatorio: str) -> str:
    if relatorio is None:
        return ""
    relatorio = str(relatorio)
    if len(relatorio) > MAX_RELATORIO_CHARS:
        return relatorio[:MAX_RELATORIO_CHARS] + "\n[RELATÓRIO TRUNCADO EM {} CARACTERES - Python guard]".format(MAX_RELATORIO_CHARS)
    return relatorio

def montar_system_prompt() -> str:
    n = len(perguntas())
    return f"""
        Voce e um classificador tecnico de relatorios.
        Leia o relatorio recebido e responda exatamente {n} perguntas.
        Regras obrigatorias:
        - Responda somente com JSON valido.
        - Para cada pergunta responda com dois campos: qN (boolean) e qN_justificativa (string, max 300 caracteres).
        - Se qN for true, qN_justificativa deve ser string vazia "".
        - Se qN for false, qN_justificativa e OBRIGATORIA: explique em 1-2 frases objetivas o que falta no relatorio para a resposta ser verdadeira (max 300 caracteres, nao deixe vazia).
        - Nao escreva texto adicional fora do JSON.
        - Avalie somente com base no relatorio enviado.
        - Se a informacao nao estiver clara no relatorio, responda false e justifique o que falta.
    """.strip()

def montar_user_prompt(row: pd.Series) -> str:
    id_caso = str(row[SOURCE_ID_COL])
    id_indicador, id_produto, ano = _get_ids_from_case(id_caso)
    relatorio = truncar_relatorio(row.get(SOURCE_REPORT_COL, ""))
    payload = {
        "id_indicador": id_indicador,
        "ano": ano,
        "perguntas": {p["chave"]: p["texto"] for p in perguntas()},
        "relatorio": relatorio
    }
    user_prompt = json.dumps(payload, ensure_ascii=False)
    # Guard adicional: se JSON total > MAX_PROMPT_CHARS, truncar relatorio proporcionalmente
    if len(user_prompt) > MAX_PROMPT_CHARS:
        excesso = len(user_prompt) - MAX_PROMPT_CHARS
        # reduzir relatorio no tamanho do excesso + margem
        novo_limite = max(1000, len(relatorio) - excesso - 500)
        relatorio = relatorio[:novo_limite] + "\n[RELATÓRIO TRUNCADO PARA CABER NO PROMPT]"
        payload["relatorio"] = relatorio
        user_prompt = json.dumps(payload, ensure_ascii=False)
        print(f"Aviso: prompt truncado para {len(user_prompt)} chars (limite {MAX_PROMPT_CHARS}) - caso {id_caso}")
    # Log estimativa de tokens (~4 chars/token)
    est_tokens = len(user_prompt) // 4 + len(montar_system_prompt()) // 4
    if est_tokens > 6000:
        print(f"Aviso: prompt estimado em ~{est_tokens} tokens para {id_caso} (relatorio {len(relatorio)} chars)")
    return user_prompt


# =========================
# 7) Chamada Azure OpenAI
# =========================
def get_aoai_client():
    if AZURE_OPENAI_API_KEY is not None and AZURE_OPENAI_ENDPOINT is not None:
        if AZURE_OPENAI_API_VERSION == "v1":
            adapted_endpoint = AZURE_OPENAI_ENDPOINT if not AZURE_OPENAI_ENDPOINT.endswith("/") else AZURE_OPENAI_ENDPOINT[:-1]
            adapted_endpoint += "/openai/v1/"
            return OpenAI(
                api_key=AZURE_OPENAI_API_KEY,
                base_url=adapted_endpoint
            )
        else:
            return AzureOpenAI(
                api_key=AZURE_OPENAI_API_KEY,
                azure_endpoint=AZURE_OPENAI_ENDPOINT,
                api_version=AZURE_OPENAI_API_VERSION
            )
    else:
        return AzureOpenAI(
            http_client=get_openai_httpx_sync_client(),
            api_version=AZURE_OPENAI_API_VERSION
    )


def chamar_ia_relatorio_batch(rows: pd.DataFrame) -> (str, list[dict]):
    system_prompt = montar_system_prompt()
    response_format = montar_json_schema()
    jsonl_requests = []
    prompts_log = []
    for _, row in enumerate(rows):
        user_prompt = montar_user_prompt(row)
        id_caso = str(row.get(SOURCE_ID_COL, ""))
        jsonl_dict = {
            "custom_id": id_caso,
            "method": "POST",
            "url": "/chat/completions",
            "body": {
                "model": AZURE_OPENAI_MODEL,
                "temperature": AZURE_OPENAI_MODEL_TEMPERATURE,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "response_format": response_format
            }
        }
        jsonl_requests.append(json.dumps(jsonl_dict, ensure_ascii=False))
        id_indicador, _, _ = _get_ids_from_case(id_caso)
        prompts_log.append({
            "id_indicador": id_indicador,
            "versao_prompt": PROMPT_VERSION,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "duracao_segundos": 0.0,
            "dt_log_utc": dt.datetime.utcnow()
        })
    jsonl_content = ("\n".join(jsonl_requests) + "\n").encode("utf-8")
    client = get_aoai_client()
    upload_response = client.files.create(
        file=("monitoramentoriscos-" + dt.datetime.now().strftime("%Y%m%d-%H%M") + ".jsonl", jsonl_content),
        purpose="batch"
    )
    batch_response = client.batches.create(
        input_file_id=upload_response.id,
        endpoint="/chat/completions",
        completion_window="24h"
    )
    return batch_response.id, prompts_log


def verificar_status_lote_batch(batch_id: str):
    client = get_aoai_client()
    batch = client.batches.retrieve(batch_id)
    return {
        "id": batch.id,
        "status": batch.status,
        "output_file_id": batch.output_file_id,
        "error_file_id": batch.error_file_id,
        "errors": batch.errors,
        "request_counts": batch.request_counts,
        "metadata": batch.metadata
    }


def aguardar_finalizacao_lote_batch(batch_id: str, poll_interval: int = 60) -> dict:
    """
    Faz polling do status do batch até que ele seja concluído, falhe ou expire.
    
    Args:
        batch_id: O ID do batch.
        poll_interval: Intervalo em segundos entre as verificações.
        
    Returns:
        O dicionário de status final do batch.
    """
    print(f"Iniciando monitoramento do batch {batch_id}...")
    while True:
        status_info = verificar_status_lote_batch(batch_id)
        status = status_info["status"]
        print(f"Status atual do batch {batch_id}: {status}")
        if status in BATCH_STATUS_TERMINAIS:
            return status_info
        time.sleep(poll_interval)


def pegar_resultados_de_batch(batch_response_file_id: str):
    client = get_aoai_client()
    file_response = client.files.content(batch_response_file_id)
    texto = file_response.text.strip()

    resultados = []
    erros = []

    if not texto:
        return resultados, erros

    for linha in texto.splitlines():
        item = json.loads(linha)

        custom_id = item.get("custom_id")
        if not custom_id:
            erros.append({
                "id_indicador": 0,
                "id_produto": 0,
                "ano": 0,
                "mensagem_erro": "custom_id ausente no resultado Batch",
                "payload_json": linha,
                "dt_erro_utc": dt.datetime.utcnow()
            })
            continue

        id_indicador, id_produto, ano = _get_ids_from_case(custom_id)

        try:
            if "response" in item:
                content = item["response"]["body"]["choices"][0]["message"]["content"]
                data = extrair_conteudo_da_resposta_ia(content)
                data['modelo'] = item["response"]["body"]["model"]
                saida = normalizar_saida(id_indicador, id_produto, ano, data)
                resultados.append(saida)
            elif "error" in item:
                raise Exception(item["error"])
        except Exception as e:
            erros.append({
                "id_indicador": id_indicador,
                "id_produto": id_produto,
                "ano": ano,
                "mensagem_erro": str(e),
                "payload_json": linha,
                "dt_erro_utc": dt.datetime.utcnow()
            })

    return resultados, erros


def pegar_erros_de_batch(batch_error_file_id: str):
    client = get_aoai_client()
    error_response = client.files.content(batch.error_file_id)
    error_text = error_response.text.strip()
    erros = []
    if error_text:
        for linha in error_text.splitlines():
            item = json.loads(linha)
            id_caso = item.get("custom_id")
            id_indicador, id_produto, ano = _get_ids_from_case(id_caso)

            erros.append({
                "id_indicador": id_indicador,
                "id_produto": id_produto,
                "ano": ano,
                "mensagem_erro": str(
                    item.get("error", "Erro no Batch")
                ),
                "payload_json": linha,
                "dt_erro_utc": dt.datetime.utcnow()
            })
    return erros


def chamar_ia_relatorio(client: OpenAI, row: pd.Series) -> dict:
    user_prompt = montar_user_prompt(row)
    system_prompt = montar_system_prompt()

    last_error = None
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            inicio = time.perf_counter()
            response = client.chat.completions.create(
                model=AZURE_OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format=montar_json_schema()
            )
            data = extrair_conteudo_da_resposta_ia(response.choices[0].message.content)
            duracao = time.perf_counter() - inicio
            print(f"Analise de {str(row[SOURCE_ID_COL])} concluída em {duracao:.2f}s")
            return {"data": data, "duracao": duracao, "system_prompt": system_prompt, "user_prompt": user_prompt}

        except Exception as e:
            last_error = e
            print(f"{dt.datetime.utcnow()} - Erro na tentativa {tentativa} de {str(row[SOURCE_ID_COL])}: {e}")
            if tentativa < MAX_RETRIES:
                time.sleep(RETRY_SECONDS)
            else:
                raise last_error


# =========================
# 8) Persistencia
# =========================
def normalizar_saida(id_indicador: int, id_produto: int, ano: int, resposta: dict) -> list[dict]:
    agora = dt.datetime.utcnow()

    linhas = []
    for p in perguntas():
        chave = p["chave"]
        chave_j = f"{chave}_justificativa"
        resp = bool(resposta[chave])
        just = str(resposta.get(chave_j, "") or "")
        if len(just) > 300:
            just = just[:300]
        if resp is True:
            just = ""
        linhas.append({
            "id_indicador": id_indicador,
            "ano": ano,
            "id_produto": id_produto,
            "id_pergunta": int(chave[1:]),
            "resposta": resp,
            "justificativa": just,
            "status_analise": "ANALISADO",
            "versao_prompt": PROMPT_VERSION,
            "modelo_ia": resposta.get("modelo", AZURE_OPENAI_MODEL),
            "dt_analise_utc": agora
        })
    return linhas


def normalizar_erro(id_indicador: int, id_produto: int, ano: int, user_prompt: str, erro: Exception) -> dict:
    return {
        "id_indicador": id_indicador,
        "id_produto": id_produto,
        "ano": ano,
        "mensagem_erro": str(erro),
        "payload_json": user_prompt,
        "dt_erro_utc": dt.datetime.utcnow()
    }


def gravar_resultados(resultados: list[list[dict]]):
    linhas = [linha for bloco in resultados for linha in bloco]
    if linhas:
        df = spark.createDataFrame(linhas, schema=TARGET_SCHEMA)
        target_fields = [f.name for f in spark.table(TARGET_TABLE).schema.fields]
        for col_name in target_fields:
            target_type = spark.table(TARGET_TABLE).schema[col_name].dataType
            df = df.withColumn(col_name, df[col_name].cast(target_type))
        df.select(target_fields).write.mode("append").insertInto(TARGET_TABLE)

def gravar_erros(erros: list[dict]):
    if erros:
        df = spark.createDataFrame(erros, schema=ERROR_SCHEMA)
        target_fields = [f.name for f in spark.table(ERROR_TABLE).schema.fields]
        for col_name in target_fields:
            target_type = spark.table(ERROR_TABLE).schema[col_name].dataType
            df = df.withColumn(col_name, df[col_name].cast(target_type))
        df.select(target_fields).write.mode("append").insertInto(ERROR_TABLE)

def gravar_prompts_log(prompts_list: list[dict]):
    if prompts_list:
        df = spark.createDataFrame(prompts_list, schema=PROMPTS_LOG_SCHEMA)
        target_fields = [f.name for f in spark.table(PROMPTS_LOG_TABLE).schema.fields]
        for col_name in target_fields:
            target_type = spark.table(PROMPTS_LOG_TABLE).schema[col_name].dataType
            df = df.withColumn(col_name, df[col_name].cast(target_type))
        df.select(target_fields).write.mode("append").insertInto(PROMPTS_LOG_TABLE)


# =========================
# 9) Processamento principal
# =========================
def _inicializa():
    bootstrap_tables()
    _carregar_perguntas()

def _inicializa_processo(ano: int):
    _inicializa()
    if ano > 0:
        print(f"Consultando apurações pendentes de análise apenas para o ano de {ano}")
    pendentes = get_pendentes(ano)
    rows = pendentes.to_dict(orient="records")
    print(f"Foram encontradas {len(rows)} apurações pendentes de processamento")
    return rows


def enviar_lote_batch(ano: int = ANO):
    rows = _inicializa_processo(ano)
    if len(rows) == 0:
        return None
    batch_id, prompts_log = chamar_ia_relatorio_batch(rows)
    gravar_prompts_log(prompts_log)
    return batch_id


def processar_lote_batch(batch_id):
    final_status = aguardar_finalizacao_lote_batch(batch_id)
    if final_status["status"] not in BATCH_STATUS_TERMINAIS:
        return {
            "batch_id": batch_id,
            "status": final_status["status"],
            "processado": False
        }
    bootstrap_tables()
    resultados = []
    erros = []
    if final_status.get("output_file_id"):
        print(f"Batch {batch_id} concluído com sucesso. Baixando resultados...")
        resultados, erros = pegar_resultados_de_batch(final_status["output_file_id"])
    if final_status.get("error_file_id"):
        print(f"Batch {batch_id} também possui erros. Baixando erros...")
        erros.extend(pegar_erros_de_batch(final_status["error_file_id"]))

    gravar_resultados(resultados)
    print(f"Gravadas {len(resultados)} análises")
    gravar_erros(erros)
    print(f"Gravados {len(erros)} erros na geração de análises")
    ultimo_id_gravado = resultados[-1][0]['id_indicador'] if len(resultados) > 0 else 0
    return {
        "batch_id": batch_id,
        "status": final_status["status"],
        "processado": True,
        "processados": len(resultados) + len(erros),
        "gravados": len(resultados),
        "erros": len(erros),
        "total_pendentes_lidos": final_status['request_counts'].total,
        "ultimo_id_gravado": ultimo_id_gravado
    }


def processar_row_online(row_dict: dict):
    row = pd.Series(row_dict)
    id_caso = str(row[SOURCE_ID_COL])
    id_indicador, id_produto, ano = _get_ids_from_case(id_caso)
    client = get_aoai_client()

    try:
        resposta = chamar_ia_relatorio(client, row)
        saida = normalizar_saida(id_indicador, id_produto, ano, resposta["data"])
        if not saida:
            raise ValueError("Resposta da IA não gerou linhas (PERGUNTAS vazio?)")
        return {
            "status": "ok",
            "resultado": saida,
            "duracao": resposta["duracao"],
            "prompts": {
                "system_prompt": resposta["system_prompt"],
                "user_prompt": resposta["user_prompt"]
            }
        }

    except Exception as e:
        return {
            "status": "erro",
            "erro": normalizar_erro(id_indicador, id_produto, ano, montar_user_prompt(row), e)
        }


def processar_lote_online(ano: int = ANO, batch_size: int = BATCH_SIZE, write_batch_size: int = 10, max_workers: int = MAX_WORKERS):
    rows = _inicializa_processo(ano)
    total_disponivel = len(rows)

    if batch_size <= 0:
        raise ValueError("batch_size deve ser maior que zero")

    if max_workers <= 0:
        raise ValueError("max_workers deve ser maior que zero")

    workers = max_workers if batch_size >= max_workers else batch_size
    print(f"Solicitado processamento de no máximo {batch_size} apurações, com {workers} agentes paralelos")
    
    if total_disponivel == 0:
        return {
            "processados": 0,
            "gravados": 0,
            "erros": 0
        }
    
    gravados = 0
    erros = 0
    processados = 0
    ultimo_id_gravado = 0

    buffer_resultados = []
    buffer_erros = []
    buffer_prompts = []
    idx = 0
    futures = {}


    with ThreadPoolExecutor(max_workers=workers) as executor:
        while (idx < total_disponivel or futures) and gravados < batch_size:
            while idx < total_disponivel and len(futures) < workers and gravados + len(buffer_resultados) < batch_size:
                row_dict = rows[idx]
                future = executor.submit(processar_row_online, row_dict)
                futures[future] = row_dict
                idx += 1

            if not futures:
                break

            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)

            for future in done:
                futures.pop(future, None)
                result = future.result()
                status = result["status"]
                processados += 1

                if status == "ok":
                    if gravados + len(buffer_resultados) < batch_size:
                        buffer_resultados.append(result["resultado"])

                        id_caso = result["resultado"][0].get("id_indicador", 0)
                        buffer_prompts.append({
                            "id_indicador": id_caso,
                            "versao_prompt": result["resultado"][0].get("versao_prompt", ""),
                            "system_prompt": result["prompts"]["system_prompt"],
                            "user_prompt": result["prompts"]["user_prompt"],
                            "duracao_segundos": result["duracao"],
                            "dt_log_utc": dt.datetime.utcnow()
                        })

                        if len(buffer_resultados) >= write_batch_size:
                            gravar_resultados(buffer_resultados)
                            gravar_prompts_log(buffer_prompts)
                            gravados += len(buffer_resultados)
                            ultimo_id_gravado = id_caso
                            buffer_resultados = []
                            buffer_prompts = []

                else:
                    buffer_erros.append(result["erro"])

                    if len(buffer_erros) >= write_batch_size:
                        gravar_erros(buffer_erros)
                        erros += len(buffer_erros)
                        buffer_erros = []

        restante_necessario = batch_size - gravados

        if buffer_resultados and restante_necessario > 0:
            para_gravar = buffer_resultados[:restante_necessario]
            para_gravar_prompts = buffer_prompts[:restante_necessario]
            gravar_resultados(para_gravar)
            gravar_prompts_log(para_gravar_prompts)
            ultimo_item = para_gravar[-1]
            if isinstance(ultimo_item, list) and len(ultimo_item) > 0:
                ultimo_id_gravado = ultimo_item[0].get("id_indicador")
            elif isinstance(ultimo_item, dict):
                ultimo_id_gravado = ultimo_item.get("id_indicador")
            gravados += len(para_gravar)

        if buffer_erros:
            gravar_erros(buffer_erros)
            erros += len(buffer_erros)

    return {
        "processados": processados,
        "gravados": gravados,
        "erros": erros,
        "total_pendentes_lidos": total_disponivel,
        "ultimo_id_gravado": ultimo_id_gravado if ultimo_id_gravado is not None else 0
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Processamento Online
# Abaixo, segue a forma de executar o processo batch de criação de análises, utilizando o modo online.
# 
# Desta forma, determina-se um máximo de indicadores (parametro batch_size) e uma quantidade de threads para execução em paralelo (parametro max_workers). As chamadas à IA são feitas em modo chat, de forma que fica aguardando as respostas para cada indicador, e elas vão sendo salvas no lakehouse conforme chegam (em lotes menores que o batch_size, conforme parâmetro write_batch_size).

# CELL ********************

# =========================
# 10) Execucao Online
# =========================
inicio_execucao = dt.datetime.utcnow()
resultado_execucao = None
if EXECUTAR_MODO_ONLINE:
    resultado_execucao = processar_lote_online(ano=ANO)
resultado_execucao

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Processamento em Batch
# Uma outra possibilidade é executar o processo de criação de análises em modo batch total.
# 
# Desta forma, as chamadas à IA de todos os indicadores pendentes (conforme parâmetro ano) são feitas em modo batch na IA.
# 
# Primeiramente, é enviado um arquivo com todos os prompts para a IA, e recebe-se um ID de processo batch; 
# o provedor tem até 24 horas para realizar o processamento de todos os prompts.
# 
# É possível verificar o status do envio utilizando-se do método aguardar_finalizacao_lote_batch(). 
# Quando um envio é processado, aí pode-se baixar um arquivo contendo todas as respostas de prompts, e gravá-las de uma vez no lakehouse.
# 
# O método processar_lote_batch() já faz a espera (fica em polling, checando a cada 60 segundos) e grava tudo quando estiver pronto.

# CELL ********************

# =========================
# 10) Execucao Batch
# =========================
id_batch = None
if not EXECUTAR_MODO_ONLINE:
    id_batch = enviar_lote_batch(ano=ANO)
    if (id_batch is not None):
        print(f"Enviado lote e criado batch para processamento com ID {id_batch}")
    else:
        print("Não havia itens a serem analisados, não foi gerado batch para processamento")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# Código abaixo lista os batches enviados e mostra status deles, caso precise recuperar um batch posteriormente pelo ID 

# CELL ********************

from datetime import datetime, timezone

batch_list = sorted(get_aoai_client().batches.list(), key=lambda batch: batch.created_at, reverse=True)
for batch in batch_list:
    criado = datetime.fromtimestamp(batch.created_at, tz=timezone.utc).isoformat()
    completado = None
    if batch.completed_at is not None:
        completado = datetime.fromtimestamp(batch.completed_at, tz=timezone.utc).isoformat()
    print(f"Batch {batch.id} ({batch.request_counts.total} itens) - status {batch.status}; criado em {criado}, completado em {completado}")
id_batch = batch_list[0].id if len(batch_list) > 0 else None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

inicio_execucao = dt.datetime.utcnow()
resultado_execucao = None
if not EXECUTAR_MODO_ONLINE and id_batch is not None:
    resultado_execucao = processar_lote_batch(id_batch)
resultado_execucao

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
