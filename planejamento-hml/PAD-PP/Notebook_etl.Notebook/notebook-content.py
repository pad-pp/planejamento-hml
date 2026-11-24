# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "45d760e0-76f9-44a9-8ffe-ebff38d12a67",
# META       "default_lakehouse_name": "DB_PAD_PP",
# META       "default_lakehouse_workspace_id": "fa89e973-58ac-4f5a-8dee-69512c070b80",
# META       "known_lakehouses": [
# META         {
# META           "id": "45d760e0-76f9-44a9-8ffe-ebff38d12a67"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col
# notebookutils já está disponível no Fabric

PPA_ALVO = "2024-2027"  # ajuste aqui o filtro desejado
ATIVO_ALVO = "S"  # novo filtro para a coluna ATIVO, ajuste conforme necessário (ex: "S" ou 1)

# Lakehouse de destino: DB_PAD_PP_2024_2027
dest_lakehouse_id = "4c0311f5-70bb-4741-98d7-9a1230b79a48"  
# DB_PAD_PP_2024_2027

onelake_endpoint = notebookutils.conf.get("trident.onelake.endpoint").replace("https://", "")
workspace_id = notebookutils.runtime.context['currentWorkspaceId']


def destino_path(table_name: str) -> str:
    """Retorna o caminho ABFSS da tabela no lakehouse DB_PAD_PP_2024_2027,
    em Tables/dbo/<table_name>.

    Exemplo de retorno:
    abfss://<workspace_id>@<onelake_endpoint>/<dest_lakehouse_id>/Tables/dbo/ORGAO
    """
    return f"abfss://{workspace_id}@{onelake_endpoint}/{dest_lakehouse_id}/Tables/dbo/{table_name}"


# ---------------------------------------------------
# Lista de tabelas a copiar (mesma estrutura, dados filtrados)
# Adicione/remova nomes conforme necessário.
# ---------------------------------------------------

tabelas = [
    "ACAO", "ORGAO", "PROGRAMA", "PRODUTO", "ACAO", "UNIDADE_ORCAMENTARIA", "INDICADOR" , 
    "POLITICA_PUBLICA", "ODS" ,"OBJETIVO_ESTRATEGICO", "BENEFICIARIO", "META_ODS",
     "VINCULO_ODS", "VINCULO_ACAO", "VINCULO_OBJETIVO_ESTRATEGICO", "BENEFICIARIO","VINCULO_BENEFICIARIO",
     "APURACAO_ANUAL_ACAO", "APURACAO_MENSAL_INDICADOR" , "META_INDICADOR", "APURACAO_ANUAL_INDICADOR",
     "APURACAO_ANUAL_ACAO" , "APURACAO_MENSAL_ACAO"
]


# ---------------------------------------------------
# Loop simples: lê da origem, aplica filtro, grava no destino
# ---------------------------------------------------

for tabela in tabelas:
    print(f"Processando tabela: {tabela}")

    # Ler tabela do lakehouse de origem (DB_PAD_PP é o default)
    df = spark.read.table(tabela)

    # Se a coluna PPA existir, aplica filtro; senão, mantém todos os registros
    if "PPA" in [c.upper() for c in df.columns]:
        # Encontrar o nome real da coluna PPA respeitando maiúsculas/minúsculas
        col_ppa = [c for c in df.columns if c.upper() == "PPA"][0]
        df = df.filter(col(col_ppa) == PPA_ALVO)

    # Se a coluna ATIVO existir, aplica filtro; senão, mantém todos os registros
    if "ATIVO" in [c.upper() for c in df.columns]:
        # Encontrar o nome real da coluna ATIVO respeitando maiúsculas/minúsculas
        col_ativo = [c for c in df.columns if c.upper() == "ATIVO"][0]
        df = df.filter(col(col_ativo) == True)

  
    # Gravar no lakehouse destino, em Tables/dbo/<tabela>
    target_path = destino_path(tabela)
    df.write.format("delta").mode("overwrite").save(target_path)

    print(f"Tabela {tabela} gravada em: {target_path}")

df = spark.sql("SELECT * FROM DB_PAD_PP.ACAO LIMIT 100")
display(df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
