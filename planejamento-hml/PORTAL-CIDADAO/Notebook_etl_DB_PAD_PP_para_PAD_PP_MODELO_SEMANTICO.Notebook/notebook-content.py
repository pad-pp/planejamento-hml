# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4e2c9278-4a00-4123-937d-f9f2ea2610f7",
# META       "default_lakehouse_name": "DB_PAD_PP_",
# META       "default_lakehouse_workspace_id": "fa89e973-58ac-4f5a-8dee-69512c070b80",
# META       "known_lakehouses": [
# META         {
# META           "id": "4e2c9278-4a00-4123-937d-f9f2ea2610f7"
# META         },
# META         {
# META           "id": "b6e3c753-8b05-430a-aedf-65bf1d2579e6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

def filtro_padrao(df, ppa, ativo=True):
    if 'PPA' in df.columns:
        df = df.filter(df.PPA == ppa).drop('PPA')
    if ativo and 'ATIVO' in df.columns:
        df = df.filter(df.ATIVO == True).drop('ATIVO')
    return df

def etl_padrao(df):
    df = df.toDF(*[c.lower() for c in df.columns])
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def etl_orgao(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.orgao')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.orgao', mode='overwrite')


def etl_programa(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.programa')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.programa', mode='overwrite')    


def etl_acao(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.acao')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.acao', mode='overwrite') 


def etl_produto(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.produto')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.produto', mode='overwrite')  


def etl_indicador(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.indicador')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.indicador', mode='overwrite')


def etl_politica_publica(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.politica_publica')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.politica_publica', mode='overwrite')


def etl_objetivo_estrategico(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.objetivo_estrategico')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.objetivo_estrategico', mode='overwrite')


def etl_beneficiario(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.beneficiario')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.beneficiario', mode='overwrite')


def etl_ods(input_datalake, output_datalake, ppa):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.ods')
    df = filtro_padrao(df, ppa)
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.ods', mode='overwrite')


def etl_data_de_atualizacao(input_datalake, output_datalake):
    df = spark.read.table(f'{input_datalake}.epa_por_ppa.data_de_atualizacao')
    df = etl_padrao(df)
    df.write.option("overwriteSchema", "true").saveAsTable(f'{output_datalake}.dbo.data_de_atualizacao', mode='overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main(input_datalake, output_datalake, ppa):
    etl_orgao(input_datalake, output_datalake, ppa)
    etl_programa(input_datalake, output_datalake, ppa)
    etl_acao(input_datalake, output_datalake, ppa)
    etl_produto(input_datalake, output_datalake, ppa)
    etl_indicador(input_datalake, output_datalake, ppa)

    etl_politica_publica(input_datalake, output_datalake, ppa)
    etl_objetivo_estrategico(input_datalake, output_datalake, ppa)
    etl_beneficiario(input_datalake, output_datalake, ppa)
    etl_ods(input_datalake, output_datalake, ppa)
    
    etl_data_de_atualizacao(input_datalake, output_datalake)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

input_datalake = 'db_pad_pp_'
output_datalake = 'pad_pp_modelo_semantico'
ppa = '2024-2027'
main(input_datalake, output_datalake, ppa)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
