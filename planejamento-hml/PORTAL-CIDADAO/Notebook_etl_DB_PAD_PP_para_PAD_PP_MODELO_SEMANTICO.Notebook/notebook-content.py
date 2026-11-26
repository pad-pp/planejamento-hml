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

import functools

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

def extract_and_load(input_datalake, output_datalake, ppa, schema, input_table_name, output_table_name=None):
    target_table_name = input_table_name if output_table_name is None else output_table_name
    def inner(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            print(f'Executando {input_datalake}.{schema}.{input_table_name}')
            df = spark.read.table(f'{input_datalake}.{schema}.{input_table_name}')
            df = filtro_padrao(df, ppa)
            df = etl_padrao(df)
            df = function(df)
            output_schema = 'dbo'
            df.write.option('overwriteSchema', 'true').saveAsTable(f'{output_datalake}.{output_schema}.{target_table_name}', mode='overwrite')
            print(f'Gravado em {output_datalake}.{output_schema}.{target_table_name}')
        return wrapper
    return inner

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

el = functools.partial(
    extract_and_load,
    input_datalake='DB_PAD_PP_',
    output_datalake='PAD_PP_MODELO_SEMANTICO',
    ppa='2024-2027')

@el(schema='epa_por_ppa', input_table_name='orgao')
def etl_orgao(df):
    return df

@el(schema='epa_por_ppa', input_table_name='programa')
def etl_programa(df):
    return df

@el(schema='epa_por_ppa', input_table_name='produto')
def etl_produto(df):
    return df

@el(schema='epa_por_ppa', input_table_name='acao')
def etl_acao(df):
    return df

@el(schema='epa_por_ppa', input_table_name='vinculo_acao')
def etl_vinculo_acao(df):
    return df

@el(schema='epa_por_ppa', input_table_name='indicador')
def etl_indicador(df):
    return df

@el(schema='epa_por_ppa', input_table_name='politica_publica')
def etl_politica_publica(df):
    return df

@el(schema='epa_por_ppa', input_table_name='vinculo_politica_publica')
def etl_vinculo_politica_publica(df):
    return df

@el(schema='epa_por_ppa', input_table_name='objetivo_estrategico')
def etl_objetivo_estrategico(df):
    return df

@el(schema='epa_por_ppa', input_table_name='vinculo_objetivo_estrategico')
def etl_vinculo_objetivo_estrategico(df):
    return df

@el(schema='epa_por_ppa', input_table_name='beneficiario')
def etl_beneficiario(df):
    return df

@el(schema='epa_por_ppa', input_table_name='ods')
def etl_ods(df):
    return df

@el(schema='epa_por_ppa', input_table_name='data_de_atualizacao')
def etl_data_de_atualizacao(df):
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    etl_orgao()                        # OK
    etl_programa()                     # OK
    etl_acao()                         # OK
    etl_vinculo_acao()                 # OK
    etl_produto()                      # OK
    etl_indicador()                    # OK

    etl_politica_publica()             # OK
    etl_vinculo_politica_publica()     # OK
    etl_objetivo_estrategico()         # OK
    etl_vinculo_objetivo_estrategico() # OK
    etl_beneficiario()
    etl_ods()
    
    etl_data_de_atualizacao() # OK

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main()

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
