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

df = spark.read.table( 'db_pad_pp_.epa_por_ppa.data_de_atualizacao')
df = df.withColumnRenamed('DATA', 'ultima_atualizacao')
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.saveAsTable('pad_pp_modelo_semantico.dbo.data_de_atualizacao', mode='overwrite')

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
