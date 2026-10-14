-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse_name": "",
-- META       "default_lakehouse_workspace_id": "",
-- META       "known_lakehouses": []
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "bf4e6282-8a35-4fb0-b03f-a39b3e0d871f",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "bf4e6282-8a35-4fb0-b03f-a39b3e0d871f",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_ORGAO AS
SELECT
    CONCAT(CODIGO_ORGAO, ' - ', NOME) AS [Código e Nome Órgão],
    CODIGO_ORGAO AS [Código Órgão],
    NOME AS [Nome Órgão],
    NOME_SIMPLIFICADO AS [Nome Órgão Simplificado],
    PODER AS [Poder]
FROM ORGAO
WHERE PPA = '2024-2027';

select * from dbo.v_ORGAO

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
