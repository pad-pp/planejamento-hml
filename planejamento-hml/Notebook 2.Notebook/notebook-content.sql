-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
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

CREATE VIEW orgao_testes
AS
SELECT CODIGO_ORGAO AS [Código Órgão]
FROM ORGAO
where PPA = '2024-2027'


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
