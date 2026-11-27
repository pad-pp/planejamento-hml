-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "22fe4cd2-2e0d-408f-b4cd-308e2078fccb",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "22fe4cd2-2e0d-408f-b4cd-308e2078fccb",
-- META           "type": "Lakewarehouse"
-- META         },
-- META         {
-- META           "id": "1453f73e-441a-bc82-48d6-542933df602c",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_orgao AS
SELECT
    codigo_orgao || ' - ' || nome AS [Código e Nome Órgão]
    ,codigo_orgao AS [Código Órgão]
    ,nome AS [Nome Órgão]
    ,nome_simplificado AS [Nome Órgão Simplificado]
    ,poder AS [Poder]
FROM orgao;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_programa AS
SELECT 
  codigo_programa AS [Código Programa]
  ,p.nome AS [Nome Programa]
  ,p.codigo_programa || ' - ' || p.nome AS [Código e Nome Programa]
  ,p.objetivo AS [Objetivo Programa]
  ,CASE p.abrangencia_espacial
    WHEN 'Estadual' THEN 'Estado'
    WHEN 'Municipal' THEN 'Município'
    WHEN 'Regional' THEN 'Região'
  END AS [Abrangência Espacial]
  ,p.tipo as [Tipo]
  ,p.diagnostico as [Diagnóstico Programa]
  ,p.classificacao AS [Classificação]
  ,p.codigo_orgao_responsavel as [Código Órgão Responsável Programa]
  ,o.nome AS [Órgão Responsável]
  ,o.poder AS [Poder Órgão Responsável]
FROM programa p
JOIN orgao o ON p.codigo_orgao_responsavel = o.codigo_orgao;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_indicador AS
SELECT 
  i.codigo_indicador            AS [Código Indicador]
  ,i.nome                       AS [Nome Indicador]
  ,i.unidade_de_medida          AS [Unidade de Medida]
  ,i.tipo                       AS [Tipo de Indicador]
  ,CASE i.polaridade
    WHEN 'Positiva' THEN 'Quanto maior, melhor'
    WHEN 'Negativa' THEN 'Quanto menor, melhor'
  END AS [Polaridade]
  ,CASE i.base_geografica
    WHEN 'Estadual' THEN 'Estado'
    WHEN 'Municipal' THEN 'Município'
    WHEN 'Regional' THEN 'Região'
  END AS [Base Geográfica]
  ,i.forma_de_totalizacao_anual AS [Forma de Totalização Anual]
  ,i.forma_de_totalizacao_ppa   AS [Forma de Totalização PPA]
  ,i.periodicidade              AS [Periodicidade]
  ,i.meta_final_ppa             AS [Meta ao Final do PPA]
  ,i.categoria                  AS [Categoria Indicador]
  ,COALESCE(i.codigo_produto, 9999*10000 + i.codigo_programa) AS [Código Produto]
  ,i.codigo_indicador || ' - ' || i.nome AS [Código e Nome Indicador]
  ,i.codigo_orgao               AS [Código Órgão Responsável Indicador]
  ,i.codigo_programa            AS [Código Programa]
  ,o.nome_simplificado          AS [Órgão Responsável]
  ,i.codigo_programa || ' - ' || pgm.nome AS [Nome Programa]
  ,CASE
    WHEN i.meta_anual_1 IS NOT NULL
      OR i.meta_anual_2 IS NOT NULL
      OR i.meta_anual_3 IS NOT NULL
      OR i.meta_anual_4 IS NOT NULL THEN 'Sim'
    ELSE 'Não'
  END AS [Possui Metas Anuais]
  ,i.descricao                  AS [Descrição]
  ,i.fonte                      AS [Fonte]
  ,i.valor_linha_de_base        AS [Valor Linha de Base]
  ,i.periodo_linha_de_base      AS [Período Linha de Base]
  ,i.explicacao_meta_ppa        AS [Explicação Meta ao Final do PPA]
  ,'Sim'                        AS [Linha de Base Correta]
  ,i.soma_acoes                 AS [Totalização do Indicador é Somatório das Metas das Ações]
  ,i.soma_uos                   AS [Totalização da Ação é Somatório das Metas das UOs]
FROM indicador i
JOIN orgao o ON i.codigo_orgao = o.codigo_orgao
LEFT JOIN programa pgm ON i.codigo_programa = pgm.codigo_programa;
GO;

-- Linha de Base Correta = IF(AND(NOT(ISBLANK(Indicador[Valor Linha de Base])), IF(Indicador[Polaridade] == "Quanto maior, melhor", ISBLANK(COUNTROWS(FILTER(RELATEDTABLE('Indicador Apuração'), 'Indicador Apuração'[Meta do Ano] >= Indicador[Valor Linha de Base]))), ISBLANK(COUNTROWS(FILTER(RELATEDTABLE('Indicador Apuração'), 'Indicador Apuração'[Meta do Ano] <= Indicador[Valor Linha de Base]))))), "Sim", "Não")
-- Possui Metas Anuais = IF(AND(NOT(ISBLANK(Indicador[Meta ao Final do PPA])), ISBLANK(COUNTROWS(FILTER(RELATEDTABLE('Indicador Apuração'), ISBLANK('Indicador Apuração'[Meta do Ano]))))), "Sim", "Não")

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_produto AS
(
SELECT
  9999*10000 + codigo_programa         AS [Código Produto]
  ,'INDICADORES DE RESULTADO'          AS [Nome Produto]
  ,'INDICADORES DE RESULTADO'          AS [Código e Nome Produto]
  ,codigo_programa                     AS [Código Programa]
  ,codigo_programa || ' - ' || nome    AS [Programa e Produto para Busca Textual]
  ,NULL                                AS [Descrição Produto]
  ,CAST(0 AS BIT)                      AS [Produto Prioritário]
  ,'Não'                               AS [Meta Prioritária]
  ,'Finalístico'                       AS [Classificação Produto]
FROM programa
)
UNION
(
SELECT
  pdt.codigo_produto                   AS [Código Produto]
  ,pdt.nome                            AS [Nome Produto]
  ,pdt.codigo_produto || ' - ' || pdt.nome AS [Código e Nome Produto]
  ,pdt.codigo_programa                 AS [Código Programa]
  ,pdt.codigo_programa || ' - ' || pgm.nome || ' ' || pdt.nome [Programa e Produto para Busca Textual]
  ,pdt.descricao                       AS [Descrição Produto]
  ,pdt.prioritario                     AS [Produto Prioritário]
  ,CASE 
   WHEN pdt.prioritario = 1 THEN 'Sim'
   WHEN pdt.prioritario = 0 THEN 'Não'
  END [Meta Prioritária]
  ,pdt.classificacao                   AS [Classificação Produto]
FROM produto pdt
JOIN programa pgm ON pdt.codigo_programa = pgm.codigo_programa
);
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_acao AS
SELECT
  a.codigo_acao || '.' || va.codigo_orgao || '.' || va.codigo_programa || '.' || va.codigo_produto AS [Código Ação]
  ,a.codigo_acao || ' - ' || a.nome AS [Código e Nome Ação]
  ,va.codigo_orgao                  AS [Código Órgão]
  ,va.codigo_programa               AS [Código Programa]
  ,va.codigo_produto                AS [Código Produto]
  ,a.nome                           AS [Denominação]
  ,a.nome_sucinto                   AS [Denominação Sucinta]
  ,a.descricao                      AS [Descrição]
  ,a.finalidade                     AS [Finalidade]
  ,a.forma_de_implementacao         AS [Formas de Implementação]
  ,va.codigo_programa || ' - ' || pgm.nome || ' ' || va.codigo_produto || ' - ' || pdt.nome AS [Programa e Produto para Busca Textual]
  ,a.tipo                           AS [Tipo de Ação]
FROM acao a
JOIN (SELECT distinct codigo_orgao, codigo_programa, codigo_produto, codigo_acao FROM vinculo_acao) va ON va.codigo_acao = a.codigo_acao
LEFT JOIN programa pgm ON pgm.codigo_programa = va.codigo_programa
LEFT JOIN produto pdt ON pdt.codigo_produto = va.codigo_produto;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_politica_publica AS
SELECT
  vpp.codigo_produto AS [Código Produto]
  ,pp.nome AS [Política Pública]
  ,pp.descricao AS [Política Pública Descrição]
FROM politica_publica pp
JOIN (SELECT DISTINCT codigo_produto, codigo_politica_publica FROM vinculo_politica_publica)
 vpp ON vpp.codigo_politica_publica = pp.codigo_politica_publica;
 GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_objetivo_estrategico AS
SELECT
  oe.codigo_oe            AS [Código]
  ,oe.nome             AS [Nome]
  ,oe.descricao        AS [Descrição]
  ,voe.codigo_programa AS [Código Programa]
FROM objetivo_estrategico oe
JOIN vinculo_objetivo_estrategico voe ON oe.codigo_oe = voe.codigo_oe;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_categoria_economica AS
SELECT
  codigo_categoria_economica                   AS [Código Grupo]
  ,nome                                        AS [Descrição Grupo]
  ,codigo_categoria_economica || ' - ' || nome AS [Grupo Descritivo]
FROM categoria_economica;
GO;

CREATE OR ALTER VIEW dbo.v_elemento_de_despesa AS
SELECT
  codigo_elemento_de_despesa                   AS [Código Elemento]
  ,nome                                        AS [Descrição Elemento]
  ,codigo_elemento_de_despesa || ' - ' || nome AS [Elemento Descritivo]
FROM elemento_de_despesa;
GO;

CREATE OR ALTER VIEW dbo.v_grupo_de_despesa AS
SELECT
  codigo_natureza_de_despesa AS [Código Grupo]
  ,nome                      AS [Descrição Grupo]
  ,codigo_natureza_de_despesa || ' - ' || nome AS [Grupo Descritivo]
FROM grupo_de_despesa;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_acao_uo AS
SELECT DISTINCT
  codigo_acao AS [Código Ação]
  ,codigo_uo  AS [Código UO]
FROM vinculo_acao;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER VIEW dbo.v_ods AS
SELECT DISTINCT
  codigo_ods AS [Código ODS]
  ,imagem    AS [Imagem]
  ,url       AS [URL]
FROM tabelas_externas.dbo.ods;
GO;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
