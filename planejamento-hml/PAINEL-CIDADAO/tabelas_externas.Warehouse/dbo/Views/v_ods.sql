CREATE   VIEW dbo.v_ods AS
SELECT DISTINCT
  codigo_ods AS [Código ODS]
  ,imagem    AS [Imagem]
  ,url       AS [URL]
FROM ods;

GO