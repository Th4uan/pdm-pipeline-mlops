-- checar_gold: a gold da aula anterior existe e tem dados utilizaveis.
-- BigQueryCheckOperator falha se QUALQUER coluna da primeira linha for falsa.
SELECT
  COUNT(*) >= 100                                     AS gold_tem_linhas,
  SAFE_DIVIDE(COUNTIF(preco > 0), COUNT(*)) >= 0.95   AS preco_valido
FROM `{{ gold_table }}`
