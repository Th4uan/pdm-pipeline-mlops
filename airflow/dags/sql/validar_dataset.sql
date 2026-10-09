-- validar_dataset: a tabela desta execucao esta apta para treino.
-- Para provocar a falha em aula: Trigger DAG w/ config {"min_linhas": 1000000}
{% set ex = ti.xcom_pull(task_ids='definir_execucao') %}
SELECT
  COUNT(*) >= {{ params.min_linhas }}       AS volume_minimo,
  COUNTIF(conjunto = 'validacao') >= 30     AS tem_validacao,
  COUNTIF(conjunto = 'teste') >= 30         AS tem_teste,
  COUNT(DISTINCT id) = COUNT(*)             AS imovel_em_um_so_conjunto,
  COUNTIF(preco IS NULL) = 0                AS alvo_sem_nulo
FROM `{{ ex['tabela'] }}`
