-- preparar_dataset (VERSAO DO ALUNO): snapshot da gold para ESTA execucao.
--   - uma linha por imovel;
--   - so as colunas do contrato (nada derivado do preco);
--   - split determinístico POR IMOVEL via hash do id: o mesmo imovel cai sempre no
--     mesmo conjunto, independente da ordem das linhas ou de quem executa.
{% set ex = ti.xcom_pull(task_ids='definir_execucao') %}
CREATE OR REPLACE TABLE `{{ ex['tabela'] }}` AS
WITH unicos AS (
  SELECT id, area_util, area_total, quartos, banheiros, garagens, preco
  FROM `{{ gold_table }}`
  WHERE preco IS NOT NULL AND preco > 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1
)
SELECT
  *,
  -- TODO 1: separar 70% treino, 15% validacao e 15% teste POR IMOVEL.
  -- Dica: MOD(ABS(FARM_FINGERPRINT(CAST(id AS STRING))), 100) da um numero de 0 a 99
  --       que e' sempre o mesmo para o mesmo id. Use-o num CASE WHEN.
  'treino' AS conjunto
FROM unicos
