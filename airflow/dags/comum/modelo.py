"""Pipeline scikit-learn — identico ao notebook da aula anterior.

Restricoes de serving (nao relaxe sem testar o /predict):
  - entrada = array posicional [area_util, area_total, quartos, banheiros, garagens];
  - sem lambda e sem FunctionTransformer com funcao local (nao desserializa no container);
  - so estimadores do proprio scikit-learn.
"""

from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline


def construir_pipeline(n_estimators=200, max_depth=12, random_state=42):
    return Pipeline(
        steps=[
            ("imputacao", SimpleImputer(strategy="median")),
            (
                "floresta",
                RandomForestRegressor(
                    n_estimators=n_estimators,
                    max_depth=max_depth,
                    random_state=random_state,
                    # n_jobs=2 para caber na VM junto com o Airflow
                    n_jobs=2,
                ),
            ),
        ]
    )
