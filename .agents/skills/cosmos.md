---
name: Astronomer Cosmos Setup
description: Guia de como configurar o Astronomer Cosmos com dbt e Airflow utilizando a CLI do Astro.
---

# Astronomer Cosmos com Astro CLI

Esta skill define o padrão de como inicializar e estruturar o Astronomer Cosmos para orquestrar projetos de dbt utilizando o Apache Airflow gerenciado pelo Astro.

## Visão Geral
O Astronomer Cosmos transforma cada modelo do `dbt` em uma `Task` individual do Airflow de forma automática, garantindo melhor observabilidade. No Astro CLI, adotamos primordialmente a execução local usando virtual environments para evitar conflitos de dependência entre o dbt e o Airflow.

## Passo a Passo da Configuração

### 1. Inicializar Projeto Astro
Execute na raiz do projeto (ou no subdiretório de infra):
```bash
astro dev init
```

### 2. Configurar o Ambiente Virtual no Dockerfile
Crie um ambiente virtual para o dbt para evitar conflito com as bibliotecas do Airflow. Edite o `Dockerfile` gerado para conter algo semelhante a:

```dockerfile
FROM quay.io/astronomer/astro-runtime:11.3.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-athena-community dbt-core && deactivate
```
*(Altere `dbt-athena-community` pelo adapter específico do seu dwh, se necessário. O teste menciona Iceberg/Athena).*

### 3. Adicionar Cosmos como Dependência
Adicione ao arquivo `requirements.txt`:
```txt
astronomer-cosmos
```

### 4. Posicionar o Projeto dbt
O projeto dbt deverá ficar localizado dentro do diretório de dags do Airflow, ex: `dags/dbt/algum_projeto_dbt/`.

Exemplo de estrutura:
```text
├── dags/
│   ├── dbt/
│   │   └── woba_bookings_dbt/
│   │       ├── dbt_project.yml
│   │       ├── models/
│   │       └── macros/
│   └── orquestracao_cosmos_dag.py
├── Dockerfile
├── requirements.txt
└── ...
```

### 5. Criar o DAG File (`orquestracao_cosmos_dag.py`)
No código do DAG usando o Cosmos (`dags/orquestracao_cosmos_dag.py`), configure apontando para o virtual env:

```python
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping # ou Athena Profile Mapping equivalente
import os
from datetime import datetime

airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping( # IMPORTANTE: Ajustar para AWS IAM Profile se usando Athena
        conn_id="airflow_db", 
        profile_args={"schema": "public"},
    ),
)

woba_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        Path(airflow_home) / "dags/dbt/woba_bookings_dbt",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="woba_bookings_dbt_dag",
    default_args={"retries": 2},
)
```

### 6. Subir o Ambiente
Inicie com:
```bash
astro dev start
```
Acesse a interface do Airflow (por padrão em localhost:8080) e acione seu DAG mapeando os modelos dbt.
