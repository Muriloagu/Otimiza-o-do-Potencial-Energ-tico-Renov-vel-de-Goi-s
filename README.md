
# Pipeline de Dados e Machine Learning
# INTEGRAÇÃO DE DADOS, ARMAZENAMENTO, ML e VISUALIZAÇÃO

## 📄 Visão Geral do Projeto

Este projeto tem como objetivo principal **Desenvolver um modelo de regressão que permita prever o potencial de geração de energia renovável (solar e/ou eólica) em diferentes regiões do território goiano, considerando variáveis operacionais, climáticas e de infraestrutura do sistema elétrico** Desenvolver o modelo após implementar um **pipeline de ELT (Extract, Load, Transform)** utilizando **dados abertos do Operador Nacional do Sistema Elétrico (ONS)**, **APIs de dados climáticos NASA POWER e OPEN METEO, MICROREGIÕES do estado de Goiás**. O pipeline é construído com ferramentas modernas de **Modern Data Engineering** e/ou **AWS CLOUD** para garantir a extração, carregamento e transformação eficiente e orquestrada dos dados.

A arquitetura do projeto segue a metodologia de **ELT**, onde os dados são extraídos, carregados primeiro em sua forma *raw* (bruta) e, em seguida, transformados dentro do *data 
warehouse (snowflake)* para análises posteriores.

O modelo será treinado através do **Google Colab**, dados dos resultados serão salvos em CSV e enviados para o *datawarehouse snowflake*.

## 🛠️ Tecnologias Utilizadas

| Categoria | Tecnologia | Uso no Projeto |
| :--- | :--- | :--- |
| **Fonte de Dados** | AWS S3 | Armazenamento dos dados brutos da ONS. |
| **Extração & Carga (EL)** | **Airbyte** (Local) | Conector para extrair os dados da fonte (AWS) e carregá-los no *data warehouse* (Snowflake). |
| **Data Warehouse (Load)** | **Snowflake** | Ambiente de destino para carregamento dos dados brutos e execução das transformações. |
| **Transformação (T)** | **dbt (data build tool)** | Sistema de pastas para modelagem, unificação, cruzamento e normalização dos dados dentro do Snowflake. |
| **Orquestração** | **Apache Airflow** | Agendamento e monitoramento do pipeline completo (Airbyte $\rightarrow$ dbt). |
| **Modelo Python** | **Google Colab** | Treinamento e aplicação de modelos de machine learning. |

---

## 🎯 Fase 1: Configuração e Extração

### 1. Fonte de Dados (AWS/ONS)

Os dados brutos da ONS estão acessíveis em um *bucket* no ambiente **AWS S3**.

Bases Gerais: https://dados.ons.org.br.
Base API Open-Meteo: https://open-meteo.com/.
Base API Nasa Power: https://power.larc.nasa.gov/docs/services/api/.

* **Ação:** Configurar as credenciais e o caminho (URI) do *bucket* que será consumido.
* **FATOR CAPACIDADE**: https://dados.ons.org.br/dataset/fator-capacidade-2

### 2. Configuração do Airbyte

O **Airbyte** será executado localmente (via Docker) para gerenciar o processo de Extração e Carga (**EL**).

1.  **Instalação Local:** Instalar e iniciar o Airbyte Através do docker e abctl, 
https://docs-airbyte-com.translate.goog/platform/using-airbyte/getting-started/oss-quickstart?_x_tr_sl=en&_x_tr_tl=pt&_x_tr_hl=pt&_x_tr_pto=tc
2.  **Configuração da Fonte (Source):** Criar uma **Source Connection** apontando para o *bucket* da AWS (Dados ONS) e outras bases.
3.  **Configuração do Destino (Destination):** Criar uma **Destination Connection** para o **Snowflake**, fornecendo as credenciais necessárias.
4.  **Criação da Conexão:** Criar a **Connection** que sincroniza os dados da Source para a Destination. Executar manualmente. *Não deixar agendamentos*

---

## 🏗️ Fase 2: Transformação de Dados (dbt)

### 1. Configuração do Ambiente dbt

O **dbt** será a ferramenta central de transformação dos dados dentro do Snowflake.

1.  **Instalação:** Instalar o `dbt-snowflake` e configurar o perfil de conexão (`profiles.yml`) para o Snowflake.
2.  **Estrutura do Projeto:** O projeto dbt será organizado nas pastas `models/staging` e `models/core`. *Não utilizamos schemas Mart nem Intermediate para o projeto*

**profiles.yml**
```
laboratorio_dbt:
  outputs:
    dev:
      account: SHTEGVC-SG99814
      database: LAB_PIPELINE
      password: "mudar@123"
      role: AIRBYTE_DEV
      schema: CORE
      threads: 4
      type: snowflake
      user: AIRBYTE_DEV	
      warehouse: LAB_WH_AIRBYTE
  target: dev

```

**schema.yml**
```
 sources:
  - name: STAGING
    tables:
      - name: DISPONIBILIDADE_USINA_2025_07
      - name: DISPONIBILIDADE_USINA_2025_08
      - name: DISPONIBILIDADE_USINA_2025_09
      - name: GERACAO_USINA_2025_01
      - name: GERACAO_USINA_2025_02
      - name: GERACAO_USINA_2025_03
      - name: GERACAO_USINA_2025_04
      - name: GERACAO_USINA_2025_05
      - name: GERACAO_USINA_2025_06
      - name: GERACAO_USINA_2025_07
      - name: GERACAO_USINA_2025_08
      - name: GERACAO_USINA_2025_09
      - name: FATOR_CAPACIDADE_2025_01	
      - name: FATOR_CAPACIDADE_2025_02		
      - name: FATOR_CAPACIDADE_2025_03		
      - name: FATOR_CAPACIDADE_2025_04		
      - name: FATOR_CAPACIDADE_2025_05		
      - name: FATOR_CAPACIDADE_2025_06		
      - name: FATOR_CAPACIDADE_2025_07		
      - name: FATOR_CAPACIDADE_2025_08		
      - name: FATOR_CAPACIDADE_2025_09				
models:

  - name: stg_fator_capacidade
    columns:
      - name: instante
        tests: [not_null]
      - name: id_subsistema
        tests: [not_null]

  - name: stg_usina_disponibilidade
    columns:
      - name: instante
        tests: [not_null]
      - name: id_subsistema
        tests: [not_null]

  - name: stg_geracao_usina
    columns:
      - name: din_instante
        tests: [not_null]
      - name: id_subsistema
        tests: [not_null]

  - name: dim_tempo
    columns:
      - name: id_dim_tempo 
        tests:
          - not_null
          - unique
      - name: instante 
        tests: [not_null]

  - name: dim_usina
    columns:
      - name: id_dim_usina
        tests:
          - not_null
          - unique

```

### 2. Modelagem e Transformação

O objetivo é transformar os dados brutos (carregados pelo Airbyte) em modelos limpos e prontos para análise.

* **Unificação de Tabelas:** Criar modelos SQL que **unifiquem** diferentes tabelas de dados brutos da ONS.
* **Cruzamento de Dados:** Desenvolver *joins* e modelos que **cruzem** informações (ex: Geração vs. Carga).
* **Normalização:** Garantir que as tabelas finais (*marts*) estejam **normalizadas** e sigam as boas práticas de modelagem (Data Marts), facilitando o consumo por ferramentas de BI.

```
laboratorio_dbt/
├── models/             # Arquivos SQL que definem as transformações (tabelas, views).
│   ├── staging/        # Camada de "staging" (limpeza inicial).
│   └── core/          # Camada de "marts" (modelos de negócio).
├── macros/             # Arquivos com funções SQL reutilizáveis.
├── tests/              # Arquivos de testes para validar a qualidade dos dados.
├── seeds/              # Arquivos CSV com dados estáticos que serão carregados no Data Warehouse.
├── analyses/           # Arquivos SQL para análises ad-hoc.
├── snapshots/          # Arquivos para rastrear mudanças em tabelas ao longo do tempo.
├── dbt_project.yml     # Arquivo de configuração principal do projeto.
├── profiles.yml        # Arquivo com os detalhes de conexão com o Data Warehouse.
├── README.md           # Documentação do projeto.
└── .gitignore          # Define quais arquivos e pastas serão ignorados pelo Git.
```
---

## 🔄 Fase 3: Orquestração (Airflow)

### 1. Configuração do Airflow

O **Apache Airflow** garantirá que o pipeline seja executado na ordem correta.

1.  **Configuração de Conexões:** Configurar as conexões no Airflow para se comunicar com o **Snowflake**  e orquestrar o **dbt** (via *Operator* ou *bash*).

### 2. Desenvolvimento da DAG (Directed Acyclic Graph)

O fluxo de trabalho (`laboratorio_dbt.py`) orquestrará a execução dos passos:

1.  **Task 1 (EL):** Utilizar um *Operator* para acionar a **Connection** do Airbyte.
2.  **Task 2 (T):** Utilizar o `DbtCloudOperator` ou `BashOperator` para executar o `dbt run` no projeto de transformação.
3.  **Task 3 (Tests - Opcional):** Executar `dbt test` para validar a qualidade dos dados transformados.

O pipeline não será **agendado** para nosso projeto. Rodará manualmente.

```
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

project = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/laboratorio_dbt",
)

profile = ProfileConfig(
    profile_name="laboratorio_dbt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dev", # usa a Connection criada manualmente no Airflow
        profile_args={
            "database": "DB_PROJETO_FINAL",
            "schema": "CORE",
            "warehouse": "WH_PROJETO_FINAL",
            "role": "TOOLS",
        },
    ),
)

dag = DbtDag(
    dag_id="laboratorio_dbt",
    project_config=project,
    profile_config=profile,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # ou "@daily"
)

```

##  👮 Fase 4: Treinando os Modelos disponíveis do repositório GIT

**--- 💥 Modelo Random Forest ENERGIA SOLAR**: randomforest_energia_solar.ipynb

### Etapa 1: Carrega e Prepara os Dados de Treinamento
### Etapa 2: Treinamento do Modelo de Regressão
### Etapa 3: Salva o Modelo Treinado
### Etapa 4: Usando o Modelo para Prever o Potencial Solar
### Etapa 5: Carregando arquivo com as previsões
### Etapa 6: Calculando a Média de Radiação por Município
### Etapa 7: Seleciona os Municípios com Maior Potencial
### Etapa 8: Gera o Mapa Interativo com Folium

**--- Modelo Random Forest ENERGIA EOLICA**: randomforest_energia_eolica.ipynb

### Etapa 1: Carregar e Preparar os Dados para o Modelo Eólico
### Etapa 2: Treinamento do Modelo de Regressão para Vento
### Etapa 3: Salva o Modelo Treinado
### Etapa 4: Prever o Potencial Eólico para Goiás
### Etapa 5: Gerar o Mapa de Potencial Eólico

**--- 💥 Modelo XGBoost ENERGIA SOLAR**: xgboost_energia_solar.ipynb

### Etapa 1: Carregando e Preparando os Dados
### Etapa 2: Treinamento do Modelo com XGBoost
### Etapa 3: Salvando o modelo
### Etapa 4: Prever Potencial Solar para Goiás com XGBoost
### Etapa 5: Gerando o Mapa de Potencial Solar (usando o XGBoost)

**--- Modelo XGBoost ENERGIA EOLICA**: xgboost_energia_eolica.ipynb

### Etapa 1: Carregando e Preparando os Dados
### Etapa 2: Treinamento do Modelo com XGBoost
### Etapa 3: Salvando o modelo
### Etapa 4: Prever Potencial EOLICA para Goiás com XGBoost
### Etapa 5: Gerando o Mapa de Potencial Eólico

##  👮 Fase 5: Melhoramento GRID SEARCH

**--- 💥 GRID SEARCH Random Forest ENERGIA SOLAR**: RandomForestGridSearch_energia_eolica.ipynb

## Resultados

Os resultados foram demonstrados no no próprio notebook.

Trabalhamos uma tentativa de Enviar o CSV dos resultados via AIRBYTE pra o snowflake para trabalharmos a visualização via METABASE. A importação foi um sucesso, mas existe uma limitação nos gráficos de MAPA do METABASE.

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
* Python 3.x
* [Opcional, mas recomendado] Ambiente Virtual (venv)

Para replicar este ambiente:

1.  **Clone o Repositório:**
    ```bash
    git clone [https://github.com/Muriloagu/trabalho_final_modulo_2](https://github.com/Muriloagu/Otimiza-o-do-Potencial-Energ-tico-Renov-vel-de-Goi-s/
    cd https://github.com/Muriloagu/Otimiza-o-do-Potencial-Energ-tico-Renov-vel-de-Goi-s/
    cd trabalho_final_modulo_2
    ```
2.  **Instale as Dependências:**
    Todas as dependências necessárias estão listadas no arquivo `requirements.txt`. 
    Nas pastas 6 - 7 - 8 - 9 - 10
    Instale-as usando o `pip`:
    ```bash
    ENTRE NA PASTA COM O MODELO
    cd arquivos_gerados
    Execute o arquivo _requirements para os modelos.
    pip install -r [...]_requirements.txt
    ```
2.1  **Rodando os Modelos:**
    Nas pastas 6 - 7 - 8 - 9 - 10
    [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1z9mCZ29coi9MNsCo1SrIjOWIvcGpa7oE?usp=sharing)
    
    ```bash
    Todos os arquivos no COLAB tem etapas bem definidas para guiar a execução, com explicações.
    ```
3.  **AIRBYTE e METABASE:** Instalamos localmente utilizando o quick start do aribyte:
    ```bash
    localhost:8000 - AIRBYTE
    localhost:3000 - METABASE
    ```
4.  **Inicie o DBT e Airflow:** Utilizamos o codespace do GIT demonstrado em aula para rodar o DBT - AIRFLOW:
    ```bash
    astro dev start
    ```
5.  **Execute a Pipeline no Airflow:**
    * Acesse a UI do Airflow.
    * Desbloqueie a DAG chamada `laboratorio_dbt`.
    * Acione a DAG para iniciar o fluxo de ELT.

---

## 🧑‍💻 Contato

* **Autores:** Marcelo Barros De Azevedo Vieira, Gabriel dos Santos Silva, Murilo Vieira Aguiar
* **Instituição:** IFG - Instituto Federal de Goiás 
* **Disciplinas:** Modelagem de Dados para IA, Machine Learning, Cloud Computing

---
