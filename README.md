# B3 Analytics Engine: Pipeline de Dados para Análise de Ativos Financeiros

## 1. Introdução

Este projeto consiste em um pipeline completo de engenharia de dados (ETL – Extração, Transformação e Carga) focado na centralização e análise inteligente de dados financeiros de importantes empresas brasileiras negociadas na bolsa de valores, como Petrobras (PETR4), Vale (VALE3) e Itaú Unibanco (ITUB4).

A solução foi desenvolvida para resolver o desafio da fragmentação de informações, que normalmente exige a consulta a múltiplas fontes de dados (portais de cotações, bases históricas, APIs de dados fundamentalistas). Ao unificar esses dados, o projeto oferece uma base sólida para a geração de relatórios, dashboards e análises, proporcionando insights claros e acessíveis para a tomada de decisão de investimentos.

A arquitetura é totalmente conteinerizada com Docker e orquestrada pelo Apache Airflow, utilizando tecnologias como Apache Kafka para mensageria, MinIO como Data Lake e PostgreSQL como Data Warehouse.

---

## 2. Pré-requisitos

Para executar este projeto, é necessário ter os seguintes softwares instalados em sua máquina:

* **Docker**
* **Docker Compose**

---

## 3. Guia de Execução

Siga os passos abaixo para configurar e executar todo o ambiente e o pipeline de dados.

### Passo 1: Iniciar o Ambiente

Abra um terminal na pasta raiz do projeto e execute o seguinte comando para construir as imagens e iniciar todos os serviços conteinerizados:

```bash
docker-compose up --build
```

Aguarde o processo de inicialização. Os serviços estarão prontos quando você visualizar nos logs do terminal uma mensagem de saúde do Airflow, semelhante a esta:

```log
airflow-webserver-1  | 127.0.0.1 - - [06/Aug/2025:14:11:34 +0000] "GET /health HTTP/1.1" 200 255 "-" "curl/7.88.1"
```

Este processo pode levar aproximadamente 4 minutos, dependendo da sua máquina.

### Passo 2: Acessar a Interface do Apache Airflow

Após a inicialização completa, acesse a interface web do Airflow em seu navegador:

* **URL:** `http://localhost:8080`
* **Login:** `admin`
* **Senha:** `admin`

### Passo 3: Configurar as Conexões no Airflow

Para que o Airflow possa se comunicar com os outros serviços (MinIO e PostgreSQL), é necessário configurar duas conexões.

1.  Na barra de navegação superior, posicione o mouse sobre **"Admin"** e, no menu suspenso, clique em **"Connections"**.
2.  Na página de conexões, clique no botão azul com o ícone de **"+"** para adicionar uma nova conexão.

#### Conexão 1: MinIO (Data Lake)

Preencha o formulário com as seguintes informações:

* **Connection Id:** `minio_conn`
* **Connection Type:** `Amazon Web Services`
* **AWS Access Key ID:** `minioadmin`
* **AWS Secret Access Key:** `minioadmin`
* **Extra:** (copie e cole o JSON abaixo)
    ```json
    {
      "endpoint_url": "http://minio:9000"
    }
    ```

Clique em **"Save"**.

#### Conexão 2: PostgreSQL (Data Warehouse)

Clique novamente no botão **"+"** para adicionar a segunda conexão.

* **Connection Id:** `postgres_dw_conn`
* **Connection Type:** `Postgres`
* **Host:** `postgres`
* **Schema (Database):** `b3_dw`
* **Login:** `user`
* **Password:** `password`
* **Port:** `5432`

Clique em **"Save"** novamente.

### Passo 4: Executar os Pipelines (DAGs)

Com as conexões configuradas, retorne à página principal clicando em **"DAGs"** na barra de navegação.

Você verá duas DAGs listadas. Ative e execute-as na seguinte ordem:

1.  **`1_historical_load_dag`**: Clique no ícone de "play" (▶️) para acioná-la manualmente. Esta DAG realiza a carga dos dados históricos.
2.  **`2_incremental_market_data_dag`**: Faça o mesmo para esta DAG. Ela é responsável pela captura e processamento dos dados incrementais/em tempo real.

### Passo 5: Visualizar o Dashboard

Aguarde aproximadamente 2 minutos para que os pipelines processem os dados e os carreguem no Data Warehouse.

Após esse período, acesse o dashboard analítico em seu navegador para visualizar os dados e os gráficos:

* **URL:** `http://localhost:8501`

A partir deste ponto, o dashboard será atualizado automaticamente à medida que novos dados são processados pelo pipeline incremental.
