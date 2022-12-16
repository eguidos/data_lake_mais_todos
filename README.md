# Datalake: Proposta de implementação e consolidação de dados.

## Conteúdo deste artigo
1. Resumo;
2. Solução:
    1. ETL de dados com Apache Spark / Proposta de implementação de datalake.
    2. Desenvolvimento do componente de consolidação de dados.
    3. Proposta de schedulagem do pipeline de dados.
3. Execução;
4. Conclusão;

* Resumo
    * O conteúdo deste pipeline de **ETL** tem como objetivo demonstrar o fluxo de execução de dados e criação de uma proposta de datalake, implementando kpi's  utlizando como base **Apache Spark**. Além da aplicação de transformações para viabilizar a implementação das transformações (*que também podem conter kpis*), os dados foram consolidados em tabelas no banco **Postgres**.
    * Tecnologias que viabilizaram a realização do case:
        * [Apache Airflow](https://airflow.apache.org/)
        * [Postgres](https://www.postgresql.org/)
        * [Apache Pyspark](https://spark.apache.org/docs/latest/api/python/)
    


## **ETL de dados com Apache Spark / Proposta de implementação de datalake.**
A estrutura necessária para a execução do pipeline de ETL foi implementada seguindo o padrão semelhante a Arquitetura Orientada a Serviçõs (SOA) evidenciado abaixo. Este padrão contribui para fácil manutenção do código fonte e principalmente para escalabilidade de análises sob o dado coletado.
![DATA](/resources/project_model.png)


## **Sources**
Contém as fontes de dados disponibilizadas para o desenvolvimento do case.

## **RAW**:
Módulo que faz o processo de extração do csv disponibilizado pela organização salva a tabela para manter a carga histórica de ingestôes na tabela california_housing_train e validade a quantidade de registros: extração x dado persistido (salvo no banco), 

## Integration   
Módulo que cria flags de acordo com o intervalo de idade, e aplica conversão de nomes em colunas.

## Business
Módulo que cria uma tabela consolidada agrupando a soma da população e a média do valor de casa por idade.

## DAO
Módulo que viabiliza a escrita / leitura das tabelas no postgress.

* **Solução**
    * Para este case, foi-se desenvolvido um módulo de transformações com o framework pyspark, seguindo as orientações do [README](https://github.com/MaisTodos/challenge-data-engineering/blob/main/Desafio.md) enviado pela organização. Sendo assim, a proposta de implementação de datalake está orientada a responder as perguntas descritas no desafio. As respostas estão armazenadas nas tabelas de acordo com a sua pergunta, respectivamente:
        1. Qual a coluna com maior desvio padrão?
            - median_house_value
        2. Qual valor mínimo e o máximo?
            - Tabela: **california_housing_train** e **stddev_table**
            -  Colunas median_income e median_house_value
        3. Alteração no datatype e schema da tabela:
            - **californians_consolidated_data**
        4. Agregações:
            -  **ca_consolidated_data**

### Proposta de schedulagem do pipeline de dados.

    O serviço de schedulagem foi desenvolvido por meio do Apache Airflow, dividido entre tasks, conforme descreve a imagem abaixo. As dags desenvovlidas estão disponíveis no módulo airflow deste projeto
![SCHEDULAGEM](/resources/consolidated_dag.png)

### Execução:
1. Fazer o download deste projeto a partir da branch `guilherme_santos`.
2. Executar os comandos em um interpretador bash de sua preferência:
    1. `export AIRFLOW_HOME=~/airflow`
    1. `export SPARK_PATH='/opt/spark-3.2.0-bin-hadoop3.2'`
    1. `export PYTHONPATH="${PYTHONPATH}:${pwd}"`
    1. `export SPARK_HOME=/opt/spark`
3. Executar o script `prepair_db.sh`, que recebe como parâmetro o nome do db a ser criado:
    1. `./prepair_db.sh data_driven`
        * Este script foi desenvolvido a fim de criar e excluir usuários e tabelas no banco de dados.
4. Em seguida, executar os seguintes comandos:
    1. `sudo pip instal -r requirements`
    1. `python3 setup.py install`
    1. `airflow db init`
    1. `airflow scheduler`
    * Caso não haja nenhum erro de isntalação, o serviço de schedulagem do airflow será inicializado.

3. Abrir um novo terminal e executar novamente o passo 2 descrito nesta seção, em seguida o comando:
    1. `airflow webserver --port 8008`.
    * Caso não haja nenhum erro de isntalação, o serviço de schedulagem do airflow será inicializado.

4. Abrir o navegador de sua preferência e digitar:
    1. `localhost:8080`.
    * A tela abaixo deverá ser exibida:
    ![AIRFLOWINIT](/resources/airflow.png) 
    1. Abir a DAG e clicar no ícone de play, localizado no canto superior direito.
    1. Aguardar a execução do pipeline.
    1. OS dados estarão disponíveis para consulta nas tabelas de :
        1. californians_consolidated_data;
        2. ca_consolidated_data.
        3. stddev_table.

### Conclusão:
1. As perguntas respondidas através deste pipeline e modelo de implementação de datalake asseguram que as tabelas geradas irão garantir que os KPI's como desvio padrão, média de população, categorização da população de acordo com a latitude e longitude, desvio minimo e máximo sempre serão processados sem intervenção humana, podendo neste mesmo pipeline incluir novas fontes de dados a fim de democratizar os dados em um datalake centralizado.