# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Análise da Qualidade dos Hospitais nos EUA
# MAGIC
# MAGIC ###Nome: Daniela Lameirão Pinto de Abreu Rosas
# MAGIC
# MAGIC ###Objetivo do Trabalho
# MAGIC
# MAGIC O objetivo deste trabalho é construir um pipeline de dados utilizando tecnologias na nuvem para analisar a qualidade dos hospitais nos EUA. O foco é identificar os principais fatores que influenciam a satisfação do paciente, avaliar a variação da taxa de mortalidade entre diferentes tipos de hospitais, explorar a correlação entre a eficácia dos cuidados e a taxa de readmissão, e identificar os estados com os melhores e piores desempenhos em termos de segurança do atendimento hospitalar.
# MAGIC
# MAGIC ###Problema a Ser Resolvido
# MAGIC
# MAGIC Identificar e analisar os fatores que influenciam a qualidade do atendimento hospitalar nos Estados Unidos.
# MAGIC
# MAGIC #####Perguntas de Negócio
# MAGIC
# MAGIC **1. Quais são os principais fatores que influenciam a satisfação do paciente?**
# MAGIC
# MAGIC - **Objetivo**: Identificar quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente.
# MAGIC
# MAGIC **2. Como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais (e.g., governamentais, privados)?**
# MAGIC
# MAGIC - **Objetivo**: Avaliar a taxa de mortalidade para fornecer insights sobre a eficácia e a segurança dos cuidados prestados em diferentes tipos de hospitais.
# MAGIC
# MAGIC **3. Existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão?**
# MAGIC
# MAGIC - **Objetivo**: Analisar se hospitais que prestam cuidados eficazes também apresentam menores taxas de readmissão.
# MAGIC
# MAGIC **4. Quais estados têm os melhores e piores desempenhos em termos de segurança do atendimento hospitalar?**
# MAGIC
# MAGIC - **Objetivo**: Avaliar a segurança do atendimento por estado para destacar áreas geográficas que necessitam de melhorias específicas na saúde pública.
# MAGIC
# MAGIC #####Plataforma Utilizada
# MAGIC
# MAGIC A plataforma utilizada para a construção do pipeline de dados foi o Databricks Community Edition, que oferece um ambiente gratuito com algumas limitações. Todos os processos, desde a coleta até a análise dos dados, foram realizados nesta plataforma.
# MAGIC
# MAGIC ##Detalhamento
# MAGIC
# MAGIC ###1. Busca pelos Dados (E - Extração)
# MAGIC
# MAGIC Foi escolhido o conjunto de dados "Hospital General Information" disponível no Kaggle. Este conjunto de dados contém informações detalhadas sobre mais de 4.000 hospitais nos EUA, incluindo taxas de mortalidade, segurança do atendimento, experiência do paciente, entre outros indicadores de qualidade.
# MAGIC
# MAGIC #####Fonte dos dados:
# MAGIC
# MAGIC Utilizarei o arquivo "Hospital General Information.csv" 
# MAGIC O conjunto de dados foi baixado de [ https://data.medicare.gov/data/hospital-compare ]
# MAGIC Base de Dados: Foi escolhida uma base de dados do Kaggle.
# MAGIC Link para os Dados: https://www.kaggle.com/datasets/center-for-medicare-and-medicaid/hospital-ratings
# MAGIC
# MAGIC #####Campos do conjunto de dados:
# MAGIC
# MAGIC ID do provedor
# MAGIC Nome do Hospital
# MAGIC Endereço
# MAGIC Cidade
# MAGIC Estado
# MAGIC CEP
# MAGIC Nome do Condado
# MAGIC Número de telefone
# MAGIC Tipo de hospital
# MAGIC Propriedade Hospitalar
# MAGIC Serviços de emergência
# MAGIC Atende aos critérios para uso significativo de EHRs
# MAGIC Classificação geral do hospital
# MAGIC Nota de rodapé sobre a classificação geral do hospital
# MAGIC Comparação nacional de mortalidade
# MAGIC Nota de rodapé sobre comparação nacional de mortalidade
# MAGIC Comparação nacional de segurança do atendimento
# MAGIC Nota de rodapé sobre comparação nacional de segurança de cuidados
# MAGIC Comparação nacional de readmissão
# MAGIC Nota de rodapé sobre comparação nacional de readmissão
# MAGIC Comparação nacional da experiência do paciente
# MAGIC Nota de rodapé sobre comparação nacional da experiência do paciente
# MAGIC Comparação nacional da eficácia dos cuidados
# MAGIC Nota de rodapé sobre a eficácia dos cuidados de saúde
# MAGIC Comparação nacional da pontualidade do atendimento
# MAGIC Nota de rodapé sobre a oportunidade do atendimento na comparação nacional
# MAGIC Uso eficiente de comparação nacional de imagens médicas
# MAGIC Uso eficiente de comparação nacional de imagens médicas
# MAGIC
# MAGIC ###2. Coleta dos Dados (E - Extração)
# MAGIC
# MAGIC Os dados foram baixados do Kaggle e armazenados localmente. Posteriormente, foram carregados para o Databricks File System (DBFS) para facilitar o processamento e a análise na nuvem.
# MAGIC
# MAGIC Os dados foram baixados do Kaggle e armazenados localmente. Posteriormente, foram carregados para o Databricks File System (DBFS) para facilitar o processamento e a análise na nuvem.
# MAGIC
# MAGIC **Processo de Coleta**
# MAGIC
# MAGIC - Criação e configuração de um bucket no AWS S3.
# MAGIC - Upload do arquivo "Hospital General Information.csv" para o bucket S3.
# MAGIC - Montagem do bucket S3 no Databricks.
# MAGIC - Carregamento dos dados do CSV no Spark DataFrame.
# MAGIC
# MAGIC - **Fonte dos Dados**: [Kaggle](https://www.kaggle.com/datasets/amritpal24/top-1000-companies-details)
# MAGIC - **Local de Armazenamento**: 
# MAGIC - **Licença de Uso**: [Licença Kaggle](https://www.kaggle.com/datasets/amritpal24/top-1000-companies-details/license)
# MAGIC - **Plataforma**: Usarei a Plataforma Databricks para a construção do pipeline de dados, utilizando a versão Databricks Community Edition.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.2. Preparação para a extração dos dados
# MAGIC
# MAGIC Montar o bucket S3 onde os dados estão armazenados. Isso vai permitir que o Databricks acesse diretamente os arquivos no Amazon S3.
# MAGIC
# MAGIC #####2.2.1. Montar o bucket do S3 e carregar os dados do CSV para um DataFrame do Spark

# COMMAND ----------

# Configurar as credenciais da AWS
import os

os.environ["AWS_ACCESS_KEY_ID"] = "your-access-key-id"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your-secret-access-key"

# Montar o bucket S3 no Databricks
mounts = dbutils.fs.mounts()

if any(mount.mountPoint == "xxxxxxxxxxxxxxx" for mount in mounts):
    print("O bucket já está montado.")
else:
    aws_access_key_id = "xxxxxxxxxxxxxxxxx"
    aws_secret_access_key = "xxxxxxxxxxxxxxx"

    dbutils.fs.mount(
        source="xxxxxxxxxxxxxxx",
        mount_point="xxxxxxxxxxxxxxx",
        extra_configs={
            "fs.s3a.access.key": aws_access_key_id,
            "fs.s3a.secret.key": aws_secret_access_key,
        },
    )

# Carregar os dados do CSV no Spark DataFrame
file_path = "xxxxxxxxxxxxxxxxx/Hospital General Information.csv"
hospital_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Mostrar a estrutura do DataFrame
hospital_df.printSchema()

# Mostrar as primeiras linhas do DataFrame
display(hospital_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Transformação dos Dados (T - Transformação)
# MAGIC
# MAGIC Foi realizada a transformação dos dados para prepará-los para a análise. Este processo incluiu a limpeza, a seleção e a formatação dos dados conforme necessário.
# MAGIC
# MAGIC **Processos de Transformação**
# MAGIC
# MAGIC - Renomeação das colunas para português.
# MAGIC - Seleção de colunas relevantes.
# MAGIC - Limpeza de dados (remoção de valores nulos e substituição de valores inválidos).
# MAGIC - Conversão de comparações categóricas para valores numéricos.
# MAGIC - Validação dos dados (verificação de valores nulos e estatísticas descritivas).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.1. Renomear as colunas para português

# COMMAND ----------

# Renomear as colunas para português
hospital_df = (
    hospital_df.withColumnRenamed("Provider ID", "ID do Provedor")
    .withColumnRenamed("Hospital Name", "Nome do Hospital")
    .withColumnRenamed("Address", "Endereço")
    .withColumnRenamed("City", "Cidade")
    .withColumnRenamed("State", "Estado")
    .withColumnRenamed("ZIP Code", "CEP")
    .withColumnRenamed("County Name", "Nome do Condado")
    .withColumnRenamed("Phone Number", "Número de Telefone")
    .withColumnRenamed("Hospital Type", "Tipo de Hospital")
    .withColumnRenamed("Hospital Ownership", "Propriedade Hospitalar")
    .withColumnRenamed("Emergency Services", "Serviços de Emergência")
    .withColumnRenamed(
        "Meets criteria for meaningful use of EHRs",
        "Atende aos Critérios para Uso Significativo de EHRs",
    )
    .withColumnRenamed("Hospital overall rating", "Classificação Geral do Hospital")
    .withColumnRenamed(
        "Hospital overall rating footnote",
        "Nota de Rodapé sobre Classificação Geral do Hospital",
    )
    .withColumnRenamed(
        "Mortality national comparison", "Comparação Nacional de Mortalidade"
    )
    .withColumnRenamed(
        "Mortality national comparison footnote",
        "Nota de Rodapé sobre Comparação Nacional de Mortalidade",
    )
    .withColumnRenamed(
        "Safety of care national comparison",
        "Comparação Nacional de Segurança do Atendimento",
    )
    .withColumnRenamed(
        "Safety of care national comparison footnote",
        "Nota de Rodapé sobre Comparação Nacional de Segurança do Atendimento",
    )
    .withColumnRenamed(
        "Readmission national comparison", "Comparação Nacional de Readmissão"
    )
    .withColumnRenamed(
        "Readmission national comparison footnote",
        "Nota de Rodapé sobre Comparação Nacional de Readmissão",
    )
    .withColumnRenamed(
        "Patient experience national comparison",
        "Comparação Nacional da Experiência do Paciente",
    )
    .withColumnRenamed(
        "Patient experience national comparison footnote",
        "Nota de Rodapé sobre Comparação Nacional da Experiência do Paciente",
    )
    .withColumnRenamed(
        "Effectiveness of care national comparison",
        "Comparação Nacional da Efetividade do Atendimento",
    )
    .withColumnRenamed(
        "Effectiveness of care national comparison footnote",
        "Nota de Rodapé sobre Efetividade do Atendimento",
    )
    .withColumnRenamed(
        "Timeliness of care national comparison",
        "Comparação Nacional da Pontualidade do Atendimento",
    )
    .withColumnRenamed(
        "Timeliness of care national comparison footnote",
        "Nota de Rodapé sobre Pontualidade do Atendimento",
    )
    .withColumnRenamed(
        "Efficient use of medical imaging national comparison",
        "Comparação Nacional do Uso Eficiente de Imagens Médicas",
    )
    .withColumnRenamed(
        "Efficient use of medical imaging national comparison footnote",
        "Nota de Rodapé sobre Uso Eficiente de Imagens Médicas",
    )
)

# Exibir as primeiras linhas do DataFrame renomeado
display(hospital_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.2 Justificativa para Exclusão de Colunas com Muitos Valores Nulos
# MAGIC
# MAGIC Após a conversão das colunas categóricas para valores numéricos, verifiquei e justifiquei a exclusão das colunas que apresentavam muitos valores nulos:

# COMMAND ----------

# MAGIC %md
# MAGIC ####Justificativa para Exclusão de Colunas com Muitos Valores Nulos
# MAGIC
# MAGIC Após a conversão das colunas categóricas para valores numéricos, verifiquei e justifiquei a exclusão das colunas que apresentavam muitos valores nulos:
# MAGIC
# MAGIC **Para embasar a decisão de como lidar com dados ausentes, aqui estão diretrizes baseadas em fontes acadêmicas e práticas:**
# MAGIC
# MAGIC #####- Porcentagem de Valores Nulos Inferior a 10%:
# MAGIC
# MAGIC Ação: Geralmente é aceitável tratar esses valores e continuar com a análise.
# MAGIC Justificativa: Segundo Pigott (2001), uma quantidade menor de valores nulos tende a introduzir menos viés nas análises, especialmente quando a ausência de dados é aleatória (MCAR)​ (SpringerOpen)​.
# MAGIC
# MAGIC #####- Porcentagem de Valores Nulos Entre 10% e 30%:
# MAGIC
# MAGIC Ação: Pode ser necessário considerar uma abordagem de substituição (como imputação) ou uma análise mais cuidadosa.
# MAGIC Justificativa: Conforme Allison (2001) e Schafer & Graham (2002), níveis moderados de valores nulos podem ser tratados adequadamente com métodos como imputação múltipla (MI) ou análise de casos disponíveis para mitigar o impacto dos dados ausentes​ (SpringerOpen)​​ (Oxford Academic)​.
# MAGIC
# MAGIC #####- Porcentagem de Valores Nulos Superior a 30%:
# MAGIC
# MAGIC Ação: Pode ser mais prático descartar essas colunas ou perguntas relacionadas e focar em outras áreas.
# MAGIC Justificativa: De acordo com Schafer & Graham (2002), quando a porcentagem de dados ausentes é alta, a imprecisão e o viés introduzidos podem ser significativos, sendo recomendado descartar essas colunas para manter a robustez da análise​ (SpringerOpen)​​ (Oxford Academic)​​ (SpringerLink)​.
# MAGIC
# MAGIC ####- Decisão sobre Colunas com Altos Percentuais de Valores Nulos
# MAGIC
# MAGIC Para garantir a qualidade e integridade dos dados analisados, é importante lidar adequadamente com colunas que possuem altos percentuais de valores nulos. Com base na literatura e nas práticas recomendadas, tomaremos as seguintes ações:
# MAGIC
# MAGIC **- Colunas com Nulidade Acima de 10%: Ação: Excluir colunas com mais de 10% de valores nulos.**
# MAGIC
# MAGIC Justificativa: De acordo com Schafer & Graham (2002), quando a porcentagem de dados ausentes é alta, a imprecisão e o viés introduzidos podem ser significativos, tornando impraticável a imputação ou o tratamento desses valores. Assim, é recomendável descartar essas colunas para manter a robustez da análise​ (SpringerOpen)​​ (Oxford Academic)​.
# MAGIC
# MAGIC **- Colunas com Nulidade Abaixo de 10% : Ação: Tratar os valores nulos restantes.**
# MAGIC
# MAGIC Justificativa: Segundo Pigott (2001), uma quantidade menor de valores nulos tende a introduzir menos viés nas análises, especialmente quando a ausência de dados é aleatória (MCAR). Portanto, é aceitável realizar a imputação desses valores para continuar com a análise​ (SpringerOpen)​.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Identificar e Exclir as Colunas com valores nulos Acima de 10%
# MAGIC
# MAGIC - **Listar em ordem decrescente os Percentuais de nulos em cada atributo**
# MAGIC - **Mostrar a quantidade de nulos em cada coluna**
# MAGIC - **Excluir as colunas com percentual de nulos acima de 10%**
# MAGIC - **Guardar o novo dataframe**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Verificação de valores nulos
null_counts = hospital_df.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in hospital_df.columns]
)

# Calcular o total de linhas no DataFrame
total_rows = hospital_df.count()

# Calcular a porcentagem de valores nulos em cada coluna
null_percentages = null_counts.select(
    [(col(c) / total_rows * 100).alias(c + "_percent") for c in null_counts.columns]
)

# Juntar a quantidade de valores nulos e a porcentagem em um único DataFrame
null_counts_with_percentage = null_counts.crossJoin(null_percentages)

# Transformar o DataFrame em uma lista de tuplas para ordenar
null_data = []
for column in null_counts_with_percentage.columns:
    if column.endswith("_percent"):
        original_column_name = column.replace("_percent", "")
        count = null_counts_with_percentage.select(original_column_name).collect()[0][0]
        percentage = null_counts_with_percentage.select(column).collect()[0][0]
        null_data.append((original_column_name, count, percentage))

# Ordenar os dados pela porcentagem de valores nulos em ordem decrescente
null_data_sorted = sorted(null_data, key=lambda x: x[2], reverse=True)

# Criar um DataFrame com os resultados
df_null_sorted = spark.createDataFrame(
    null_data_sorted, ["Coluna", "Quantidade de Nulos", "Porcentagem de Nulidade (%)"]
)

# Exibir o DataFrame ordenado
display(df_null_sorted)

# Filtrar colunas com mais de 10% de valores nulos para exclusão
columns_to_exclude = [col[0] for col in null_data_sorted if col[2] > 10]

# Excluir essas colunas do DataFrame
df_limpo = hospital_df.drop(*columns_to_exclude)

# Verificar novamente a quantidade de valores nulos após a exclusão
null_counts_after = df_limpo.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in df_limpo.columns]
)
display(null_counts_after)

# Exibir as primeiras linhas do DataFrame limpo
display(df_limpo)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.3. Preparação da Conversão dos dados  

# COMMAND ----------

df_limpo.columns


# COMMAND ----------

from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

# Listar as colunas a serem indexadas
colunas_para_indexar = [
    "Tipo de Hospital",
    "Propriedade Hospitalar",
    "Serviços de Emergência",
    "Atende aos Critérios para Uso Significativo de EHRs",
    "Classificação Geral do Hospital",
    "Comparação Nacional de Mortalidade",
    "Comparação Nacional de Segurança do Atendimento",
    "Comparação Nacional de Readmissão",
    "Comparação Nacional da Experiência do Paciente",
    "Comparação Nacional da Efetividade do Atendimento",
    "Comparação Nacional da Pontualidade do Atendimento",
    "Comparação Nacional do Uso Eficiente de Imagens Médicas"
]

# Criar indexadores para cada coluna especificada
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_Index", handleInvalid="keep")
    for col in colunas_para_indexar
]

# Criar um pipeline com os indexadores
pipeline = Pipeline(stages=indexers)

# Ajustar o pipeline aos dados e transformar os dados
df_indexed = pipeline.fit(df_limpo).transform(df_limpo)

# Exibir a estrutura do DataFrame transformado
df_indexed.printSchema()

# Selecionar as colunas que queremos manter (excluindo as originais que foram indexadas)
colunas_manter = [col for col in df_indexed.columns if col not in colunas_para_indexar]

# Criar um novo DataFrame apenas com as colunas desejadas
df_final = df_indexed.select(colunas_manter)

# Exibir a estrutura do DataFrame final
df_final.printSchema()

# Exibir as primeiras linhas do DataFrame final para verificar a remoção das colunas originais
df_final.show()

# Se estiver usando um notebook Databricks, você pode usar display para visualizar o DataFrame
display(df_final)


# COMMAND ----------

from pyspark.sql.functions import col

# Listar todas as colunas do DataFrame
all_columns = df_final.columns

# Converter todas as colunas para string
for col_name in all_columns:
    df_final = df_final.withColumn(col_name, col(col_name).cast("string"))

# Exibir a estrutura do DataFrame atualizado
df_final.printSchema()
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Modelagem dos Dados (T - Transformação)
# MAGIC
# MAGIC Foi construído um modelo de dados em Esquema Estrela, que é uma abordagem comum para Data Warehouses. As tabelas de dimensões e fatos foram criadas e populadas de acordo com os dados disponíveis.
# MAGIC
# MAGIC ####Esquema Estrela
# MAGIC
# MAGIC **Tabelas Dimensão**
# MAGIC
# MAGIC - Dim_Provedor
# MAGIC - Dim_Estado
# MAGIC - Dim_Tipo_Hospital
# MAGIC
# MAGIC **Tabelas de Fato**
# MAGIC
# MAGIC - Fato_Satisfacao_Paciente
# MAGIC - Fato_Mortalidade_Hospital
# MAGIC - Fato_Efetividade_Readmissao
# MAGIC - Fato_Seguranca_Estado
# MAGIC
# MAGIC
# MAGIC ###Diagrama Esquema Estrela 
# MAGIC O diagrama do esquema estrela foi feito utilizando a ferramenta [GenMyModel](https://app.genmymodel.com).
# MAGIC
# MAGIC ![Diagrama Esquema Estrela](files/shared_uploads/danilameirao@gmail.com/DatabaseDiagram.png)
# MAGIC
# MAGIC ####Catálogo de Dados: 
# MAGIC Descrição Detalhada dos Dados e Seus Domínios
# MAGIC
# MAGIC ####Tabelas Fato:
# MAGIC
# MAGIC **Fato_Satisfacao_Paciente**
# MAGIC
# MAGIC - D_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
# MAGIC - Estado_Index: Identificador do estado do hospital. (VARCHAR(2)) (FK para Dim_Estado)
# MAGIC - Tipo_Hospital_Index: Identificador do tipo de hospital. (VARCHAR(10)) (FK para Dim_Tipo_Hospital)
# MAGIC - Comparacao_Seguranca_Cuidado_Index: Índice categórico representando a comparação de segurança do cuidado. (VARCHAR(10))
# MAGIC - Comparacao_Readmissao_Index: Índice categórico representando a comparação de readmissão. (VARCHAR(10))
# MAGIC - Comparacao_Experiencia_Paciente_Index: Índice categórico representando a comparação da experiência do paciente. (VARCHAR(10))
# MAGIC - Comparacao_Efetividade_Cuidado_Index: Índice categórico representando a comparação da efetividade do cuidado. (VARCHAR(10))
# MAGIC - Comparacao_Pontualidade_Cuidado_Index: Índice categórico representando a comparação da pontualidade do cuidado. (VARCHAR(10))
# MAGIC - Comparacao_Uso_Eficiente_Imagem_Index: Índice categórico representando a comparação do uso eficiente de imagens médicas. (VARCHAR(10))
# MAGIC
# MAGIC **Fato_Mortalidade_Hospital**
# MAGIC
# MAGIC - ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
# MAGIC - Propriedade_Hospitalar_Index: Identificador da propriedade hospitalar. (VARCHAR(10)) (FK para Dim_Tipo_Hospital)
# MAGIC - Comparacao_Mortalidade_Index: Índice categórico representando a comparação de mortalidade. (VARCHAR(10))
# MAGIC - Comparacao_Readmissao_Index: Índice categórico representando a comparação de readmissão. (VARCHAR(10))
# MAGIC
# MAGIC **Fato_Efetividade_Readmissao**
# MAGIC
# MAGIC - ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
# MAGIC - Comparacao_Efetividade_Cuidado_Index: Índice categórico representando a comparação da efetividade do cuidado. (VARCHAR(10))
# MAGIC - Comparacao_Readmissao_Index: Índice categórico representando a comparação de readmissão. (VARCHAR(10))
# MAGIC
# MAGIC **Fato_Seguranca_Estado**
# MAGIC
# MAGIC - ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
# MAGIC - Estado_Index: Identificador do estado do hospital. (VARCHAR(2)) (FK para Dim_Estado)
# MAGIC - Comparacao_Seguranca_Cuidado_Index: Índice categórico representando a comparação de segurança do cuidado. (VARCHAR(10)).
# MAGIC
# MAGIC ####Tabelas Dimensão
# MAGIC
# MAGIC **Dim_Provedor**
# MAGIC
# MAGIC - ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (Chave Primária)
# MAGIC
# MAGIC **Dim_Estado**
# MAGIC
# MAGIC - Estado_Index: Identificador do estado do hospital. (VARCHAR(2)) (Chave Primária)
# MAGIC
# MAGIC **Dim_Tipo_Hospital**
# MAGIC
# MAGIC - Tipo_Hospital_Index: Identificador do tipo de hospital. (VARCHAR(10))
# MAGIC - Propriedade_Hospitalar_Index: Identificador da propriedade hospitalar. (VARCHAR(10)) (Chaves Primárias Conjuntas)
# MAGIC
# MAGIC ####Linhagem dos Dados
# MAGIC
# MAGIC - Fonte dos Dados: O conjunto de dados foi baixado do portal Medicare Hospital Compare, disponível no Kaggle.
# MAGIC
# MAGIC - Conjunto de Dados Utilizado: "Hospital General Information.csv"
# MAGIC
# MAGIC - Técnica Utilizada: Os dados foram coletados do Kaggle e armazenados no Databricks File System (DBFS). Posteriormente, os dados foram carregados no Spark DataFrame e processados para análise.
# MAGIC

# COMMAND ----------

# registrar o DataFrame df_final como uma tabela temporária:

df_final.createOrReplaceTempView("df_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Carga dos Dados (L - Carga)
# MAGIC
# MAGIC Os dados transformados foram carregados para o Data Warehouse no Databricks. Utilizamos pipelines de ETL para garantir a carga eficiente dos dado
# MAGIC
# MAGIC **Processos de Carga**
# MAGIC
# MAGIC - Criação das tabelas de dimensões e de fatos.
# MAGIC - Inserção dos dados transformados nas tabelas criadas.
# MAGIC
# MAGIC #####5.1.1.  Carregar os Dados do CSV para um DataFrame do Spark

# COMMAND ----------

# MAGIC %md
# MAGIC **Limpar Diretórios**

# COMMAND ----------

# Verifique e remova o conteúdo dos diretórios
dbutils.fs.rm("dbfs:/user/hive/warehouse/dim_provedor", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/dim_estado", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/dim_tipo_hospital", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/fato_satisfacao_paciente", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/fato_mortalidade_hospital", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/fato_efetividade_readmissao", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/fato_seguranca_estado", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os nomes das colunas no DataFrame df_limpo1
# MAGIC DESCRIBE TABLE df_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tabelas Dimensão
# MAGIC
# MAGIC ####Criar a Tabela Dim_Provedor 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Dim_Provedor se já existir
# MAGIC DROP TABLE IF EXISTS Dim_Provedor;
# MAGIC -- Criar a tabela Dim_Provedor
# MAGIC CREATE TABLE Dim_Provedor (ID_Provedor STRING);
# MAGIC -- Inserir dados na tabela Dim_Provedor
# MAGIC INSERT INTO
# MAGIC   Dim_Provedor
# MAGIC SELECT
# MAGIC   DISTINCT `ID do Provedor` AS ID_Provedor
# MAGIC FROM
# MAGIC   df_final;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Dim_Provedor
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dim_Estado

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Dim_Estado se já existir
# MAGIC DROP TABLE IF EXISTS Dim_Estado;
# MAGIC -- Criar a tabela Dim_Estado
# MAGIC CREATE TABLE Dim_Estado (Estado_Index STRING);
# MAGIC -- Inserir dados na tabela Dim_Estado
# MAGIC INSERT INTO
# MAGIC   Dim_Estado
# MAGIC SELECT
# MAGIC   DISTINCT Estado AS Estado_Index
# MAGIC FROM
# MAGIC   df_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Dim_Estado
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dim_Tipo_Hospital

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Dim_Tipo_Hospital se já existir
# MAGIC DROP TABLE IF EXISTS Dim_Tipo_Hospital;
# MAGIC -- Criar a tabela Dim_Tipo_Hospital
# MAGIC CREATE TABLE Dim_Tipo_Hospital (
# MAGIC   Tipo_Hospital_Index STRING,
# MAGIC   Propriedade_Hospitalar_Index STRING
# MAGIC );
# MAGIC -- Inserir dados na tabela Dim_Tipo_Hospital
# MAGIC INSERT INTO
# MAGIC   Dim_Tipo_Hospital
# MAGIC SELECT
# MAGIC   DISTINCT `Tipo de Hospital_Index` AS Tipo_Hospital_Index,
# MAGIC   `Propriedade Hospitalar_Index` AS Propriedade_Hospitalar_Index
# MAGIC FROM
# MAGIC   df_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Dim_Tipo_Hospital
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tabelas de Fato
# MAGIC
# MAGIC ####Fato_Satisfacao_Paciente

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Fato_Satisfacao_Paciente se já existir
# MAGIC DROP TABLE IF EXISTS Fato_Satisfacao_Paciente;
# MAGIC -- Criar a tabela Fato_Satisfacao_Paciente
# MAGIC CREATE TABLE Fato_Satisfacao_Paciente (
# MAGIC   ID_Provedor STRING,
# MAGIC   Estado STRING,
# MAGIC   Tipo_Hospital STRING,
# MAGIC   Comparacao_Seguranca_Cuidado STRING,
# MAGIC   Comparacao_Readmissao STRING,
# MAGIC   Comparacao_Experiencia_Paciente STRING,
# MAGIC   Comparacao_Efetividade_Cuidado STRING,
# MAGIC   Comparacao_Pontualidade_Cuidado STRING,
# MAGIC   Comparacao_Uso_Eficiente_Imagem STRING
# MAGIC );
# MAGIC -- Inserir dados na tabela Fato_Satisfacao_Paciente
# MAGIC INSERT INTO
# MAGIC   Fato_Satisfacao_Paciente
# MAGIC SELECT
# MAGIC   DISTINCT `ID do Provedor` AS ID_Provedor,
# MAGIC   Estado,
# MAGIC   `Tipo de Hospital_Index` AS Tipo_Hospital,
# MAGIC   `Comparação Nacional de Segurança do Atendimento_Index` AS Comparacao_Seguranca_Cuidado,
# MAGIC   `Comparação Nacional de Readmissão_Index` AS Comparacao_Readmissao,
# MAGIC   `Comparação Nacional da Experiência do Paciente_Index` AS Comparacao_Experiencia_Paciente,
# MAGIC   `Comparação Nacional da Efetividade do Atendimento_Index` AS Comparacao_Efetividade_Cuidado,
# MAGIC   `Comparação Nacional da Pontualidade do Atendimento_Index` AS Comparacao_Pontualidade_Cuidado,
# MAGIC   `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index` AS Comparacao_Uso_Eficiente_Imagem
# MAGIC FROM
# MAGIC   df_final;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Fato_Satisfacao_Paciente
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fato_Mortalidade_Hospital

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Fato_Mortalidade_Hospital se já existir
# MAGIC DROP TABLE IF EXISTS Fato_Mortalidade_Hospital;
# MAGIC -- Criar a tabela Fato_Mortalidade_Hospital
# MAGIC CREATE TABLE Fato_Mortalidade_Hospital (
# MAGIC   ID_Provedor STRING,
# MAGIC   Propriedade_Hospitalar_Index STRING,
# MAGIC   Comparacao_Mortalidade_Index STRING,
# MAGIC   Comparacao_Readmissao_Index STRING
# MAGIC );
# MAGIC -- Inserir dados na tabela Fato_Mortalidade_Hospital
# MAGIC INSERT INTO
# MAGIC   Fato_Mortalidade_Hospital
# MAGIC SELECT
# MAGIC   DISTINCT `ID do Provedor` AS ID_Provedor,
# MAGIC   `Propriedade Hospitalar_Index` AS Propriedade_Hospitalar_Index,
# MAGIC   `Comparação Nacional de Mortalidade_Index` AS Comparacao_Mortalidade_Index,
# MAGIC   `Comparação Nacional de Readmissão_Index` AS Comparacao_Readmissao_Index
# MAGIC FROM
# MAGIC   df_final;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Fato_Mortalidade_Hospital
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fato_Efetividade_Readmissao

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Fato_Efetividade_Readmissao se já existir
# MAGIC DROP TABLE IF EXISTS Fato_Efetividade_Readmissao;
# MAGIC -- Criar a tabela Fato_Efetividade_Readmissao
# MAGIC CREATE TABLE Fato_Efetividade_Readmissao (
# MAGIC   ID_Provedor STRING,
# MAGIC   Comparacao_Efetividade_Cuidado STRING,
# MAGIC   Comparacao_Readmissao STRING
# MAGIC );
# MAGIC -- Inserir dados na tabela Fato_Efetividade_Readmissao
# MAGIC INSERT INTO
# MAGIC   Fato_Efetividade_Readmissao
# MAGIC SELECT
# MAGIC   DISTINCT `ID do Provedor` AS ID_Provedor,
# MAGIC   `Comparação Nacional da Efetividade do Atendimento_Index` AS Comparacao_Efetividade_Cuidado,
# MAGIC   `Comparação Nacional de Readmissão_Index` AS Comparacao_Readmissao
# MAGIC FROM
# MAGIC   df_final;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Fato_Efetividade_Readmissao
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fato_Seguranca_Estado

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eliminar a tabela Fato_Seguranca_Estado se já existir
# MAGIC DROP TABLE IF EXISTS Fato_Seguranca_Estado;
# MAGIC -- Criar a tabela Fato_Seguranca_Estado
# MAGIC CREATE TABLE Fato_Seguranca_Estado (
# MAGIC   ID_Provedor STRING,
# MAGIC   Estado STRING,
# MAGIC   Comparacao_Seguranca_Cuidado STRING
# MAGIC );
# MAGIC -- Inserir dados na tabela Fato_Seguranca_Estado
# MAGIC INSERT INTO
# MAGIC   Fato_Seguranca_Estado
# MAGIC SELECT
# MAGIC   DISTINCT `ID do Provedor` AS ID_Provedor,
# MAGIC   Estado,
# MAGIC   `Comparação Nacional de Segurança do Atendimento_Index` AS Comparacao_Seguranca_Cuidado
# MAGIC FROM
# MAGIC   df_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar os dados da tabela Dim_Provedor
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   Fato_Seguranca_Estado
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Carga para o Redshift

# COMMAND ----------

# Registrar o DataFrame df_final como uma tabela temporária para consultas SQL

df_final.createOrReplaceTempView("df_final")

# COMMAND ----------

# Carregar os dados do CSV no Spark DataFrame
file_path = "/mnt/data-storage-1421/Hospital General Information.csv"
hospital_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Transformar o DataFrame Spark para Pandas
df_final = hospital_df.toPandas()


# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# Configurar a sessão Spark
spark = SparkSession.builder.appName("RedshiftLoad").getOrCreate()

# Certifique-se de que df_final é um DataFrame Spark
# Substitua este exemplo pela sua própria lógica para criar df_final
df_final = spark.read.csv("Hospital General Information.csv", header=True, inferSchema=True)

# Transforme seu DataFrame do Spark para Pandas
pandas_df = df_final.toPandas()

# Configurar as credenciais da AWS
os.environ["AWS_ACCESS_KEY_ID"] = "xxxxxxxxxxxxxxx"
os.environ["AWS_SECRET_ACCESS_KEY"] = "xxxxxxxxxxxxxxxxxxxx"

# Detalhes do cluster Redshift
redshift_endpoint = "xxxxxxxxxxxxxxxxxxxxxx"
redshift_db = "xxxxxxxxxxxxxxx"  # Nome do banco de dados que você criou
redshift_user = "xxxxxxxxxxx"
redshift_password = "xxxxxxxxxxxxx"
redshift_port = 5439  # Porta padrão para Redshift

# Criação da string de conexão
connection_string = f"postgresql://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}"

# Criação do engine de conexão
engine = create_engine(connection_string)

# Carregue os dados para o Redshift
pandas_df.to_sql('nome_da_tabela', engine, index=False, if_exists='replace')

# Verificação de sucesso
print("Dados carregados com sucesso para o Amazon Redshift!")



# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Qualidade dos Dados : Análise Exploratória de Dados (EDA)
# MAGIC
# MAGIC **Na Análise Exploratória de Dados (EDA), verificamos os seguintes aspectos:**
# MAGIC
# MAGIC - Valores Nulos: Já realizamos essa análise anteriormente, mas vamos verificar novamente no contexto da EDA.
# MAGIC - Estatísticas Descritivas: Usar SQL para obter algumas estatísticas descritivas.
# MAGIC - Distribuição dos Dados: Verificar a distribuição dos dados para cada coluna.
# MAGIC - Valores Únicos: Verificar os valores únicos em colunas categóricas.
# MAGIC - Vamos iniciar com SQL para a EDA:
# MAGIC
# MAGIC **6.1. Valores Nulos**
# MAGIC
# MAGIC Já foi verificado anteriormente que colunas com mais de 10% de valores nulos foram excluídas.
# MAGIC
# MAGIC **6.2. Estatísticas Descritivas**
# MAGIC
# MAGIC Será usado SQL para gerar algumas estatísticas descritivas:
# MAGIC
# MAGIC **Interpretação dos Resultados**
# MAGIC
# MAGIC **Valores Mínimos (Min)**
# MAGIC
# MAGIC Todos os atributos categóricos têm um valor mínimo de 0, indicando que há hospitais que estão na categoria mais baixa para cada um dos índices.
# MAGIC
# MAGIC **Valores Máximos (Max)**
# MAGIC
# MAGIC Todos os atributos categóricos têm um valor máximo de 3, indicando que há hospitais que estão na categoria mais alta para cada um dos índices.
# MAGIC
# MAGIC **Médias (Avg)**
# MAGIC
# MAGIC - Segurança do Atendimento: A média de 0.99 sugere que, em média, os hospitais estão ligeiramente acima da categoria mais baixa.
# MAGIC - Readmissão: A média de 1.08 sugere que, em média, os hospitais estão próximos da categoria mais baixa, mas com uma pequena variação.
# MAGIC - Experiência do Paciente: A média de 1.40 indica que a experiência do paciente tende a ser um pouco melhor em comparação com a segurança e readmissão, mas ainda está longe da categoria máxima.
# MAGIC - Efetividade do Atendimento: A média de 0.43 é a mais baixa entre os índices, sugerindo que a efetividade do atendimento geralmente é classificada nas categorias mais baixas.
# MAGIC - Pontualidade do Atendimento: A média de 1.28 indica que a pontualidade do atendimento está acima da média mais baixa, mas ainda há espaço para melhorias.
# MAGIC - Uso Eficiente de Imagens: A média de 0.80 sugere que o uso eficiente de imagens médicas está mais próximo das categorias mais baixas.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Executar a consulta de análise exploratória
# MAGIC SELECT
# MAGIC   'Min' AS Estatistica,
# MAGIC   MIN(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Segurança do Atendimento_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Segurança Atendimento`,
# MAGIC   MIN(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Readmissão_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Readmissão`,
# MAGIC   MIN(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Experiência do Paciente_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Experiência Paciente`,
# MAGIC   MIN(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Efetividade do Atendimento_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Efetividade Atendimento`,
# MAGIC   MIN(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Pontualidade do Atendimento_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Pontualidade Atendimento`,
# MAGIC   MIN(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Uso Eficiente de Imagens`
# MAGIC FROM
# MAGIC   df_final
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Max' AS Estatistica,
# MAGIC   MAX(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Segurança do Atendimento_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Segurança Atendimento`,
# MAGIC   MAX(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Readmissão_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Readmissão`,
# MAGIC   MAX(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Experiência do Paciente_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Experiência Paciente`,
# MAGIC   MAX(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Efetividade do Atendimento_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Efetividade Atendimento`,
# MAGIC   MAX(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Pontualidade do Atendimento_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Pontualidade Atendimento`,
# MAGIC   MAX(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index` AS INT
# MAGIC     )
# MAGIC   ) AS `Uso Eficiente de Imagens`
# MAGIC FROM
# MAGIC   df_final
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Avg' AS Estatistica,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Segurança do Atendimento_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS `Segurança Atendimento`,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Readmissão_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS `Readmissão`,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Experiência do Paciente_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS `Experiência Paciente`,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Efetividade do Atendimento_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS `Efetividade Atendimento`,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Pontualidade do Atendimento_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS `Pontualidade Atendimento`,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS `Uso Eficiente de Imagens`
# MAGIC FROM
# MAGIC   df_final
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####6.3 Verificar as Categorias em Cada Coluna e Contar as Frequências

# COMMAND ----------

# MAGIC %md
# MAGIC **Significado dos Valores Numéricos em Cada Coluna**
# MAGIC
# MAGIC **As colunas possuem os mesmas categorias de classificação:  Segurança no Atendimento; Readmissão:
# MAGIC ;Experiência do Paciente; Efetividade do Atendimento; Pontualidade do Atendimento; Uso Eficiente de Imagens**
# MAGIC
# MAGIC - 0.0: Abaixo da média nacional
# MAGIC - 1.0: Na média nacional
# MAGIC - 2.0: Acima da média nacional 
# MAGIC - 3.0: Muito acima da média nacional
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Explicação
# MAGIC
# MAGIC **Segurança no Atendimento:**
# MAGIC
# MAGIC - A maioria dos hospitais está "Abaixo da média nacional" (2168).
# MAGIC - Poucos hospitais estão "Muito acima da média nacional" (664).
# MAGIC
# MAGIC **Readmissão:**
# MAGIC
# MAGIC - A maioria dos hospitais está "Abaixo da média nacional" (2119).
# MAGIC - Poucos hospitais estão "Muito acima da média nacional" (810).
# MAGIC
# MAGIC **Experiência do Paciente:**
# MAGIC
# MAGIC - A distribuição é mais equilibrada, mas ainda assim a maioria está "Na média nacional" (1214).
# MAGIC
# MAGIC **Efetividade do Atendimento:**
# MAGIC
# MAGIC - A grande maioria dos hospitais está "Abaixo da média nacional" (3238).
# MAGIC
# MAGIC **Pontualidade do Atendimento:**
# MAGIC
# MAGIC - A distribuição é relativamente equilibrada, mas a maioria está "Abaixo da média nacional" (1554).
# MAGIC
# MAGIC **Uso Eficiente de Imagens:**
# MAGIC
# MAGIC - A maioria dos hospitais está "Abaixo da média nacional" (2051).
# MAGIC
# MAGIC Essa análise nos mostra que a maioria dos hospitais está na média ou abaixo da média nacional em todos os aspectos, indicando áreas de melhoria no atendimento hospitalar.
# MAGIC
# MAGIC **Conclusão da Análise de Qualidade dos Dados**
# MAGIC
# MAGIC Os dados apresentam variações significativas entre os hospitais em relação aos índices avaliados. 
# MAGIC
# MAGIC A distribuição dos valores indica que há mais hospitais com desempenho abaixo da média nacional em diversas categorias.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH categorias AS (
# MAGIC   SELECT
# MAGIC     'Seguranca Atendimento' AS Categoria,
# MAGIC     `Comparação Nacional de Segurança do Atendimento_Index` AS Valor,
# MAGIC     COUNT(*) AS Quantidade
# MAGIC   FROM
# MAGIC     df_final
# MAGIC   GROUP BY
# MAGIC     `Comparação Nacional de Segurança do Atendimento_Index`
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Readmissao' AS Categoria,
# MAGIC     `Comparação Nacional de Readmissão_Index` AS Valor,
# MAGIC     COUNT(*) AS Quantidade
# MAGIC   FROM
# MAGIC     df_final
# MAGIC   GROUP BY
# MAGIC     `Comparação Nacional de Readmissão_Index`
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Experiencia Paciente' AS Categoria,
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index` AS Valor,
# MAGIC     COUNT(*) AS Quantidade
# MAGIC   FROM
# MAGIC     df_final
# MAGIC   GROUP BY
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index`
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Efetividade Atendimento' AS Categoria,
# MAGIC     `Comparação Nacional da Efetividade do Atendimento_Index` AS Valor,
# MAGIC     COUNT(*) AS Quantidade
# MAGIC   FROM
# MAGIC     df_final
# MAGIC   GROUP BY
# MAGIC     `Comparação Nacional da Efetividade do Atendimento_Index`
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Pontualidade Atendimento' AS Categoria,
# MAGIC     `Comparação Nacional da Pontualidade do Atendimento_Index` AS Valor,
# MAGIC     COUNT(*) AS Quantidade
# MAGIC   FROM
# MAGIC     df_final
# MAGIC   GROUP BY
# MAGIC     `Comparação Nacional da Pontualidade do Atendimento_Index`
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Uso Eficiente de Imagens' AS Categoria,
# MAGIC     `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index` AS Valor,
# MAGIC     COUNT(*) AS Quantidade
# MAGIC   FROM
# MAGIC     df_final
# MAGIC   GROUP BY
# MAGIC     `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index`
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   categorias PIVOT (
# MAGIC     SUM(Quantidade) FOR Categoria IN (
# MAGIC       'Seguranca Atendimento' AS `Segurança Atendimento`,
# MAGIC       'Readmissao' AS `Readmissão`,
# MAGIC       'Experiencia Paciente' AS `Experiência Paciente`,
# MAGIC       'Efetividade Atendimento' AS `Efetividade Atendimento`,
# MAGIC       'Pontualidade Atendimento' AS `Pontualidade Atendimento`,
# MAGIC       'Uso Eficiente de Imagens' AS `Uso Eficiente de Imagens`
# MAGIC     )
# MAGIC   )
# MAGIC ORDER BY
# MAGIC   Valor
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Histogramas com Curva KDE
# MAGIC
# MAGIC Os histogramas com a curva KDE (Kernel Density Estimation) mostram a distribuição das categorias em cada coluna categórica do DataFrame.
# MAGIC
# MAGIC **Comparação Nacional de Segurança do Atendimento_Index_Index**
# MAGIC
# MAGIC - Distribuição: A maior parte dos hospitais está na categoria 0, seguida pelas categorias 1 e 2. A categoria 3 tem a menor frequência.
# MAGIC - Interpretação: Isso indica que a maioria dos hospitais está classificada abaixo ou na média nacional de segurança do atendimento, com menos hospitais sendo classificados como acima da média.
# MAGIC
# MAGIC **Comparação Nacional de Readmissão_Index_Index**
# MAGIC
# MAGIC - Distribuição: A maioria dos hospitais está na categoria 0, com frequências decrescentes nas categorias 1, 2 e 3.
# MAGIC - Interpretação: Semelhante à segurança do atendimento, a maioria dos hospitais está classificada abaixo ou na média nacional em termos de readmissão.
# MAGIC
# MAGIC **Comparação Nacional da Experiência do Paciente_Index_Index**
# MAGIC
# MAGIC - Distribuição: A distribuição é mais equilibrada entre as categorias, com uma ligeira predominância da categoria 1.
# MAGIC - Interpretação: A experiência do paciente varia mais entre os hospitais, mas ainda assim, muitos estão na média ou abaixo.
# MAGIC
# MAGIC **Comparação Nacional da Efetividade do Atendimento_Index_Index**
# MAGIC
# MAGIC - Distribuição: A maioria dos hospitais está na categoria 0, com poucas ocorrências nas categorias 2 e 3.
# MAGIC - Interpretação: A efetividade do atendimento é predominantemente classificada como na média ou abaixo da média nacional.
# MAGIC
# MAGIC **Comparação Nacional da Pontualidade do Atendimento_Index_Index**
# MAGIC
# MAGIC - Distribuição: A distribuição é relativamente equilibrada entre as categorias 0, 1 e 2, com a categoria 3 tendo menos frequências.
# MAGIC - Interpretação: A pontualidade do atendimento é variável, com muitos hospitais ainda na média ou abaixo.
# MAGIC
# MAGIC **Comparação Nacional do Uso Eficiente de Imagens Médicas_Index_Index**
# MAGIC
# MAGIC - Distribuição: Há uma distribuição relativamente uniforme entre as categorias, com uma leve predominância da categoria 1.
# MAGIC - Interpretação: O uso eficiente de imagens médicas varia consideravelmente, com muitos hospitais classificados na média ou abaixo.
# MAGIC
# MAGIC ###Box Plots
# MAGIC
# MAGIC Os box plots fornecem uma visão clara da distribuição dos dados, mostrando a mediana, quartis e possíveis outliers.
# MAGIC
# MAGIC **Comparação Nacional de Segurança do Atendimento_Index_Index**
# MAGIC
# MAGIC - Box Plot: A mediana está na categoria 0, com um intervalo interquartil que vai até a categoria 2. Existem alguns outliers nas categorias superiores.
# MAGIC - Interpretação: A maioria dos hospitais tem uma segurança do atendimento na média ou abaixo, mas há alguns que se destacam positivamente.
# MAGIC - Ação: Verificar a causa dos outliers superiores e determinar se representam práticas excepcionais ou erros de dados.
# MAGIC
# MAGIC **Comparação Nacional de Readmissão_Index_Index**
# MAGIC
# MAGIC - Box Plot: Semelhante ao de segurança do atendimento, a mediana está na categoria 0 com outliers nas categorias superiores.
# MAGIC - Interpretação: A readmissão é predominantemente na média ou abaixo, com alguns hospitais com melhor performance.
# MAGIC - Ação: Investigar os outliers para entender as práticas que levam a uma menor readmissão e considerar a remoção de outliers negativos se forem erros.
# MAGIC
# MAGIC **Comparação Nacional da Experiência do Paciente_Index_Index**
# MAGIC
# MAGIC - Box Plot: A mediana está um pouco acima da categoria 1, com uma maior variabilidade nos dados.
# MAGIC - Interpretação: A experiência do paciente é mais distribuída, com muitos hospitais na média ou acima.
# MAGIC - Ação: Focar na análise dos hospitais que estão significativamente abaixo da mediana para melhorar a experiência do paciente.
# MAGIC
# MAGIC **Comparação Nacional da Efetividade do Atendimento_Index_Index**
# MAGIC
# MAGIC - Box Plot: A mediana está na categoria 0, com poucos outliers.
# MAGIC - Interpretação: A efetividade do atendimento é predominantemente classificada como na média ou abaixo.
# MAGIC - Ação: Examinar os hospitais com outliers superiores para identificar boas práticas e verificar possíveis erros nos outliers inferiores.
# MAGIC
# MAGIC **Comparação Nacional da Pontualidade do Atendimento_Index_Index**
# MAGIC
# MAGIC - Box Plot: A mediana está na categoria 1, com uma distribuição equilibrada.
# MAGIC - Interpretação: A pontualidade do atendimento é variável, com muitos hospitais na média ou acima.
# MAGIC - Ação: Analisar os outliers para entender os fatores que contribuem para uma melhor pontualidade e replicar essas práticas.
# MAGIC
# MAGIC **Comparação Nacional do Uso Eficiente de Imagens Médicas_Index_Index**
# MAGIC
# MAGIC - Box Plot: A mediana está na categoria 1, com uma distribuição relativamente uniforme.
# MAGIC - Interpretação: O uso eficiente de imagens médicas varia consideravelmente entre os hospitais.
# MAGIC - Ação: Identificar hospitais com desempenho inferior para recomendar melhorias no uso eficiente de imagens médicas.
# MAGIC
# MAGIC ###Verificação da Distribuição Normal
# MAGIC A verificação da distribuição normal ajuda a entender se os dados seguem uma distribuição normal ou se apresentam uma distribuição diferente.
# MAGIC
# MAGIC **Comparação Nacional de Segurança do Atendimento_Index_Index**
# MAGIC
# MAGIC - Distribuição: A curva KDE mostra picos em cada categoria, indicando que os dados são categóricos e não seguem uma distribuição normal contínua.
# MAGIC
# MAGIC **Comparação Nacional de Readmissão_Index_Index**
# MAGIC
# MAGIC - Distribuição: Semelhante à segurança do atendimento, os dados são categóricos e apresentam picos em cada categoria.
# MAGIC
# MAGIC **Comparação Nacional da Experiência do Paciente_Index_Index**
# MAGIC
# MAGIC - Distribuição: A distribuição é mais equilibrada entre as categorias, com picos em cada categoria.
# MAGIC
# MAGIC **Comparação Nacional da Efetividade do Atendimento_Index_Index**
# MAGIC
# MAGIC - Distribuição: Predominância na categoria 0, com picos menores nas outras categorias.
# MAGIC
# MAGIC **Comparação Nacional da Pontualidade do Atendimento_Index_Index**
# MAGIC
# MAGIC - Distribuição: Distribuição relativamente equilibrada entre as categorias, com picos em cada categoria.
# MAGIC
# MAGIC **Comparação Nacional do Uso Eficiente de Imagens Médicas_Index_Index**
# MAGIC
# MAGIC - Distribuição: Picos em cada categoria, indicando uma distribuição categórica.
# MAGIC
# MAGIC
# MAGIC ###Conclusão da Análise da Qualidade dos Dados
# MAGIC
# MAGIC - **Verificação de Valores Nulos**
# MAGIC
# MAGIC Após a verificação e tratamento dos valores nulos, o conjunto de dados foi aprimorado para garantir que apenas as colunas com dados completos ou com uma quantidade mínima de valores nulos fossem mantidas. Isso assegura que a análise subsequente seja confiável e representativa.
# MAGIC
# MAGIC - **Verificação de Valores Fora dos Limites Esperados**
# MAGIC
# MAGIC Foi realizada uma análise dos valores únicos nas colunas categóricas. As categorias foram verificadas e encontraram-se dentro dos limites esperados (0 a 3), confirmando que os dados estão bem categorizados.
# MAGIC
# MAGIC - **Análise de Outliers**
# MAGIC
# MAGIC Os gráficos box plot foram utilizados para identificar a presença de outliers nas diversas colunas categóricas. Observou-se que:
# MAGIC
# MAGIC Comparação Nacional de Segurança do Atendimento: Apresenta outliers significativos, indicando que alguns hospitais têm desempenho notavelmente diferente da maioria.
# MAGIC
# MAGIC Comparação Nacional de Readmissão: Outliers presentes, sugerindo variabilidade na taxa de readmissão.
# MAGIC
# MAGIC Comparação Nacional da Experiência do Paciente: Distribuição variada com outliers, indicando diferentes níveis de satisfação dos pacientes.
# MAGIC
# MAGIC Comparação Nacional da Efetividade do Atendimento: Alguns outliers identificados, refletindo variabilidade na efetividade do atendimento.
# MAGIC
# MAGIC Comparação Nacional da Pontualidade do Atendimento: Variabilidade moderada com outliers, mostrando diferenças na pontualidade.
# MAGIC
# MAGIC Comparação Nacional do Uso Eficiente de Imagens Médicas: Presença de outliers, sugerindo diferentes práticas na utilização de imagens médicas.
# MAGIC
# MAGIC - **Análise de Distribuição**
# MAGIC
# MAGIC A análise da distribuição dos dados através de histogramas revelou que a maioria das colunas categóricas segue uma distribuição não normal, com a maioria dos valores concentrados nas categorias inferiores (0 e 1). Isso indica que muitos hospitais tendem a ter desempenhos na média ou abaixo dela.
# MAGIC
# MAGIC ###Conclusão Geral
# MAGIC
# MAGIC A qualidade dos dados é satisfatória para a análise pretendida. Apesar da presença de outliers e algumas distribuições não normais, os dados são robustos o suficiente para fornecer insights significativos sobre os fatores que influenciam a satisfação do paciente. As etapas de limpeza e transformação garantiram a integridade e a representatividade dos dados, permitindo uma análise confiável e detalhada.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col
import pandas as pd

# Configurações do Matplotlib
%matplotlib inline

# Definir as colunas categóricas
categorical_columns = [
    "Comparação Nacional de Segurança do Atendimento_Index", 
    "Comparação Nacional de Readmissão_Index", 
    "Comparação Nacional da Experiência do Paciente_Index", 
    "Comparação Nacional da Efetividade do Atendimento_Index", 
    "Comparação Nacional da Pontualidade do Atendimento_Index", 
    "Comparação Nacional do Uso Eficiente de Imagens Médicas_Index"
]

# Selecionar as colunas categóricas e convertê-las para um DataFrame do Pandas
pandas_df = df_final.select(categorical_columns).toPandas()

# Convertendo as colunas para o tipo numérico
pandas_df = pandas_df.apply(pd.to_numeric, errors='coerce')

# Função para plotar histogramas
def plot_histograms(df, columns):
    n_cols = 2
    n_rows = (len(columns) + n_cols - 1) // n_cols  # Calcula o número de linhas necessárias
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, n_rows * 5))
    axes = axes.flatten()
    
    for i, column in enumerate(columns):
        sns.histplot(df[column], kde=True, ax=axes[i])
        axes[i].set_title(f'Distribuição de {column}')
        axes[i].set_xlabel(column)
        axes[i].set_ylabel('Frequência')
    
    # Remover eixos não utilizados
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])
    
    plt.tight_layout()
    plt.show()

# Função para plotar boxplots
def plot_boxplots(df, columns):
    n_cols = 2
    n_rows = (len(columns) + n_cols - 1) // n_cols  # Calcula o número de linhas necessárias
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, n_rows * 5))
    axes = axes.flatten()
    
    for i, column in enumerate(columns):
        sns.boxplot(x=df[column], ax=axes[i])
        axes[i].set_title(f'Boxplot de {column}')
        axes[i].set_xlabel(column)
    
    # Remover eixos não utilizados
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])
    
    plt.tight_layout()
    plt.show()

# Função para verificar a distribuição normal
def check_normal_distribution(df, columns):
    n_cols = 2
    n_rows = (len(columns) + n_cols - 1) // n_cols
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, n_rows * 5))
    axes = axes.flatten()
    
    for i, column in enumerate(columns):
        sns.histplot(df[column], kde=True, stat="density", linewidth=0, ax=axes[i])
        sns.kdeplot(df[column], color="r", linewidth=2, ax=axes[i])
        axes[i].set_title(f'Verificação de Distribuição Normal para {column}')
        axes[i].set_xlabel(column)
        axes[i].set_ylabel('Densidade')
    
    # Remover eixos não utilizados
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])
    
    plt.tight_layout()
    plt.show()

# Executar as funções para gerar os gráficos
plot_histograms(pandas_df, categorical_columns)
plot_boxplots(pandas_df, categorical_columns)
check_normal_distribution(pandas_df, categorical_columns)





# COMMAND ----------

# MAGIC %md
# MAGIC ###6.b. Solução do Problema
# MAGIC
# MAGIC Com os dados limpos e validados, passamos à análise para responder às perguntas de negócio delineadas no objetivo do trabalho.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consultas SQL para Responder às Perguntas de Negócio

# COMMAND ----------

# Registrar o DataFrame como uma tabela temporária para consultas SQL
df_final.createOrReplaceTempView("df_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ###b.1) Quais são os principais fatores que influenciam a satisfação do paciente?
# MAGIC
# MAGIC - **Objetivo**: Identificar quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calcular a correlação entre a satisfação do paciente e outros fatores
# MAGIC SELECT
# MAGIC   CORR(
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index`,
# MAGIC     `Comparação Nacional de Segurança do Atendimento_Index`
# MAGIC   ) AS corr_Seguranca_Atendimento,
# MAGIC   CORR(
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index`,
# MAGIC     `Comparação Nacional de Readmissão_Index`
# MAGIC   ) AS corr_Readmissao,
# MAGIC   CORR(
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index`,
# MAGIC     `Comparação Nacional da Efetividade do Atendimento_Index`
# MAGIC   ) AS corr_Efetividade_Atendimento,
# MAGIC   CORR(
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index`,
# MAGIC     `Comparação Nacional da Pontualidade do Atendimento_Index`
# MAGIC   ) AS corr_Pontualidade_Atendimento,
# MAGIC   CORR(
# MAGIC     `Comparação Nacional da Experiência do Paciente_Index`,
# MAGIC     `Comparação Nacional do Uso Eficiente de Imagens Médicas_Index`
# MAGIC   ) AS corr_Uso_Eficiente_Imagens
# MAGIC FROM
# MAGIC   df_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pergunta 1: Quais são os principais fatores que influenciam a satisfação do paciente?
# MAGIC
# MAGIC ####Análise dos Resultados
# MAGIC
# MAGIC Os valores de correlação entre a satisfação do paciente (Comparação Nacional da Experiência do Paciente_Index_Index) e os outros fatores são os seguintes:
# MAGIC
# MAGIC - Segurança do Atendimento (corr_Seguranca_Atendimento): 0.6010520704707379
# MAGIC
# MAGIC - Readmissão (corr_Readmissao): 0.2284856558523016
# MAGIC
# MAGIC - Efetividade do Atendimento (corr_Efetividade_Atendimento): -0.2308621917719162
# MAGIC
# MAGIC - Pontualidade do Atendimento (corr_Pontualidade_Atendimento): 0.2456645259289548
# MAGIC
# MAGIC - Uso Eficiente de Imagens (corr_Uso_Eficiente_Imagens): -0.08022861962049851
# MAGIC
# MAGIC **Interpretação dos Resultados**
# MAGIC
# MAGIC **Segurança do Atendimento (0.60):**
# MAGIC
# MAGIC Interpretação: Há uma correlação positiva moderada a forte entre a segurança do atendimento e a satisfação do paciente. Isso significa que, conforme a segurança do atendimento melhora, a satisfação do paciente tende a aumentar.
# MAGIC Conclusão: A segurança do atendimento é um fator significativo que influencia positivamente a satisfação do paciente.
# MAGIC
# MAGIC **Readmissão (0.23):**
# MAGIC
# MAGIC Interpretação: Existe uma correlação positiva fraca entre a readmissão e a satisfação do paciente. Isso sugere que, embora haja alguma relação, não é tão forte quanto outros fatores.
# MAGIC Conclusão: A readmissão tem um impacto menor na satisfação do paciente, mas ainda é relevante.
# MAGIC
# MAGIC **Efetividade do Atendimento (-0.23):**
# MAGIC
# MAGIC Interpretação: Há uma correlação negativa fraca entre a efetividade do atendimento e a satisfação do paciente. Isso indica que, conforme a efetividade do atendimento aumenta, a satisfação do paciente tende a diminuir, embora a relação não seja muito forte.
# MAGIC Conclusão: Este resultado é contra-intuitivo e pode requerer uma análise mais detalhada para entender melhor a relação entre a efetividade do atendimento e a satisfação do paciente.
# MAGIC
# MAGIC **Pontualidade do Atendimento (0.25):**
# MAGIC
# MAGIC Interpretação: Existe uma correlação positiva fraca entre a pontualidade do atendimento e a satisfação do paciente. Isso sugere que, conforme a pontualidade melhora, a satisfação do paciente tende a aumentar.
# MAGIC Conclusão: A pontualidade do atendimento é um fator relevante que influencia a satisfação do paciente, embora não de maneira muito forte.
# MAGIC
# MAGIC **Uso Eficiente de Imagens (-0.08):**
# MAGIC
# MAGIC Interpretação: Há uma correlação negativa muito fraca entre o uso eficiente de imagens e a satisfação do paciente. Isso indica que a relação é praticamente inexistente.
# MAGIC Conclusão: O uso eficiente de imagens médicas tem um impacto muito pequeno ou nulo na satisfação do paciente.
# MAGIC
# MAGIC ###Resposta 1
# MAGIC
# MAGIC **Os principais fatores que mais influenciam a satisfação do paciente foram:**
# MAGIC
# MAGIC - Segurança do Atendimento: É o fator mais influente na satisfação do paciente, com uma correlação positiva moderada a forte.
# MAGIC
# MAGIC - Readmissão e Pontualidade do Atendimento: Ambos têm uma correlação positiva fraca com a satisfação do paciente, sugerindo que são relevantes, mas não os principais fatores.
# MAGIC
# MAGIC - Efetividade do Atendimento: A correlação negativa fraca é um achado interessante que merece uma investigação mais aprofundada para entender melhor a relação.
# MAGIC
# MAGIC - Uso Eficiente de Imagens: Tem uma correlação negativa muito fraca, indicando que não é um fator significativo na satisfação do paciente.
# MAGIC
# MAGIC **Estes resultados fornecem insights valiosos sobre quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente e podem guiar esforços de melhoria na área de saúde.**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pergunta 2. Como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais (e.g., governamentais, privados)?
# MAGIC
# MAGIC Objetivo: Avaliar a taxa de mortalidade para fornecer insights sobre a eficácia e a segurança dos cuidados prestados em diferentes tipos de hospitais.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Resposta
# MAGIC
# MAGIC Gráfico de Barras: Média da Taxa de Mortalidade por Tipo de Hospital
# MAGIC
# MAGIC **Physician:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.91
# MAGIC
# MAGIC Comentário: Hospitais com classificação de "Physician" apresentam a maior taxa de mortalidade média entre todos os tipos de hospitais analisados. Isso pode indicar um potencial problema na qualidade do atendimento ou outras questões estruturais que impactam negativamente a mortalidade.
# MAGIC
# MAGIC **Voluntary non-profit - Other:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.72
# MAGIC
# MAGIC Comentário: Hospitais voluntários sem fins lucrativos, classificados como "Other", também apresentam uma taxa de mortalidade relativamente alta. Isso pode sugerir que, apesar de serem instituições sem fins lucrativos, há desafios em manter a qualidade do atendimento.
# MAGIC
# MAGIC **Proprietary:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.71
# MAGIC
# MAGIC Comentário: Hospitais proprietários têm uma taxa de mortalidade próxima à dos hospitais voluntários. Este grupo inclui hospitais com fins lucrativos, onde a qualidade do atendimento pode ser impactada pela busca por lucros.
# MAGIC
# MAGIC **Government - State:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.69
# MAGIC
# MAGIC Comentário: Hospitais estaduais governamentais apresentam uma taxa de mortalidade um pouco menor que os grupos anteriores. Isso pode refletir uma maior regulamentação e padrões de qualidade implementados a nível estadual.
# MAGIC
# MAGIC **Government - Local:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.67
# MAGIC
# MAGIC Comentário: Hospitais locais do governo apresentam uma taxa de mortalidade similar à dos hospitais estaduais, sugerindo que a administração local pode ter uma influência positiva na qualidade do atendimento.
# MAGIC
# MAGIC **Government - Federal:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.65
# MAGIC
# MAGIC Comentário: Hospitais federais mostram uma taxa de mortalidade um pouco menor, o que pode ser resultado de políticas e regulamentos federais mais rigorosos.
# MAGIC
# MAGIC **Private:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.59
# MAGIC
# MAGIC Comentário: Hospitais privados apresentam uma taxa de mortalidade menor em comparação com muitos dos tipos anteriores, possivelmente devido a melhores recursos e infraestrutura.
# MAGIC
# MAGIC **Voluntary non-profit - Private:**
# MAGIC
# MAGIC Média da Taxa de Mortalidade: 0.57
# MAGIC
# MAGIC Comentário: Hospitais voluntários privados sem fins lucrativos têm a menor taxa de mortalidade média, sugerindo que este modelo de hospital pode proporcionar um atendimento de qualidade superior devido ao foco na saúde dos pacientes em vez de lucros.
# MAGIC
# MAGIC ###Conclusão
# MAGIC
# MAGIC Os resultados indicam que a mortalidade hospitalar varia significativamente entre diferentes tipos de hospitais. Hospitais "Physician" e "Voluntary non-profit - Other" têm as maiores taxas de mortalidade, enquanto "Private" e "Voluntary non-profit - Private" têm as menores. Isso sugere que a estrutura organizacional e o modelo de financiamento de um hospital podem ter um impacto significativo na qualidade do atendimento e, consequentemente, na taxa de mortalidade dos pacientes.

# COMMAND ----------

df_final.createOrReplaceTempView("df_final")

# COMMAND ----------

# MAGIC %md
# MAGIC Esta consulta agrupa os hospitais por tipo de propriedade (governamental, privado, etc.) e calcula a média da taxa de mortalidade para cada grupo. O resultado será ordenado da maior para a menor taxa de mortalidade.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calcular a média da taxa de mortalidade para cada tipo de hospital
# MAGIC SELECT
# MAGIC   `Propriedade Hospitalar_Index` AS Tipo_Hospital,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Mortalidade_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS Media_Taxa_Mortalidade
# MAGIC FROM
# MAGIC   df_final
# MAGIC GROUP BY
# MAGIC   `Propriedade Hospitalar_Index`
# MAGIC ORDER BY
# MAGIC   Media_Taxa_Mortalidade DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Tabela de mapeamento:**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMP VIEW Tipo_Hospital_Map AS
# MAGIC SELECT
# MAGIC   0 AS Tipo_Hospital_Index,
# MAGIC   'Government - Federal' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   1 AS Tipo_Hospital_Index,
# MAGIC   'Government - State' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   2 AS Tipo_Hospital_Index,
# MAGIC   'Government - Local' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   3 AS Tipo_Hospital_Index,
# MAGIC   'Private' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   4 AS Tipo_Hospital_Index,
# MAGIC   'Voluntary non-profit - Private' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   5 AS Tipo_Hospital_Index,
# MAGIC   'Voluntary non-profit - Other' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   6 AS Tipo_Hospital_Index,
# MAGIC   'Physician' AS Tipo_Hospital
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   7 AS Tipo_Hospital_Index,
# MAGIC   'Proprietary' AS Tipo_Hospital;

# COMMAND ----------

# MAGIC %md
# MAGIC **Junção da tabela de mapeamento com os resultados para obter a tradução dos valores categóricos:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calcular a média da taxa de mortalidade para cada tipo de hospital com a tradução dos valores categóricos
# MAGIC SELECT
# MAGIC   m.Tipo_Hospital,
# MAGIC   AVG(
# MAGIC     CAST(
# MAGIC       f.`Comparação Nacional de Mortalidade_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS Media_Taxa_Mortalidade
# MAGIC FROM
# MAGIC   df_final f
# MAGIC   JOIN Tipo_Hospital_Map m ON f.`Propriedade Hospitalar_Index` = m.Tipo_Hospital_Index
# MAGIC GROUP BY
# MAGIC   m.Tipo_Hospital
# MAGIC ORDER BY
# MAGIC   Media_Taxa_Mortalidade DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Execute a consulta SQL e converta o resultado em um DataFrame do Pandas**

# COMMAND ----------

result = spark.sql(
    """
    SELECT 
        m.Tipo_Hospital,
        AVG(CAST(f.`Comparação Nacional de Mortalidade_Index` AS DOUBLE)) AS Media_Taxa_Mortalidade
    FROM 
        df_final f
    JOIN 
        Tipo_Hospital_Map m
    ON 
        f.`Propriedade Hospitalar_Index` = m.Tipo_Hospital_Index
    GROUP BY 
        m.Tipo_Hospital
    ORDER BY 
        Media_Taxa_Mortalidade DESC
    """
).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC **Plotar o gráfico de barras com a biblioteca matplotlib**

# COMMAND ----------

import matplotlib.pyplot as plt

# Plotar gráfico de barras
plt.figure(figsize=(10, 6))
bars = plt.bar(
    result["Tipo_Hospital"], result["Media_Taxa_Mortalidade"], color="skyblue"
)
plt.xlabel("Tipo de Hospital")
plt.ylabel("Média da Taxa de Mortalidade")
plt.title("Média da Taxa de Mortalidade por Tipo de Hospital")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()

# Adicionar os valores nas barras
for bar in bars:
    yval = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        yval + 0.01,
        round(yval, 2),
        ha="center",
        va="bottom",
    )

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão?
# MAGIC
# MAGIC Objetivo: Analisar se hospitais que prestam cuidados eficazes também apresentam menores taxas de readmissão.
# MAGIC
# MAGIC ###Resposta 3:
# MAGIC
# MAGIC **Correlação entre Efetividade do Atendimento e Taxa de Readmissão: -0.04864618495323882**
# MAGIC
# MAGIC A correlação entre a efetividade do atendimento e a taxa de readmissão é muito baixa e negativa. Isso indica que não há uma relação forte entre a efetividade dos cuidados e a taxa de readmissão nos hospitais analisados. Em outras palavras, melhorias na efetividade dos cuidados não parecem estar associadas a uma redução nas taxas de readmissão dos pacientes.

# COMMAND ----------

# MAGIC %md
# MAGIC Consulta SQL para calcular a correlação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   corr(
# MAGIC     CAST(
# MAGIC       `Comparação Nacional da Efetividade do Atendimento_Index` AS DOUBLE
# MAGIC     ),
# MAGIC     CAST(
# MAGIC       `Comparação Nacional de Readmissão_Index` AS DOUBLE
# MAGIC     )
# MAGIC   ) AS corr_Efetividade_Readmissao
# MAGIC FROM
# MAGIC   df_final;

# COMMAND ----------

# MAGIC %md
# MAGIC **Gráfico para visualizar a correlação**

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Selecionar as colunas relevantes e converter para Pandas DataFrame
pandas_df = df_final.select(
    "Comparação Nacional da Efetividade do Atendimento_Index",
    "Comparação Nacional de Readmissão_Index"
).toPandas()

# Converter as colunas para o tipo numérico
pandas_df = pandas_df.apply(pd.to_numeric, errors="coerce")

# Criar o gráfico de dispersão
plt.figure(figsize=(10, 6))
sns.scatterplot(
    x="Comparação Nacional da Efetividade do Atendimento_Index",
    y="Comparação Nacional de Readmissão_Index",
    data=pandas_df,
)
plt.title("Relação entre Efetividade do Atendimento e Taxa de Readmissão")
plt.xlabel("Efetividade do Atendimento")
plt.ylabel("Taxa de Readmissão")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Resposta :**
# MAGIC
# MAGIC Os pontos estão espalhados uniformemente ao longo dos valores de efetividade do atendimento (0 a 3) e taxa de readmissão (0 a 3).
# MAGIC
# MAGIC Não há um padrão claro ou uma tendência óbvia que sugira uma correlação forte entre as duas variáveis.
# MAGIC
# MAGIC A correlação calculada foi de -0.04864618495323882, indicando uma correlação muito fraca e negativa.
# MAGIC
# MAGIC Isso é consistente com a dispersão dos pontos, que não mostra uma relação linear entre a efetividade do atendimento e a taxa de readmissão.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Quais estados têm os melhores e piores desempenhos em termos de segurança do atendimento hospitalar?
# MAGIC
# MAGIC Objetivo: Avaliar a segurança do atendimento por estado para destacar áreas geográficas que necessitam de melhorias específicas na saúde pública.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Resposta 4: Melhores e Piores Estados em Termos de Segurança do Atendimento
# MAGIC
# MAGIC ###5 Melhores Estados:**
# MAGIC
# MAGIC **Distrito de Colúmbia (DC):***
# MAGIC
# MAGIC Média de Segurança do Atendimento: 2.50
# MAGIC
# MAGIC O Distrito de Colúmbia se destacou como o estado com a melhor média de segurança do atendimento. Isso sugere que os hospitais nesta região possuem altos padrões de segurança, proporcionando uma 
# MAGIC
# MAGIC **Nova Jersey (NJ):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 2.03
# MAGIC
# MAGIC Nova Jersey também se sobressaiu com uma alta média de segurança. Hospitais neste estado demonstram um forte compromisso com a segurança dos pacientes.
# MAGIC
# MAGIC **Connecticut (CT):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 2.00
# MAGIC Connecticut apresenta uma média de segurança elevada, indicando que os hospitais neste estado mantêm práticas seguras e efetivas para os pacientes.
# MAGIC
# MAGIC **Rhode Island (RI):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 1.91
# MAGIC Rhode Island mostra um bom desempenho na segurança do atendimento, refletindo a qualidade dos cuidados de saúde oferecidos.
# MAGIC
# MAGIC **Delaware (DE):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 1.71
# MAGIC Delaware completa a lista dos cinco melhores, com uma média alta de segurança do atendimento.
# MAGIC
# MAGIC ###5 Piores Estados:
# MAGIC
# MAGIC **Porto Rico (PR):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 0.12
# MAGIC
# MAGIC Porto Rico possui a menor média de segurança do atendimento, indicando que há uma necessidade significativa de melhorias na segurança hospitalar.
# MAGIC
# MAGIC **Samoa Americana (AS):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 0.00
# MAGIC
# MAGIC Samoa Americana, juntamente com os próximos três estados, apresenta uma média de segurança de atendimento nula, o que é extremamente preocupante e sugere falta de dados ou uma falha completa na segurança dos atendimentos.
# MAGIC
# MAGIC **Ilhas Marianas do Norte (MP):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 0.00
# MAGIC
# MAGIC Semelhante à Samoa Americana, as Ilhas Marianas do Norte também apresentam uma média de segurança nula.
# MAGIC
# MAGIC **Maryland (MD):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 0.00
# MAGIC
# MAGIC Maryland está na mesma situação dos dois estados anteriores, necessitando de uma revisão e possível intervenção para melhorar a segurança do atendimento.
# MAGIC
# MAGIC **Guam (GU):**
# MAGIC
# MAGIC Média de Segurança do Atendimento: 0.00
# MAGIC Guam completa a lista dos piores estados, com uma média de segurança nula.
# MAGIC
# MAGIC ###Conclusão
# MAGIC
# MAGIC Os resultados destacam uma disparidade significativa na segurança do atendimento hospitalar entre os estados dos EUA. Enquanto alguns estados como o Distrito de Colúmbia, Nova Jersey e Connecticut mostram um excelente desempenho, outros como Porto Rico, Samoa Americana e Guam necessitam de atenção urgente para melhorar a segurança e a qualidade dos serviços de saúde prestados. Isso aponta para a necessidade de políticas direcionadas e melhorias na gestão hospitalar nos estados com desempenho inferior.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Estado,
# MAGIC   AVG(
# MAGIC     `Comparação Nacional de Segurança do Atendimento_Index`
# MAGIC   ) AS Media_Seguranca_Atendimento
# MAGIC FROM
# MAGIC   df_final
# MAGIC GROUP BY
# MAGIC   Estado
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Gráficos em Python**

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Executar a consulta SQL para obter os dados e convertê-los para Pandas DataFrame
df_resultado = spark.sql("""
SELECT
  Estado,
  AVG(
    `Comparação Nacional de Segurança do Atendimento_Index`
  ) AS Media_Seguranca_Atendimento
FROM
  df_final
GROUP BY
  Estado
""").toPandas()

# Ordenar os dados pela média da segurança do atendimento
df_resultado = df_resultado.sort_values("Media_Seguranca_Atendimento", ascending=False)

# Criar o gráfico
plt.figure(figsize=(20, 12))  # Aumentar o tamanho da figura

# Criar a paleta de cores (degradê de azul para vermelho)
paleta = sns.color_palette("coolwarm", len(df_resultado))

# Criar o gráfico de barras com largura ajustada
bar_plot = sns.barplot(
    x="Estado",
    y="Media_Seguranca_Atendimento",
    data=df_resultado,
    palette=paleta,
    ci=None,
)

# Ajustar a largura das barras
for bar in bar_plot.patches:
    bar.set_width(bar.get_width() * 0.8)

# Adicionar rótulos nas barras
for p in bar_plot.patches:
    bar_plot.annotate(
        format(p.get_height(), ".2f"),
        (p.get_x() + p.get_width() / 2.0, p.get_height()),
        ha="center",
        va="center",
        xytext=(0, 10),
        textcoords="offset points",
    )

# Rotacionar rótulos do eixo x para melhor legibilidade
plt.xticks(rotation=45, ha="right")

# Títulos e rótulos
plt.title("Média da Segurança do Atendimento por Estado")
plt.xlabel("Estado")
plt.ylabel("Média da Segurança do Atendimento")

# Exibir o gráfico
plt.show()


# COMMAND ----------

# Ordenar os dados pela média da segurança do atendimento
df_resultado = df_resultado.sort_values("Media_Seguranca_Atendimento", ascending=False)

# Obter os 5 melhores estados
melhores_estados = df_resultado.head(5)

# Obter os 5 piores estados
piores_estados = df_resultado.tail(5)

# Exibir os resultados
print("5 Melhores Estados:")
print(melhores_estados[["Estado", "Media_Seguranca_Atendimento"]])

print("\n5 Piores Estados:")
print(piores_estados[["Estado", "Media_Seguranca_Atendimento"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Análise Geral
# MAGIC
# MAGIC **Com base nas análises realizadas, podemos deduzir que:**
# MAGIC
# MAGIC - Segurança e Eficiência São Cruciais: A segurança do atendimento e a pontualidade são fatores críticos que influenciam a satisfação dos pacientes. Investimentos em melhorar a segurança e a eficiência dos cuidados podem resultar em experiências mais positivas para os pacientes.
# MAGIC
# MAGIC - Desigualdade no Desempenho Hospitalar: Existe uma disparidade significativa no desempenho hospitalar entre diferentes tipos de hospitais e regiões. Enquanto alguns hospitais e estados apresentam excelentes resultados, outros enfrentam desafios substanciais que precisam ser abordados.
# MAGIC
# MAGIC - Fatores Múltiplos Afetam a Qualidade do Cuidado: A eficácia dos cuidados, embora importante, não é o único fator determinante para a taxa de readmissão. Outras variáveis, possivelmente relacionadas a condições socioeconômicas, acesso a cuidados de saúde contínuos, e práticas de gerenciamento hospitalar, também desempenham papéis críticos.
# MAGIC
# MAGIC - Necessidade de Políticas Direcionadas: As regiões e tipos de hospitais com desempenho inferior necessitam de políticas específicas e intervenções direcionadas para melhorar a segurança e qualidade dos serviços de saúde. Focar em estratégias baseadas em dados pode ajudar a identificar e resolver os problemas mais urgentes.
# MAGIC
# MAGIC Em resumo, melhorar a segurança do atendimento e a eficiência, juntamente com abordagens políticas e gerenciais direcionadas, pode elevar a qualidade geral dos cuidados de saúde nos Estados Unidos, resultando em uma maior satisfação dos pacientes e melhores resultados de saúde.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Autoavaliação
# MAGIC
# MAGIC **Atingimento dos Objetivos**
# MAGIC
# MAGIC **Objetivo 1:** Identificar os principais fatores que influenciam a satisfação do paciente
# MAGIC
# MAGIC Conseguimos atingir este objetivo ao encontrar correlações significativas entre a satisfação do paciente e fatores como segurança do atendimento e pontualidade do atendimento. Esses fatores mostraram-se influentes na experiência geral do paciente, conforme identificado em nossa análise de correlação.
# MAGIC
# MAGIC **Objetivo 2:** Analisar como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais
# MAGIC Alcançamos este objetivo identificando que hospitais privados têm, em média, taxas de mortalidade mais baixas em comparação com hospitais governamentais. Esta análise nos permitiu compreender as variações de desempenho entre diferentes tipos de instituições hospitalares.
# MAGIC
# MAGIC **Objetivo 3:** Determinar se existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão
# MAGIC Este objetivo foi cumprido ao verificar que a correlação entre a eficácia dos cuidados e a taxa de readmissão é muito fraca. Isso sugere que outros fatores além da eficácia dos cuidados influenciam a readmissão dos pacientes.
# MAGIC
# MAGIC **Objetivo 4:** Avaliar a segurança do atendimento por estado
# MAGIC Conseguimos identificar os estados com melhor e pior desempenho em termos de segurança do atendimento hospitalar. Esta análise destacou áreas geográficas específicas que necessitam de melhorias na saúde pública.
# MAGIC
# MAGIC **Dificuldades Encontradas**
# MAGIC
# MAGIC - Inconsistências nos Nomes das Colunas: Tivemos problemas iniciais ao referenciar colunas, o que exigiu vários ajustes para garantir a consistência dos dados.
# MAGIC - Tratamento de Dados: Decidir sobre o tratamento adequado para valores nulos e outliers foi desafiador. Tivemos que garantir que essas decisões não afetassem negativamente a qualidade dos dados.
# MAGIC - Visualização dos Dados: Criar visualizações claras e informativas apresentou desafios, especialmente ao tentar usar mapas para representar dados geográficos.
# MAGIC - Execução de Consultas SQL: Adaptar cálculos de correlação e outras operações analíticas para SQL foi difícil, especialmente em um ambiente que utiliza DataFrames.
# MAGIC - Interpretação dos Resultados: A interpretação dos resultados para gerar insights acionáveis foi uma etapa que exigiu tempo e reflexão cuidadosa.
# MAGIC - Principais Dificuldades e Tempo de Execução
# MAGIC - A maior dificuldade foi a execução de consultas SQL complexas e a adaptação de cálculos de correlação para esse formato. Esta etapa demorou mais do que o esperado devido à necessidade de ajustar continuamente as consultas para garantir a precisão dos resultados. Além disso, o tratamento de dados e a decisão sobre como lidar com valores nulos e outliers também exigiram um tempo considerável, pois precisávamos garantir a integridade e a qualidade dos dados para análises subsequentes.
# MAGIC
# MAGIC Com base nessas experiências, é evidente que o planejamento cuidadoso e a compreensão detalhada dos dados são cruciais para a execução eficiente de projetos de engenharia de dados. No futuro, a criação de scripts mais automatizados para tratamento de dados e a utilização de ferramentas de visualização mais avançadas podem ajudar a mitigar alguns desses desafios.
# MAGIC
# MAGIC
# MAGIC ###Trabalhos Futuros
# MAGIC
# MAGIC Para enriquecer e expandir a análise realizada, algumas propostas para trabalhos futuros incluem:
# MAGIC
# MAGIC **Aprofundamento na Análise de Fatores:**
# MAGIC
# MAGIC Realizar análises mais detalhadas para identificar outros fatores que influenciam a satisfação do paciente, além dos já identificados, como segurança e pontualidade do atendimento. Utilizar técnicas de machine learning para descobrir padrões ocultos nos dados.
# MAGIC Segmentação dos Dados:
# MAGIC
# MAGIC **Segmentar os dados:** 
# MAGIC
# MAGIC Segmentar os dados por diferentes características demográficas, como idade, sexo e condição socioeconômica, para entender como esses fatores impactam a satisfação do paciente e outros indicadores de desempenho hospitalar.

# COMMAND ----------

# MAGIC %md
# MAGIC carregando o notebook para o Redshift 

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
