# Análise da Qualidade dos Hospitais nos EUA
### Nome: Daniela Lameirão Pinto de Abreu Rosas

O objetivo deste trabalho é construir um pipeline de dados utilizando tecnologias na nuvem para analisar a qualidade dos hospitais nos EUA. O foco é identificar os principais fatores que influenciam a satisfação do paciente, avaliar a variação da taxa de mortalidade entre diferentes tipos de hospitais, explorar a correlação entre a eficácia dos cuidados e a taxa de readmissão, e identificar os estados com os melhores e piores desempenhos em termos de segurança do atendimento hospitalar..

#### url do projeto publicado pelo databricks : [Veja o notebook completo no Databricks](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2295645624184554/1503296092627704/3793646805506685/latest.html)

### Problema a Ser Resolvido
Identificar e analisar os fatores que influenciam a qualidade do atendimento hospitalar nos Estados Unidos.

### Perguntas de Negócio

### 1. Quais são os principais fatores que influenciam a satisfação do paciente?
Objetivo: Identificar quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente.

### 2. Como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais (e.g., governamentais, privados)?
Objetivo: Avaliar a taxa de mortalidade para fornecer insights sobre a eficácia e a segurança dos cuidados prestados em diferentes tipos de hospitais.

### 3. Existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão?
Objetivo: Analisar se hospitais que prestam cuidados eficazes também apresentam menores taxas de readmissão.

### 4. Quais estados têm os melhores e piores desempenhos em termos de segurança do atendimento hospitalar?
Objetivo: Avaliar a segurança do atendimento por estado para destacar áreas geográficas que necessitam de melhorias específicas na saúde pública.

### Plataforma Utilizada
A plataforma utilizada para a construção do pipeline de dados foi o Databricks Community Edition, que oferece um ambiente gratuito com algumas limitações. Todos os processos, desde a coleta até a análise dos dados, foram realizados nesta plataforma.

# Detalhamento

## Busca pelos Dados
Foi escolhido o conjunto de dados "Hospital General Information" disponível no Kaggle. Este conjunto de dados contém informações detalhadas sobre mais de 4.000 hospitais nos EUA, incluindo taxas de mortalidade, segurança do atendimento, experiência do paciente, entre outros indicadores de qualidade.

### Fonte dos dados:
Foi escolhida uma base de dados do Kaggle. Link para os Dados: https://www.kaggle.com/datasets/center-for-medicare-and-medicaid/hospital-ratings

![image](https://github.com/dani1974/MVP3-new/assets/39570553/b87f6764-d1f5-4e5b-9505-9a58bcf30b35)

### Coleta dos Dados 

### Processo de Coleta:
•	Criação e configuração de um bucket no AWS S3.
•	Upload do arquivo "Hospital General Information.csv" para o bucket S3.
•	Montagem do bucket S3 no Databricks.
•	Carregamento dos dados do CSV no Spark DataFrame.

![image](https://github.com/dani1974/MVP3-new/assets/39570553/3afb6f1f-9dc9-42ef-b40d-6a04e89a915c)

## Transformação dos Dados

### Renomeação de Colunas
As colunas foram renomeadas para português para facilitar a análise.

### Seleção de Colunas Relevantes
Selecionamos apenas as colunas relevantes para a análise.

### Limpeza de Dados
Removemos valores nulos e substituímos valores categóricos por numéricos.

### Validação dos Dados
Verificação de valores nulos e estatísticas descritivas para garantir a qualidade dos dados.

### Screenshots

![Transformação no Databricks](path/to/screenshot_transformacao.png)












