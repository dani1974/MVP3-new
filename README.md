### Análise da Qualidade dos Hospitais nos EUA
# Nome: Daniela Lameirão Pinto de Abreu Rosas

O objetivo deste trabalho é construir um pipeline de dados utilizando tecnologias na nuvem para analisar a qualidade dos hospitais nos EUA. O foco é identificar os principais fatores que influenciam a satisfação do paciente, avaliar a variação da taxa de mortalidade entre diferentes tipos de hospitais, explorar a correlação entre a eficácia dos cuidados e a taxa de readmissão, e identificar os estados com os melhores e piores desempenhos em termos de segurança do atendimento hospitalar..

# url do projeto publicado pelo databricks : 
[Veja o notebook completo no Databricks](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2295645624184554/1503296092627704/3793646805506685/latest.html)

# Problema a Ser Resolvido
Identificar e analisar os fatores que influenciam a qualidade do atendimento hospitalar nos Estados Unidos.

# Perguntas de Negócio

# 1. Quais são os principais fatores que influenciam a satisfação do paciente?
Objetivo: Identificar quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente.

# 2. Como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais (e.g., governamentais, privados)?
Objetivo: Avaliar a taxa de mortalidade para fornecer insights sobre a eficácia e a segurança dos cuidados prestados em diferentes tipos de hospitais.

# 3. Existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão?
Objetivo: Analisar se hospitais que prestam cuidados eficazes também apresentam menores taxas de readmissão.

# 4. Quais estados têm os melhores e piores desempenhos em termos de segurança do atendimento hospitalar?
Objetivo: Avaliar a segurança do atendimento por estado para destacar áreas geográficas que necessitam de melhorias específicas na saúde pública.

# Plataforma Utilizada
A plataforma utilizada para a construção do pipeline de dados foi o Databricks Community Edition, que oferece um ambiente gratuito com algumas limitações. Todos os processos, desde a coleta até a análise dos dados, foram realizados nesta plataforma.

# Detalhamento

# 1. Busca pelos Dados (E - Extração)
Foi escolhido o conjunto de dados "Hospital General Information" disponível no Kaggle. Este conjunto de dados contém informações detalhadas sobre mais de 4.000 hospitais nos EUA, incluindo taxas de mortalidade, segurança do atendimento, experiência do paciente, entre outros indicadores de qualidade.

# Fonte dos dados:
Utilizarei o arquivo "Hospital General Information.csv" O conjunto de dados foi baixado de [ https://data.medicare.gov/data/hospital-compare ] Base de Dados: Foi escolhida uma base de dados do Kaggle. Link para os Dados: https://www.kaggle.com/datasets/center-for-medicare-and-medicaid/hospital-ratings

# Campos do conjunto de dados:
ID do provedor Nome do Hospital Endereço Cidade Estado CEP Nome do Condado Número de telefone Tipo de hospital Propriedade Hospitalar Serviços de emergência Atende aos critérios para uso significativo de EHRs Classificação geral do hospital Nota de rodapé sobre a classificação geral do hospital Comparação nacional de mortalidade Nota de rodapé sobre comparação nacional de mortalidade Comparação nacional de segurança do atendimento Nota de rodapé sobre comparação nacional de segurança de cuidados Comparação nacional de readmissão Nota de rodapé sobre comparação nacional de readmissão Comparação nacional da experiência do paciente Nota de rodapé sobre comparação nacional da experiência do paciente Comparação nacional da eficácia dos cuidados Nota de rodapé sobre a eficácia dos cuidados de saúde Comparação nacional da pontualidade do atendimento Nota de rodapé sobre a oportunidade do atendimento na comparação nacional Uso eficiente de comparação nacional de imagens médicas Uso eficiente de comparação nacional de imagens médicas

# 2. Coleta dos Dados (E - Extração)
Os dados foram baixados do Kaggle e armazenados localmente. Posteriormente, foram carregados para o Databricks File System (DBFS) para facilitar o processamento e a análise na nuvem.

# Processo de Coleta

Criação e configuração de um bucket no AWS S3.

Upload do arquivo "Hospital General Information.csv" para o bucket S3.

Montagem do bucket S3 no Databricks.

Carregamento dos dados do CSV no Spark DataFrame.

# Fonte dos Dados: Kaggle

Local de Armazenamento:

Licença de Uso: Licença Kaggle

Plataforma: Usarei a Plataforma Databricks para a construção do pipeline de dados, utilizando a versão Databricks Community Edition.


