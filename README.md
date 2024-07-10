## Ciência de Dados e Analytics - PUC-RIO
## MVP 3 - Sprint: Engenharia de Dados  // Aluna: Daniela Lameirão Pinto de Abreu Rosas

# Análise da Qualidade dos Hospitais nos EUA

O objetivo deste trabalho é construir um pipeline de dados utilizando tecnologias na nuvem para analisar a qualidade dos hospitais nos EUA. O foco é identificar os principais fatores que influenciam a satisfação do paciente, avaliar a variação da taxa de mortalidade entre diferentes tipos de hospitais, explorar a correlação entre a eficácia dos cuidados e a taxa de readmissão, e identificar os estados com os melhores e piores desempenhos em termos de segurança do atendimento hospitalar..

#### url do projeto publicado pelo databricks : https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2295645624184554/1950158164922756/3793646805506685/latest.html


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

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Detalhamento

![Fluxo dos dados drawio](https://github.com/dani1974/MVP3-new/assets/39570553/b00d1136-d493-4424-894c-6d63cf797bb3)

## Busca pelos Dados
Foi escolhido o conjunto de dados "Hospital General Information" disponível no Kaggle. Este conjunto de dados contém informações detalhadas sobre mais de 4.000 hospitais nos EUA, incluindo taxas de mortalidade, segurança do atendimento, experiência do paciente, entre outros indicadores de qualidade.

### Fonte dos dados:
Foi escolhida uma base de dados do Kaggle. Link para os Dados: https://www.kaggle.com/datasets/center-for-medicare-and-medicaid/hospital-ratings

![image](https://github.com/dani1974/MVP3-new/assets/39570553/b87f6764-d1f5-4e5b-9505-9a58bcf30b35)

### Coleta dos Dados 

### Processo de Coleta:
•	Criação e configuração de um bucket no AWS S3.
•	Upload do arquivo "Hospital General Information.csv" para o bucket S3.

![image](https://github.com/dani1974/MVP3-new/assets/39570553/3afb6f1f-9dc9-42ef-b40d-6a04e89a915c)

•	Montagem do bucket S3 no Databricks.
•	Carregamento dos dados do CSV no Spark DataFrame.

![image](https://github.com/dani1974/MVP3-new/assets/39570553/07594828-1339-4b72-be0e-057a9c4be549)

![image](https://github.com/dani1974/MVP3-new/assets/39570553/d0c59621-ffef-4e62-bd93-47d1a64ba0a1)


### Modelagem dos Dados (Modelagem)

### 4. Modelagem dos Dados (T - Transformação)
Foi construído um modelo de dados em Esquema Estrela, que é uma abordagem comum para Data Warehouses. As tabelas de dimensões e fatos foram criadas e populadas de acordo com os dados disponíveis.

#### Esquema Estrela
O diagrama do esquema estrela foi feito utilizando a ferramenta GenMyModel.

![image](https://github.com/dani1974/MVP3-new/assets/39570553/de8efae7-837c-4303-8b73-8f2ece10b977)

### Tabelas Dimensão

Dim_Provedor
Dim_Estado
Dim_Tipo_Hospital

### Tabelas de Fato

Fato_Satisfacao_Paciente
Fato_Mortalidade_Hospital
Fato_Efetividade_Readmissao
Fato_Seguranca_Estado

### Catálogo de Dados:
Descrição Detalhada dos Dados e Seus Domínios

### Tabelas Fato:

**Fato_Satisfacao_Paciente**

D_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
Estado_Index: Identificador do estado do hospital. (VARCHAR(2)) (FK para Dim_Estado)
Tipo_Hospital_Index: Identificador do tipo de hospital. (VARCHAR(10)) (FK para Dim_Tipo_Hospital)
Comparacao_Seguranca_Cuidado_Index: Índice categórico representando a comparação de segurança do cuidado. (VARCHAR(10))
Comparacao_Readmissao_Index: Índice categórico representando a comparação de readmissão. (VARCHAR(10))
Comparacao_Experiencia_Paciente_Index: Índice categórico representando a comparação da experiência do paciente. (VARCHAR(10))
Comparacao_Efetividade_Cuidado_Index: Índice categórico representando a comparação da efetividade do cuidado. (VARCHAR(10))
Comparacao_Pontualidade_Cuidado_Index: Índice categórico representando a comparação da pontualidade do cuidado. (VARCHAR(10))
Comparacao_Uso_Eficiente_Imagem_Index: Índice categórico representando a comparação do uso eficiente de imagens médicas. (VARCHAR(10))

**Fato_Mortalidade_Hospital**

ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
Propriedade_Hospitalar_Index: Identificador da propriedade hospitalar. (VARCHAR(10)) (FK para Dim_Tipo_Hospital)
Comparacao_Mortalidade_Index: Índice categórico representando a comparação de mortalidade. (VARCHAR(10))
Comparacao_Readmissao_Index: Índice categórico representando a comparação de readmissão. (VARCHAR(10))

**Fato_Efetividade_Readmissao**

ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
Comparacao_Efetividade_Cuidado_Index: Índice categórico representando a comparação da efetividade do cuidado. (VARCHAR(10))
Comparacao_Readmissao_Index: Índice categórico representando a comparação de readmissão. (VARCHAR(10))

**Fato_Seguranca_Estado**

ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (FK para Dim_Provedor)
Estado_Index: Identificador do estado do hospital. (VARCHAR(2)) (FK para Dim_Estado)
Comparacao_Seguranca_Cuidado_Index: Índice categórico representando a comparação de segurança do cuidado. (VARCHAR(10)).

### Tabelas Dimensão

**Dim_Provedor**

ID_Provedor_Index: Identificador único do hospital. (VARCHAR(10)) (Chave Primária)

**Dim_Estado**

Estado_Index: Identificador do estado do hospital. (VARCHAR(2)) (Chave Primária)

**Dim_Tipo_Hospital**

Tipo_Hospital_Index: Identificador do tipo de hospital. (VARCHAR(10))
Propriedade_Hospitalar_Index: Identificador da propriedade hospitalar. (VARCHAR(10)) (Chaves Primárias Conjuntas)

### Linhagem dos Dados

**Fonte dos Dados:** O conjunto de dados foi baixado do portal Medicare Hospital Compare, disponível no Kaggle.

### Conjunto de Dados Utilizado: "Hospital General Information.csv"

Técnica Utilizada: Os dados foram coletados do Kaggle e armazenados no Databricks File System (DBFS). Posteriormente, os dados foram carregados no Spark DataFrame e processados para análise.

### 5. Carga dos Dados (L - Carga)
Os dados transformados foram carregados para o Data Warehouse no Databricks. Utilizamos pipelines de ETL para garantir a carga eficiente dos dado

**Processos de Carga**

Criação das tabelas de dimensões e de fatos.
Inserção dos dados transformados nas tabelas criadas.

**Criação do banco de dados no Red shift:**

Criação do banco de dados ou configurar o Redshift para receber dados.

![image](https://github.com/dani1974/MVP3-new/assets/39570553/b1c202be-58bf-479c-9df4-d797c9bbafc6)


### 6. Qualidade dos Dados : Análise Exploratória de Dados (EDA)
Na Análise Exploratória de Dados (EDA), verificamos os seguintes aspectos:

**6.1. Valores Nulos**

Já foi verificado anteriormente que colunas com mais de 10% de valores nulos foram excluídas.

**6.2. Estatísticas Descritivas**

Será usado SQL para gerar algumas estatísticas descritivas:

**Interpretação dos Resultados**

**Valores Mínimos (Min)**

Todos os atributos categóricos têm um valor mínimo de 0, indicando que há hospitais que estão na categoria mais baixa para cada um dos índices.

**Valores Máximos (Max)**

Todos os atributos categóricos têm um valor máximo de 3, indicando que há hospitais que estão na categoria mais alta para cada um dos índices.

**Médias (Avg)**

Segurança do Atendimento: A média de 0.99 sugere que, em média, os hospitais estão ligeiramente acima da categoria mais baixa.
Readmissão: A média de 1.08 sugere que, em média, os hospitais estão próximos da categoria mais baixa, mas com uma pequena variação.
Experiência do Paciente: A média de 1.40 indica que a experiência do paciente tende a ser um pouco melhor em comparação com a segurança e readmissão, mas ainda está longe da categoria máxima.
Efetividade do Atendimento: A média de 0.43 é a mais baixa entre os índices, sugerindo que a efetividade do atendimento geralmente é classificada nas categorias mais baixas.
Pontualidade do Atendimento: A média de 1.28 indica que a pontualidade do atendimento está acima da média mais baixa, mas ainda há espaço para melhorias.
Uso Eficiente de Imagens: A média de 0.80 sugere que o uso eficiente de imagens médicas está mais próximo das categorias mais baixas.

### 6.3 Verificar as Categorias em Cada Coluna e Contar as Frequências

![image](https://github.com/dani1974/MVP3-new/assets/39570553/98e66eae-6ead-41a2-b0dc-b2fb7baac3c4)

**Significado dos Valores Numéricos em Cada Coluna**

As colunas possuem os mesmas categorias de classificação: Segurança no Atendimento; Readmissão: ;Experiência do Paciente; Efetividade do Atendimento; Pontualidade do Atendimento; Uso Eficiente de Imagens

0.0: Abaixo da média nacional
1.0: Na média nacional
2.0: Acima da média nacional
3.0: Muito acima da média nacional

### Explicação

**Segurança no Atendimento:**

A maioria dos hospitais está "Abaixo da média nacional" (2168).
Poucos hospitais estão "Muito acima da média nacional" (664).

**Readmissão:**

A maioria dos hospitais está "Abaixo da média nacional" (2119).
Poucos hospitais estão "Muito acima da média nacional" (810).

**Experiência do Paciente:**

A distribuição é mais equilibrada, mas ainda assim a maioria está "Na média nacional" (1214).

**Efetividade do Atendimento:**

A grande maioria dos hospitais está "Abaixo da média nacional" (3238).

**Pontualidade do Atendimento:**

A distribuição é relativamente equilibrada, mas a maioria está "Abaixo da média nacional" (1554).

**Uso Eficiente de Imagens:**

A maioria dos hospitais está "Abaixo da média nacional" (2051).

Essa análise nos mostra que a maioria dos hospitais está na média ou abaixo da média nacional em todos os aspectos, indicando áreas de melhoria no atendimento hospitalar.

**Conclusão da Análise de Qualidade dos Dados**

Os dados apresentam variações significativas entre os hospitais em relação aos índices avaliados. A distribuição dos valores indica que há mais hospitais com desempenho abaixo da média nacional em diversas categorias.

### Histogramas com Curva KDE

Os histogramas com a curva KDE (Kernel Density Estimation) mostram a distribuição das categorias em cada coluna categórica do DataFrame.

**Comparação Nacional de Segurança do Atendimento_Index_Index**

Distribuição: A maior parte dos hospitais está na categoria 0, seguida pelas categorias 1 e 2. A categoria 3 tem a menor frequência.
Interpretação: Isso indica que a maioria dos hospitais está classificada abaixo ou na média nacional de segurança do atendimento, com menos hospitais sendo classificados como acima da média.

**Comparação Nacional de Readmissão_Index_Index**

Distribuição: A maioria dos hospitais está na categoria 0, com frequências decrescentes nas categorias 1, 2 e 3.
Interpretação: Semelhante à segurança do atendimento, a maioria dos hospitais está classificada abaixo ou na média nacional em termos de readmissão.

**Comparação Nacional da Experiência do Paciente_Index_Index**

Distribuição: A distribuição é mais equilibrada entre as categorias, com uma ligeira predominância da categoria 1.
Interpretação: A experiência do paciente varia mais entre os hospitais, mas ainda assim, muitos estão na média ou abaixo.

**Comparação Nacional da Efetividade do Atendimento_Index_Index**

Distribuição: A maioria dos hospitais está na categoria 0, com poucas ocorrências nas categorias 2 e 3.
Interpretação: A efetividade do atendimento é predominantemente classificada como na média ou abaixo da média nacional.

**Comparação Nacional da Pontualidade do Atendimento_Index_Index**

Distribuição: A distribuição é relativamente equilibrada entre as categorias 0, 1 e 2, com a categoria 3 tendo menos frequências.
Interpretação: A pontualidade do atendimento é variável, com muitos hospitais ainda na média ou abaixo.

**Comparação Nacional do Uso Eficiente de Imagens Médicas_Index_Index**

Distribuição: Há uma distribuição relativamente uniforme entre as categorias, com uma leve predominância da categoria 1.
Interpretação: O uso eficiente de imagens médicas varia consideravelmente, com muitos hospitais classificados na média ou abaixo.

### Box Plots

Os box plots fornecem uma visão clara da distribuição dos dados, mostrando a mediana, quartis e possíveis outliers.

**Comparação Nacional de Segurança do Atendimento_Index_Index**

Box Plot: A mediana está na categoria 0, com um intervalo interquartil que vai até a categoria 2. Existem alguns outliers nas categorias superiores.
Interpretação: A maioria dos hospitais tem uma segurança do atendimento na média ou abaixo, mas há alguns que se destacam positivamente.
Ação: Verificar a causa dos outliers superiores e determinar se representam práticas excepcionais ou erros de dados.

**Comparação Nacional de Readmissão_Index_Index**

Box Plot: Semelhante ao de segurança do atendimento, a mediana está na categoria 0 com outliers nas categorias superiores.
Interpretação: A readmissão é predominantemente na média ou abaixo, com alguns hospitais com melhor performance.
Ação: Investigar os outliers para entender as práticas que levam a uma menor readmissão e considerar a remoção de outliers negativos se forem erros.

**Comparação Nacional da Experiência do Paciente_Index_Index**

Box Plot: A mediana está um pouco acima da categoria 1, com uma maior variabilidade nos dados.
Interpretação: A experiência do paciente é mais distribuída, com muitos hospitais na média ou acima.
Ação: Focar na análise dos hospitais que estão significativamente abaixo da mediana para melhorar a experiência do paciente.

**Comparação Nacional da Efetividade do Atendimento_Index_Index**

Box Plot: A mediana está na categoria 0, com poucos outliers.
Interpretação: A efetividade do atendimento é predominantemente classificada como na média ou abaixo.
Ação: Examinar os hospitais com outliers superiores para identificar boas práticas e verificar possíveis erros nos outliers inferiores.

**Comparação Nacional da Pontualidade do Atendimento_Index_Index**

Box Plot: A mediana está na categoria 1, com uma distribuição equilibrada.
Interpretação: A pontualidade do atendimento é variável, com muitos hospitais na média ou acima.
Ação: Analisar os outliers para entender os fatores que contribuem para uma melhor pontualidade e replicar essas práticas.

**Comparação Nacional do Uso Eficiente de Imagens Médicas_Index_Index**

Box Plot: A mediana está na categoria 1, com uma distribuição relativamente uniforme.
Interpretação: O uso eficiente de imagens médicas varia consideravelmente entre os hospitais.
Ação: Identificar hospitais com desempenho inferior para recomendar melhorias no uso eficiente de imagens médicas.

### Verificação da Distribuição Normal
A verificação da distribuição normal ajuda a entender se os dados seguem uma distribuição normal ou se apresentam uma distribuição diferente.

**Comparação Nacional de Segurança do Atendimento_Index_Index**
Distribuição: A curva KDE mostra picos em cada categoria, indicando que os dados são categóricos e não seguem uma distribuição normal contínua.

**Comparação Nacional de Readmissão_Index_Index**
Distribuição: Semelhante à segurança do atendimento, os dados são categóricos e apresentam picos em cada categoria.

**Comparação Nacional da Experiência do Paciente_Index_Index**
Distribuição: A distribuição é mais equilibrada entre as categorias, com picos em cada categoria.

**Comparação Nacional da Efetividade do Atendimento_Index_Index**
Distribuição: Predominância na categoria 0, com picos menores nas outras categorias.

**Comparação Nacional da Pontualidade do Atendimento_Index_Index**
Distribuição: Distribuição relativamente equilibrada entre as categorias, com picos em cada categoria.

**Comparação Nacional do Uso Eficiente de Imagens Médicas_Index_Index**
Distribuição: Picos em cada categoria, indicando uma distribuição categórica.

### Conclusão da Análise da Qualidade dos Dados

**Verificação de Valores Nulos**
Após a verificação e tratamento dos valores nulos, o conjunto de dados foi aprimorado para garantir que apenas as colunas com dados completos ou com uma quantidade mínima de valores nulos fossem mantidas. Isso assegura que a análise subsequente seja confiável e representativa.

**Verificação de Valores Fora dos Limites Esperados**
Foi realizada uma análise dos valores únicos nas colunas categóricas. As categorias foram verificadas e encontraram-se dentro dos limites esperados (0 a 3), confirmando que os dados estão bem categorizados.

### Análise de Outliers
Os gráficos box plot foram utilizados para identificar a presença de outliers nas diversas colunas categóricas. Observou-se que:

**Comparação Nacional de Segurança do Atendimento:** Apresenta outliers significativos, indicando que alguns hospitais têm desempenho notavelmente diferente da maioria.

**Comparação Nacional de Readmissão**: Outliers presentes, sugerindo variabilidade na taxa de readmissão.

**Comparação Nacional da Experiência do Paciente**: Distribuição variada com outliers, indicando diferentes níveis de satisfação dos pacientes.

**Comparação Nacional da Efetividade do Atendimento**: Alguns outliers identificados, refletindo variabilidade na efetividade do atendimento.

**Comparação Nacional da Pontualidade do Atendimento**: Variabilidade moderada com outliers, mostrando diferenças na pontualidade.

**Comparação Nacional do Uso Eficiente de Imagens Médicas**: Presença de outliers, sugerindo diferentes práticas na utilização de imagens médicas.

**Análise de Distribuição**

A análise da distribuição dos dados através de histogramas revelou que a maioria das colunas categóricas segue uma distribuição não normal, com a maioria dos valores concentrados nas categorias inferiores (0 e 1). Isso indica que muitos hospitais tendem a ter desempenhos na média ou abaixo dela.

### Conclusão Geral

A qualidade dos dados é satisfatória para a análise pretendida. Apesar da presença de outliers e algumas distribuições não normais, os dados são robustos o suficiente para fornecer insights significativos sobre os fatores que influenciam a satisfação do paciente. As etapas de limpeza e transformação garantiram a integridade e a representatividade dos dados, permitindo uma análise confiável e detalhada.

### 6.b. Solução do Problema
Consultas SQL para Responder às Perguntas de Negócio

### Upload do Notebook Exportado para o S3:

![image](https://github.com/dani1974/MVP3-new/assets/39570553/2b4ec1be-d5fc-4393-b7d6-1af8cbb7fc6e)


-------------------------------------------------------------------------------- **RESPOSTAS AS PERGUNTAS DE NEGÓCIO**--------------------------------------------------------------------------------------------------------------------------------------------------

### b.1) Quais são os principais fatores que influenciam a satisfação do paciente?

Objetivo: Identificar quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente.

**Análise dos Resultados**
Os valores de correlação entre a satisfação do paciente (Comparação Nacional da Experiência do Paciente_Index_Index) e os outros fatores são os seguintes:

Segurança do Atendimento (corr_Seguranca_Atendimento): 0.6010520704707379

Readmissão (corr_Readmissao): 0.2284856558523016

Efetividade do Atendimento (corr_Efetividade_Atendimento): -0.2308621917719162

Pontualidade do Atendimento (corr_Pontualidade_Atendimento): 0.2456645259289548

Uso Eficiente de Imagens (corr_Uso_Eficiente_Imagens): -0.08022861962049851

**Interpretação dos Resultados**

**Segurança do Atendimento (0.60)**: Interpretação: Há uma correlação positiva moderada a forte entre a segurança do atendimento e a satisfação do paciente. Isso significa que, conforme a segurança do atendimento melhora, a satisfação do paciente tende a aumentar. Conclusão: A segurança do atendimento é um fator significativo que influencia positivamente a satisfação do paciente.

**Readmissão (0.23)**: Interpretação: Existe uma correlação positiva fraca entre a readmissão e a satisfação do paciente. Isso sugere que, embora haja alguma relação, não é tão forte quanto outros fatores. Conclusão: A readmissão tem um impacto menor na satisfação do paciente, mas ainda é relevante.

**Efetividade do Atendimento (-0.23)**: Interpretação: Há uma correlação negativa fraca entre a efetividade do atendimento e a satisfação do paciente. Isso indica que, conforme a efetividade do atendimento aumenta, a satisfação do paciente tende a diminuir, embora a relação não seja muito forte. Conclusão: Este resultado é contra-intuitivo e pode requerer uma análise mais detalhada para entender melhor a relação entre a efetividade do atendimento e a satisfação do paciente.

**Pontualidade do Atendimento (0.25)**: Interpretação: Existe uma correlação positiva fraca entre a pontualidade do atendimento e a satisfação do paciente. Isso sugere que, conforme a pontualidade melhora, a satisfação do paciente tende a aumentar. Conclusão: A pontualidade do atendimento é um fator relevante que influencia a satisfação do paciente, embora não de maneira muito forte.

**Uso Eficiente de Imagens (-0.08)**: Interpretação: Há uma correlação negativa muito fraca entre o uso eficiente de imagens e a satisfação do paciente. Isso indica que a relação é praticamente inexistente. Conclusão: O uso eficiente de imagens médicas tem um impacto muito pequeno ou nulo na satisfação do paciente.

### Resposta 1

Os principais fatores que mais influenciam a satisfação do paciente foram:

Segurança do Atendimento: É o fator mais influente na satisfação do paciente, com uma correlação positiva moderada a forte.

Readmissão e Pontualidade do Atendimento: Ambos têm uma correlação positiva fraca com a satisfação do paciente, sugerindo que são relevantes, mas não os principais fatores.

Efetividade do Atendimento: A correlação negativa fraca é um achado interessante que merece uma investigação mais aprofundada para entender melhor a relação.

Uso Eficiente de Imagens: Tem uma correlação negativa muito fraca, indicando que não é um fator significativo na satisfação do paciente.

Estes resultados fornecem insights valiosos sobre quais aspectos do atendimento hospitalar têm maior impacto na experiência do paciente e podem guiar esforços de melhoria na área de saúde.

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### Pergunta 2. Como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais (e.g., governamentais, privados)?
Objetivo: Avaliar a taxa de mortalidade para fornecer insights sobre a eficácia e a segurança dos cuidados prestados em diferentes tipos de hospitais.

### Resposta 2
Gráfico de Barras: Média da Taxa de Mortalidade por Tipo de Hospital

![image](https://github.com/dani1974/MVP3-new/assets/39570553/12f280d5-6dcc-41f4-828f-2435b7522471)


**Physician**: Média da Taxa de Mortalidade: 0.91

Comentário: Hospitais com classificação de "Physician" apresentam a maior taxa de mortalidade média entre todos os tipos de hospitais analisados. Isso pode indicar um potencial problema na qualidade do atendimento ou outras questões estruturais que impactam negativamente a mortalidade.

**Voluntary non-profit - Other**: Média da Taxa de Mortalidade: 0.72

Comentário: Hospitais voluntários sem fins lucrativos, classificados como "Other", também apresentam uma taxa de mortalidade relativamente alta. Isso pode sugerir que, apesar de serem instituições sem fins lucrativos, há desafios em manter a qualidade do atendimento.

**Proprietary**: Média da Taxa de Mortalidade: 0.71

Comentário: Hospitais proprietários têm uma taxa de mortalidade próxima à dos hospitais voluntários. Este grupo inclui hospitais com fins lucrativos, onde a qualidade do atendimento pode ser impactada pela busca por lucros.

**Government** - State: Média da Taxa de Mortalidade: 0.69

Comentário: Hospitais estaduais governamentais apresentam uma taxa de mortalidade um pouco menor que os grupos anteriores. Isso pode refletir uma maior regulamentação e padrões de qualidade implementados a nível estadual.

**Government**- Local: Média da Taxa de Mortalidade: 0.67

Comentário: Hospitais locais do governo apresentam uma taxa de mortalidade similar à dos hospitais estaduais, sugerindo que a administração local pode ter uma influência positiva na qualidade do atendimento.

**Government** - Federal: Média da Taxa de Mortalidade: 0.65

Comentário: Hospitais federais mostram uma taxa de mortalidade um pouco menor, o que pode ser resultado de políticas e regulamentos federais mais rigorosos.

**Private**: Média da Taxa de Mortalidade: 0.59

Comentário: Hospitais privados apresentam uma taxa de mortalidade menor em comparação com muitos dos tipos anteriores, possivelmente devido a melhores recursos e infraestrutura.

**Voluntary non-profit** - Private: Média da Taxa de Mortalidade: 0.57

Comentário: Hospitais voluntários privados sem fins lucrativos têm a menor taxa de mortalidade média, sugerindo que este modelo de hospital pode proporcionar um atendimento de qualidade superior devido ao foco na saúde dos pacientes em vez de lucros.

### Conclusão
Os resultados indicam que a mortalidade hospitalar varia significativamente entre diferentes tipos de hospitais. Hospitais "Physician" e "Voluntary non-profit - Other" têm as maiores taxas de mortalidade, enquanto "Private" e "Voluntary non-profit - Private" têm as menores. Isso sugere que a estrutura organizacional e o modelo de financiamento de um hospital podem ter um impacto significativo na qualidade do atendimento e, consequentemente, na taxa de mortalidade dos pacientes.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### 3. Existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão?
Objetivo: Analisar se hospitais que prestam cuidados eficazes também apresentam menores taxas de readmissão.

### Resposta 3:
Correlação entre Efetividade do Atendimento e Taxa de Readmissão: -0.04864618495323882

A correlação entre a efetividade do atendimento e a taxa de readmissão é muito baixa e negativa. Isso indica que não há uma relação forte entre a efetividade dos cuidados e a taxa de readmissão nos hospitais analisados. Em outras palavras, melhorias na efetividade dos cuidados não parecem estar associadas a uma redução nas taxas de readmissão dos pacientes.


![image](https://github.com/dani1974/MVP3-new/assets/39570553/1f0d0f9a-dc0e-4bf8-ae79-0516d7ac0dff)


### Resposta 3:

Os pontos estão espalhados uniformemente ao longo dos valores de efetividade do atendimento (0 a 3) e taxa de readmissão (0 a 3).

Não há um padrão claro ou uma tendência óbvia que sugira uma correlação forte entre as duas variáveis.

A correlação calculada foi de -0.04864618495323882, indicando uma correlação muito fraca e negativa.

Isso é consistente com a dispersão dos pontos, que não mostra uma relação linear entre a efetividade do atendimento e a taxa de readmissão.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### 4. Quais estados têm os melhores e piores desempenhos em termos de segurança do atendimento hospitalar?
Objetivo: Avaliar a segurança do atendimento por estado para destacar áreas geográficas que necessitam de melhorias específicas na saúde pública.

![image](https://github.com/dani1974/MVP3-new/assets/39570553/0f735d17-ee43-43c6-ae63-df5f8d716da8)

### Resposta 4: Melhores e Piores Estados em Termos de Segurança do Atendimento

**5 Melhores Estados:**

Distrito de Colúmbia (DC): Média de Segurança do Atendimento: 2.50, 
O Distrito de Colúmbia se destacou como o estado com a melhor média de segurança do atendimento. Isso sugere que os hospitais nesta região possuem altos padrões de segurança, proporcionando uma

Nova Jersey (NJ):Média de Segurança do Atendimento: 2.03
Nova Jersey também se sobressaiu com uma alta média de segurança. Hospitais neste estado demonstram um forte compromisso com a segurança dos pacientes.

Connecticut (CT):Média de Segurança do Atendimento: 2.00 Connecticut apresenta uma média de segurança elevada, 
indicando que os hospitais neste estado mantêm práticas seguras e efetivas para os pacientes.

Rhode Island (RI): Média de Segurança do Atendimento: 1.91 
Rhode Island mostra um bom desempenho na segurança do atendimento, refletindo a qualidade dos cuidados de saúde oferecidos.

Delaware (DE): Média de Segurança do Atendimento: 1.71 
Delaware completa a lista dos cinco melhores, com uma média alta de segurança do atendimento.

**5 Piores Estados:**

Porto Rico (PR): Média de Segurança do Atendimento: 0.12
Porto Rico possui a menor média de segurança do atendimento, indicando que há uma necessidade significativa de melhorias na segurança hospitalar.

Samoa Americana (AS): Média de Segurança do Atendimento: 0.00
Samoa Americana, juntamente com os próximos três estados, apresenta uma média de segurança de atendimento nula, o que é extremamente preocupante e sugere falta de dados ou uma falha completa na segurança dos atendimentos.

Ilhas Marianas do Norte (MP): Média de Segurança do Atendimento: 0.00
Semelhante à Samoa Americana, as Ilhas Marianas do Norte também apresentam uma média de segurança nula.

Maryland (MD): Média de Segurança do Atendimento: 0.00
Maryland está na mesma situação dos dois estados anteriores, necessitando de uma revisão e possível intervenção para melhorar a segurança do atendimento.

Guam (GU): Média de Segurança do Atendimento: 0.00 Guam completa a lista dos piores estados, com uma média de segurança nula.

### Conclusão
Os resultados destacam uma disparidade significativa na segurança do atendimento hospitalar entre os estados dos EUA. Enquanto alguns estados como o Distrito de Colúmbia, Nova Jersey e Connecticut mostram um excelente desempenho, outros como Porto Rico, Samoa Americana e Guam necessitam de atenção urgente para melhorar a segurança e a qualidade dos serviços de saúde prestados. Isso aponta para a necessidade de políticas direcionadas e melhorias na gestão hospitalar nos estados com desempenho inferior.

### Análise Geral
Com base nas análises realizadas, podemos deduzir que:

Segurança e Eficiência São Cruciais: A segurança do atendimento e a pontualidade são fatores críticos que influenciam a satisfação dos pacientes. Investimentos em melhorar a segurança e a eficiência dos cuidados podem resultar em experiências mais positivas para os pacientes.

Desigualdade no Desempenho Hospitalar: Existe uma disparidade significativa no desempenho hospitalar entre diferentes tipos de hospitais e regiões. Enquanto alguns hospitais e estados apresentam excelentes resultados, outros enfrentam desafios substanciais que precisam ser abordados.

Fatores Múltiplos Afetam a Qualidade do Cuidado: A eficácia dos cuidados, embora importante, não é o único fator determinante para a taxa de readmissão. Outras variáveis, possivelmente relacionadas a condições socioeconômicas, acesso a cuidados de saúde contínuos, e práticas de gerenciamento hospitalar, também desempenham papéis críticos.

Necessidade de Políticas Direcionadas: As regiões e tipos de hospitais com desempenho inferior necessitam de políticas específicas e intervenções direcionadas para melhorar a segurança e qualidade dos serviços de saúde. Focar em estratégias baseadas em dados pode ajudar a identificar e resolver os problemas mais urgentes.

Em resumo, melhorar a segurança do atendimento e a eficiência, juntamente com abordagens políticas e gerenciais direcionadas, pode elevar a qualidade geral dos cuidados de saúde nos Estados Unidos, resultando em uma maior satisfação dos pacientes e melhores resultados de saúde.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Autoavaliação

**Atingimento dos Objetivos**

**Objetivo 1**: Identificar os principais fatores que influenciam a satisfação do paciente

Conseguimos atingir este objetivo ao encontrar correlações significativas entre a satisfação do paciente e fatores como segurança do atendimento e pontualidade do atendimento. Esses fatores mostraram-se influentes na experiência geral do paciente, conforme identificado em nossa análise de correlação.

**Objetivo 2**: Analisar como a taxa de mortalidade hospitalar varia entre diferentes tipos de hospitais Alcançamos este objetivo identificando que hospitais privados têm, em média, taxas de mortalidade mais baixas em comparação com hospitais governamentais. Esta análise nos permitiu compreender as variações de desempenho entre diferentes tipos de instituições hospitalares.

**Objetivo 3**: Determinar se existe uma correlação entre a eficácia dos cuidados e a taxa de readmissão Este objetivo foi cumprido ao verificar que a correlação entre a eficácia dos cuidados e a taxa de readmissão é muito fraca. Isso sugere que outros fatores além da eficácia dos cuidados influenciam a readmissão dos pacientes.

**Objetivo 4**: Avaliar a segurança do atendimento por estado Conseguimos identificar os estados com melhor e pior desempenho em termos de segurança do atendimento hospitalar. Esta análise destacou áreas geográficas específicas que necessitam de melhorias na saúde pública.

**Dificuldades Encontradas**

Inconsistências nos Nomes das Colunas: Tivemos problemas iniciais ao referenciar colunas, o que exigiu vários ajustes para garantir a consistência dos dados.
Tratamento de Dados: Decidir sobre o tratamento adequado para valores nulos e outliers foi desafiador. Tivemos que garantir que essas decisões não afetassem negativamente a qualidade dos dados.
Visualização dos Dados: Criar visualizações claras e informativas apresentou desafios, especialmente ao tentar usar mapas para representar dados geográficos.
Execução de Consultas SQL: Adaptar cálculos de correlação e outras operações analíticas para SQL foi difícil, especialmente em um ambiente que utiliza DataFrames.
Interpretação dos Resultados: A interpretação dos resultados para gerar insights acionáveis foi uma etapa que exigiu tempo e reflexão cuidadosa.

**Principais Dificuldades e Tempo de Execução**

A maior dificuldade foi a execução de consultas SQL complexas e a adaptação de cálculos de correlação para esse formato e a etapa de transformação. Esta etapa demorou mais do que o esperado devido à necessidade de ajustar continuamente as consultas para garantir a precisão dos resultados. Além disso, o tratamento de dados e a decisão sobre como lidar com valores nulos e outliers também exigiram um tempo considerável, pois precisávamos garantir a integridade e a qualidade dos dados para análises subsequentes.
Com base nessas experiências, é evidente que o planejamento cuidadoso e a compreensão detalhada dos dados são cruciais para a execução eficiente de projetos de engenharia de dados. 

**Trabalhos Futuros**
Para enriquecer e expandir a análise realizada, algumas propostas para trabalhos futuros incluem:

Aprofundamento na Análise de Fatores: Realizar análises mais detalhadas para identificar outros fatores que influenciam a satisfação do paciente, além dos já identificados, como segurança e pontualidade do atendimento. Utilizar técnicas de machine learning para descobrir padrões ocultos nos dados. Segmentação dos Dados:

Segmentar os dados: Segmentar os dados por diferentes características demográficas, como idade, sexo e condição socioeconômica, para entender como esses fatores impactam a satisfação do paciente e outros indicadores de desempenho hospitalar.****





















