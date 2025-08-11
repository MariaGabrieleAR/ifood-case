# 🚖 Case Técnico - Engenharia de Dados | iFood

Este projeto tem como objetivo demonstrar habilidades de engenharia de dados por meio da ingestão, transformação e análise de dados públicos de corridas de táxi de Nova York, simulando um cenário real de Data Lake em nuvem.

---

## 🧩 Descrição do Desafio

Responsável por construir a primeira versão do Data Lake do iFood, com foco na análise de dados de mobilidade urbana. O desafio consiste em:

- Ingerir dados brutos de corridas de **Yellow Taxis** (de janeiro a maio de 2023).
- Limpar, transformar e disponibilizar esses dados em formato Delta.
- Gerar insights analíticos com base nas informações processadas.

---

## 🛠️ Tecnologias Utilizadas

- **Apache Spark** (PySpark)
- **Delta Lake**
- **Databricks Community Edition**
- **AWS S3 (Data Lake)**
- **Python**
- **SQL**

---

## 📂 Descrição das Pastas

src/raw → Scripts responsáveis por carregar os dados originais, aplicar limpeza e tratamento inicial (remoção de nulos, padronização de datas, ajustes de tipos de dados etc.) antes de armazená-los no formato Delta.

analysis → Scripts e notebooks que utilizam os dados já tratados para gerar respostas às perguntas do case e análises adicionais que possam trazer insights relevantes.

🚀 Como Executar o Projeto

1- Clone este repositório:
bash
git clone https://github.com/seu-usuario/ifood-case.git
cd ifood-case

2- Instale as dependências:
bash
pip install -r requirements.txt

---
3- Execução no Databricks:
Faça upload dos scripts/notebooks da pasta src/raw para o Databricks.
Configure o acesso ao Data Lake (S3 no caso deste desafio).
Execute os scripts para processar os dados.
Use os notebooks da pasta analysis para responder às perguntas e explorar insights.

---
## 📁 Estrutura do Projeto

```bash
ifood-case/
├── src/                 # Código fonte da solução (ingestão, transformação, escrita)
├── analysis/            # Scripts/Notebooks com as respostas das perguntas analíticas
├── README.md            # Este documento
└── requirements.txt     # Dependências para rodar o projeto local
