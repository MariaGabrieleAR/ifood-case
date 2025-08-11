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
- ** SQL **

---

## 📁 Estrutura do Projeto

```bash
ifood-case/
├── src/                 # Código fonte da solução (ingestão, transformação, escrita)
├── analysis/            # Scripts/Notebooks com as respostas das perguntas analíticas
├── README.md            # Este documento
└── requirements.txt     # Dependências para rodar o projeto
