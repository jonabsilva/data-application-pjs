# data-application-pjs

This is a Data application created and first developed by myself (@Jowzera).

The goal is to get data from eSaj ingest, process and display data almost all the data applications.

# Stack

- Python
- Spark
- Google Storage - GCS
- Terraform
- Git

# RESPOSITORY TREE

############################## DRAFT RESPOSITORY TREE ##############################

## Draft

-- scripts/ <br>
-- ingestion <br>
-- api-gdrive <br>
-- scraping <br>
-- n-sources <br>
-- processing <br>
-- read <br>
-- struct <br>
-- persist <br>
-- consuming <br>
-- notifier <br>

############################## This part is separated ##############################

-- Telegram BOT <br>
-- bot-request # it goes to the VPS (Hostinger) <br>

# Repository

################################# RESPOSITORY TREE #################################
meu_projeto/ <br>
├── terraform/ <br>
│ ├── main.tf # Configurações principais do Terraform <br>
│ ├── variables.tf # Declaração de variáveis do Terraform <br>
│ └── outputs.tf # Saída das configurações do Terraform <br>
├── scripts/ <br>
│ ├── ingestao.py # Script Python para ingestão de dados do Google Drive e API <br>
│ ├── processamento.py # Script Python para processamento de dados com Apache Spark <br>
│ ├── notificacao.py # Script Python para notificação por e-mail e Telegram <br>
│ └── requirements.txt # Lista de dependências do Python <br>
└── spark/ <br>
│ ├── jobs/ # Diretório para os jobs do Spark <br>
│ │ └── processamento_spark.py # Job do Spark para processamento de dados <br>
│ └── submit.sh # Script para submissão de jobs do Spark <br>
└── bot_telegram/ <br>
├── bot.py # Script Python para o bot do Telegram <br>
└── requirements.txt # Lista de dependências do Python <br>

# Architecture

![Screenshot 2024-04-16 at 18 40 05](https://github.com/Jowzera/data-application-pjs/assets/51763929/c99b5ac3-9aec-4725-a790-b22c196780f3)
