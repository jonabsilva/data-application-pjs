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

-- scripts/
 -- ingestion
   -- api-gdrive
   -- scraping
   -- n-sources ****
 -- processing
   -- read
   -- struct
   -- persist
 -- consuming
   -- notifier

############################## This part is separated ##############################
-- Telegram BOT
  -- bot-request # it goes to the VPS (Hostinger)

# Repository

################################# RESPOSITORY TREE #################################
meu_projeto/
├── terraform/
│   ├── main.tf            # Configurações principais do Terraform
│   ├── variables.tf       # Declaração de variáveis do Terraform
│   └── outputs.tf         # Saída das configurações do Terraform
├── scripts/
│   ├── ingestao.py        # Script Python para ingestão de dados do Google Drive e API
│   ├── processamento.py   # Script Python para processamento de dados com Apache Spark
│   ├── notificacao.py     # Script Python para notificação por e-mail e Telegram
│   └── requirements.txt   # Lista de dependências do Python
└── spark/
│   ├── jobs/              # Diretório para os jobs do Spark
│   │   └── processamento_spark.py  # Job do Spark para processamento de dados
│   └── submit.sh          # Script para submissão de jobs do Spark
└── bot_telegram/
    ├── bot.py             # Script Python para o bot do Telegram
    └── requirements.txt   # Lista de dependências do Python

# Architecture

![Screenshot 2024-04-16 at 18 40 05](https://github.com/Jowzera/data-application-pjs/assets/51763929/c99b5ac3-9aec-4725-a790-b22c196780f3)
