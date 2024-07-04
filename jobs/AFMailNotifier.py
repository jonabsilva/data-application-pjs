from scripts.processing.datalake.ingestor import *
from jobs.AFEsaj import *
from jobs.AFGdrive import *
from scripts.reporting.gmail_notifier import send_email
from jobs.helper import Helper

from jinja2 import Template


## O CLIENT SERA CHAMDO DO CONCRETE PRODUCT
pattern = Helper()

nome = "BNS"
num_codigos = "2"
data_atual = "01-07-07-2024"
google_drive_link = "https://drive.google.com/your-link-here"

excel_path = "/Users/jonathanbrunosilva/Documents/JJTECH/BNS/POC/poc-E-SAJ.xlsx"
def send_notification():
    sender_email = pattern.get_env_variables(var="EMAIL_USER")
    sender_password = pattern.get_env_variables(var="EMAIL_PASS")
    recipient_emails = pattern.get_customer_info("config/conf-vars.json", "email_address")
    email_template = pattern.get_email_body('config/email_body.txt')
    if recipient_emails == []:
        print("No recipient_emails set, please add one e-mail address")
    else:
        # Set a default email address to sent in cc
        cc_emails = pattern.get_customer_info("config/conf-vars.json", "email_cc")
        subject = "Relatório de Dados"
        template = Template(email_template)
        dados_email = {
            "nome": nome,
            "num_codigos": num_codigos,
            "data": data_atual,
            "google_drive_link": google_drive_link
        }
        # Enviar o e-mail
        send_email(sender_email, sender_password, 
                recipient_emails, cc_emails, 
                subject, template.render(dados_email), excel_path)
