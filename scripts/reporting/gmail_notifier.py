import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os


def send_email(sender_email: str, 
               sender_password: str, 
               recipient_emails, 
               cc_emails, 
               subject: str, 
               body: str, 
               attachment_path: str):
    # Configurar o objeto MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_emails)
    msg['Cc'] = ", ".join(cc_emails)
    msg['Subject'] = subject
    
    # Adicionar o corpo do e-mail
    msg.attach(MIMEText(body, 'html'))

    # Anexar o arquivo
    with open(attachment_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(attachment_path)}")
    msg.attach(part)
    
    # Configurar o servidor SMTP
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)
    
    # Enviar o e-mail
    text = msg.as_string()
    to_addrs = recipient_emails + cc_emails
    server.sendmail(sender_email, to_addrs, text)
    server.quit()
