from jinja2 import Template

email_template = """
Houveram {{ num_codigos }} códigos atualizados na data de {{ data }} e para consultar visite sua
pasta de códigos atualizados no Google Drive <a href="{{ google_drive_link }}">'Meu GoogleDrive'</a>.

Para sua comodidade pode também consultar pelo telegram usando o número do código
e saber qual tipo de atualização ocorreu.

Atenciosamente,
Jonathan-CEO
Consultech
"""

dados_email = {
    "num_codigos": 5,
    "data": "01-07-2024",
    "google_drive_link": "https://drive.google.com/your-link-here"
}

template = Template(email_template)
email_renderizado = template.render(dados_email)
print(email_renderizado)
