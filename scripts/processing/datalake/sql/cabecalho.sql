select 
    Codigo_do_Processo as codigo_do_processo,
    Situacao_projeto as situacao_projeto,
    Classe as class,
    Assunto as assunto,
    Foro as foro,
    Vara as vara,
    Juiz as juiz,
    Distribuicao as distribuicao,
    Date as dt_date,
    Hora as time_Hora,
    Status as status,
    Controle as controle,
    Area as area,
    Valor_da_acao as valor_da_acao
from df_table
