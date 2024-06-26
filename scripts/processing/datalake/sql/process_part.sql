select 
    Codigo_do_Processo as codigo_do_processo,
    Situacao_projeto as situacao_projeto,
    Classe as class,
    Assunto as assunto,
    Foro as foro,
    Vara as vara,
    Juiz as juiz,
    Date as dt_date,
    Hora as time_Hora,
    Status as status,
    Controle as controle,
    Area as area,
    replace(Valor_da_acao, 'R$         ','') as valor_da_acao
from df_table
