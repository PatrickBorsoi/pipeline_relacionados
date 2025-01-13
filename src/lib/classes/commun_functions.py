import time
from datetime import datetime, timedelta
import re
import pandas as pd

def agendar_execucao_do_codigo(data_atual):
    'Agendamento de tarefas, so iniciar depois da hora X'
    
    agendamento_da_execucao = data_atual.replace(hour=4, minute=00, second=00, microsecond=0)
    
    if data_atual >= agendamento_da_execucao:
        agendamento_da_execucao += timedelta(days=1)
    
    segundos_restantes = (agendamento_da_execucao - data_atual).total_seconds()

    print(f'Aguardando {segundos_restantes} segundos para executar')
    time.sleep(segundos_restantes)




def renomeando_id_obra_para_busca_na_api(obra) -> str:
    'Validação do processo'
    if obra.endswith('-1') or obra.endswith('-2'):
        obra = obra[:-2]
    if obra.startswith('SEI-') or obra.startswith('E-') or obra.startswith('e-'):
        obra_sei = obra
    else:
        obra_sei = 'SEI-' + obra
    return obra_sei



def remover_tags_do_nome_do_documento_e_numero_do_documento(df: pd.DataFrame) -> dict:
    

    """
    Remover as tags que vem no nome do documento, 
    Retornando somento o nome do documento,
    Extraindo somente o numero do documento,
    Extraindo o ano do id relacionado. 
    """

    lista_numero_documento: list = []
    lista_documento: list = []
    lista_de_ano: list = []
    for index, rows in df.iterrows():
        documento: str = rows['ultimo documento']
        id_relacionado: str = rows['Id relacionado']
        numero_documento: str = re.findall(r'\d+', documento)

        if numero_documento:
            lista_numero_documento.append(numero_documento[0])


        if 'Assinado' in documento:
            match = re.search(r'Assinado (.*?) por', documento)
            if match:
                frase = match.group(1).strip()
                lista_documento.append(frase)
        else:
            lista_documento.append(documento)


        ano: str = id_relacionado[-4:]
        ano: int = ano
        lista_de_ano.append(ano)


    return {
        'numero do documento' : lista_numero_documento,
        'ultimo documento' : lista_documento,
        'ano do relacionado' : lista_de_ano
    }



def verificar_arquivo_atual_com_arquivo_anterior(df_dia_atual: pd.DataFrame, df_dia_anterior: pd.DataFrame, nome_da_coluna:str):
    df_resultado = pd.merge(df_dia_atual, df_dia_anterior, how='left', on= nome_da_coluna, suffixes=[None, '_y'], indicator=True)
    df_resultado = df_resultado.loc[df_resultado['_merge'] != "both"]
    df_resultado = df_resultado.drop(columns=['_merge'])
    df_resultado = df_resultado.loc[:, ~df_resultado.columns.str.endswith('_y')]
    return df_resultado



def salvar_progresso(arquivo_progresso, id_atual):
    with open(arquivo_progresso, "w") as f:    
        f.write(str(id_atual))

def ler_progresso(arquivo_progresso):
    try:
        with open(arquivo_progresso, "r") as f:
            return int(f.read().strip())  # Retorna o último ID processado
    except (FileNotFoundError, ValueError):
        return None  # Se não
