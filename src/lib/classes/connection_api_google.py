import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from lib.classes.generate_date import *
import os


def connection_database_tabela_mae_google_sheets( 
        numero_da_aba: int, 
        nome_do_arquivo: str, 
        id_da_pasta:str
    ):

    load_dotenv('.env')
    path_credentials = os.getenv('path_cred')

    filename = path_credentials

    scopes =[
        "https://spreadsheets.google.com/feeds", # google sheets
        "https://www.googleapis.com/auth/drive", # google drive
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        filename=filename,
        scopes=scopes
    )

    #Autorize credentials
    client = gspread.authorize(credentials=creds)

    print(client)
    #titulo e o nome da planilha / folder id e o id da planilha onde ele esta
    sheet = client.open(
        title=nome_do_arquivo,
        folder_id=id_da_pasta
    )

    sheet_table = sheet.get_worksheet(numero_da_aba)

    return sheet_table

def show_data(planilha):
    recebendo_dados = planilha
    dados = recebendo_dados.get_all_records()
    df_dados = pd.DataFrame(dados)
    return df_dados


def inserir_linhas_tabela_relacionada(numero_da_aba):
    todas_as_datas = DatasAtualEAnterior()
    dia_atual_formatado = todas_as_datas.data_atual_formatada
    
    load_dotenv('.env')

    arquivo_json = os.getenv('ARQUIVO_JSON')
    path_credentials = os.getenv('path_cred')
    FOLDER_ID = os.getenv('ID_PASTA_ATUALIZACOES')
    TITLE = os.getenv('NOME_DO_ARQUIVO_DE_ATUALIZACOES')

    filename = path_credentials
    
    scopes = [
        "https://spreadsheets.google.com/feeds", # google sheets
        "https://www.googleapis.com/auth/drive", # google drive
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        filename= filename,
        scopes= scopes
    )
    
    client = gspread.authorize(credentials=creds)
    
    planilha_completa = client.open(title=TITLE, folder_id=FOLDER_ID)

    aba_do_google_sheets = planilha_completa.get_worksheet(numero_da_aba)

    show_data(aba_do_google_sheets)
    nome_do_arquivo = f'{dia_atual_formatado}_lista_completa_tabela_mae_transformado.xlsx'
    path_database_documentos_relacionados_comparativo = rf'C:\Users\usuario\Desktop\workspace_patrick\02_pipeline_relacionado\database\database_documentos_relacionados\{dia_atual_formatado}-comparativo_arquivos_relacionados.xlsx'
    
    read_df_diario = pd.read_excel(path_database_documentos_relacionados_comparativo)

    dados_lista = read_df_diario.values.tolist()
    aba_do_google_sheets.append_rows(dados_lista, value_input_option='USER_ENTERED')