import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
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
