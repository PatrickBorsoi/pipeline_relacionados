import pandas as pd


def leitura_database(path_database: str) -> pd.DataFrame:
    database = pd.read_excel(path_database) 
    return database


def exportar_arquivo(df: pd.DataFrame ,path_destino: str):
    df.to_excel(path_destino, index=False)