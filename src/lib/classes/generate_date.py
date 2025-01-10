from datetime import datetime, timedelta

def datas_dia_atual_e_anterior() -> dict:
    f'''
    Receber datas atuais \n
    dia_atual = recebe a data sem formatação nenhuma \n
    data_atual_formatado = recebe a data com formatação \n
    data_anterior = recebe a data do dia anterior sem formatação \n
    data_anterior_formatado = recebe a data do dia anterior com formatação \n
    '''
    data_dia_atual = datetime.now()

    data_atual_formatada = data_dia_atual.strftime('%d-%m-%Y')

    dias_da_semana = data_dia_atual. weekday()

    #Dia da semana (0=segunda, 1=Terça, 2=Quarta, 3=Quinta, 4=sexta, 5=sabado, 6=domingo)
    if dias_da_semana == 0:
        diferenca = timedelta(days = 3)
    else:
        diferenca = timedelta(days = 1)
    data_dia_anterior = data_dia_atual - diferenca

    data_dia_anterior_formato = data_dia_anterior.strftime('%d-%m-%Y')

    return{
    'dia_atual': data_dia_atual,
    'data_atual_formatado': data_atual_formatada,
    'data_anterior': data_dia_anterior,
    'data_anterior_formatado': data_dia_anterior_formato
    }



class DatasAtualEAnterior:
    def __init__(self):
        self.data_atual:datetime = datetime.now()
    
    @property
    def data_atual_formatada(self) -> str:
        return self.data_atual.strftime('%d-%m-%Y')
    
    @property
    def dia_anterior(self) -> str:
        dias_da_semana = self.data_atual.weekday()
        diferenca = timedelta(days=3) if dias_da_semana == 0 else timedelta(days=1)
        dia_anterior = self.data_atual - diferenca
        dia_anterior_formatado = dia_anterior.strftime('%d-%m-%Y')
        return dia_anterior_formatado
