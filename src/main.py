import xmltodict
import pandas as pd
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

from lib.classes.connection_api_google import *
from lib.classes.generate_date import *
from lib.classes.colettor_xml import *
from lib.classes.commun_functions import *
from lib.pipelines.gerar_novos_relacionados import *
from lib.pipelines.gerar_novos_documentos_relacionados import *

while True:
    gerar_datas = DatasAtualEAnterior()
    data_atual_formatada = gerar_datas.data_atual_formatada

    agendar_execucao_do_codigo(data_atual=datetime.now())
    if datetime.now().weekday() < 5:
        pipeline_gerar_novos_arquivos_relacionados()
        gerar_documentos_relacionados()
        comparativo_de_tabelas()
        inserir_linhas_tabela_relacionada(0)
        #up
        