import xmltodict
import pandas as pd
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

from lib.classes.connection_api_google import *
from lib.classes.connection_api_google import *
from lib.classes.generate_date import *
from lib.classes.colettor_xml import *
from lib.classes.commun_functions import *



def pipeline_gerar_novos_arquivos_relacionados():
    load_dotenv('.env')

    pasta_id = os.getenv('ID_PASTA')
    nome_arquivo = os.getenv('NOME_DO_ARQUIVO')

    data_atual_anteior = DatasAtualEAnterior()
    data_atual_formatada = data_atual_anteior.data_atual_formatada
    data_anterior_formatada = data_atual_anteior.data_anterior

    database_principal = connection_database_tabela_mae_google_sheets(
        id_da_pasta=pasta_id,
        nome_do_arquivo=nome_arquivo,
        numero_da_aba=0
    )

    df_database_principal = show_data(planilha=database_principal)
    df_database_principal_sem_fase_transferido = df_database_principal[df_database_principal[' FASE'] != 'TRANSFERIDO']

    path_unidades = r'C:\Users\usuario\Desktop\workspace_patrick\02_pipeline_relacionado\database\unidades_seiop.xlsx'
    unidades_seiop = pd.read_excel(path_unidades)
    id_unidade_seiop = unidades_seiop['ID_UNIDADE']

    lista_processo_mae: list = []
    lista_processo_relacionado:list = []
    lista_que_nao_existe_relacionado:list = []


    progress_file = 'src/lib//pipelines/progress_file.txt'
    salvar_progresso(progress_file, None)
    verificacao_de_erro = False
    while verificacao_de_erro == False:
        try:
            ultimo_id_processado = ler_progresso(progress_file)

            for i, obra in df_database_principal_sem_fase_transferido.iterrows():
                obra = obra['Id Obra']
                id_obra = renomeando_id_obra_para_busca_na_api(obra= obra)
                
                if ultimo_id_processado and i <= ultimo_id_processado:
                    if len(id_obra) -1 == ultimo_id_processado:
                        break
                    continue

                unidade = False
                while unidade == False:
                    for id_unidade in id_unidade_seiop:
                        
                        resposta_do_xml_consultar_procedimento = xml_consultar_procedimento(id_obra=id_obra, id_unidade=id_unidade)
                        
                        # Verificando se a requisição passou
                        if resposta_do_xml_consultar_procedimento.status_code == 200:
                            unidade = True

                            texto_xml_consultar_procedimento = resposta_do_xml_consultar_procedimento.text #Transformando o xml em texto
                            
                            parsed_dict = xmltodict.parse(texto_xml_consultar_procedimento)#Passando o texto para um dicionario de dados
                            
                            parametros_de_inicio = parsed_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:consultarProcedimentoResponse']['parametros']
        

                            numero_de_relacionados: int = parametros_de_inicio['ProcedimentosRelacionados']['@SOAP-ENC:arrayType']
                            numero_relacionados = re.search(r'\[(\d+)\]', numero_de_relacionados)
                            numero_relacionado = int(numero_relacionados.group(1))
                            if numero_relacionado == 0:
                                lista_que_nao_existe_relacionado.append(id_obra)
                                print(f'processo mãe que não tem relacionado {id_obra} \n qtd_de_processos que nao tem relacionado {len(lista_que_nao_existe_relacionado)}')
                                break
                            lista_de_ids_relacionados: list = parametros_de_inicio['ProcedimentosRelacionados']['item']

                            if lista_de_ids_relacionados:
                                if numero_relacionado == 1:
                                    id_relacionado = lista_de_ids_relacionados['ProcedimentoFormatado']['#text']
                                    lista_processo_mae.append(id_obra)
                                    lista_processo_relacionado.append(id_relacionado)
                                if numero_relacionado > 1:
                                    for lista_de_relacionados in lista_de_ids_relacionados:
                                        id_relacionado = lista_de_relacionados['ProcedimentoFormatado']['#text']
                                        lista_processo_mae.append(id_obra)
                                        lista_processo_relacionado.append(id_relacionado)
                        
                            salvar_progresso(progress_file, i)
                            
                            print(f'\n\n qtd total de processo mãe {len(set(lista_processo_mae)) + len(set(lista_que_nao_existe_relacionado))} \n\n Processo mãe {id_obra} \n qtd processo mãe {len(set(lista_processo_mae))} \n qtd processo relacionado = {len(lista_processo_relacionado)} \n\n')
                            break
                
                    if len(df_database_principal_sem_fase_transferido) -1 == len(set(lista_processo_mae)) + len(set(lista_que_nao_existe_relacionado)):
                        verificacao_de_erro = True

                        break
        except Exception:
            verificacao_de_erro = False
            print('Deu erro no primeiro Try')
    
    dicionario_gerar_novos_relacionados ={
        'Id Obra' : lista_processo_mae,
        'id_relacionado': lista_processo_relacionado
    }

    df_relacionados = pd.DataFrame(dicionario_gerar_novos_relacionados)

    path_file_relacionado_database = 'C:/Users/usuario/Desktop/workspace/00-database/id_relacionado'

    df_relacionados.to_excel(f'{path_file_relacionado_database}/01-database_relacionados_atual.xlsx',index=False)