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


datas = DatasAtualEAnterior()

data_atual_formatada = datas.data_atual_formatada
data_anterior_formatada = datas.data_anterior




def gerar_documentos_relacionados():
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

    path_database_relacionados = 'C:/Users/usuario/Desktop/workspace/00-database/id_relacionado/01-database_relacionados_atual_teste.xlsx'
    database_relacionados = pd.read_excel(path_database_relacionados)

    progress_file = r'C:\Users\usuario\Desktop\workspace_patrick\02_pipeline_relacionado\src\lib\pipelines\progress_file.txt'
    salvar_progresso(progress_file, None)

    lista_id_obra: list = []

    lista_fase: list = []
    
    lista_id_relacionado: list = []
    
    lista_ultimo_documento: list = []
    
    lista_data_extracao: list = []

    lista_relacionados_nao_encontrados:list = []

    lista_especificacao:list = []

    ultimo_id_processado = ler_progresso(progress_file)

    for i, rows in database_relacionados.iterrows():
        try:
            id_obra = rows['Id Obra']
            id_relacionado = rows['id_relacionado']
            
            if ultimo_id_processado and i <= ultimo_id_processado:
                if i == ultimo_id_processado:
                    break
                continue

            unidade = False

            while unidade == False:

                for i_unidade, rows in unidades_seiop.iterrows():
                    id_unidade = rows['ID_UNIDADE']
                    
                    resposta_xml_listar_andamentos = xml_listar_andamentos(id_unidade=id_unidade, relacionado=id_relacionado)
                    resposta_xml_colsultar_procedimento = xml_consultar_procedimento(id_unidade=id_unidade, id_obra=id_relacionado)
                    if resposta_xml_listar_andamentos.status_code == 200 and resposta_xml_colsultar_procedimento.status_code == 200:
                        
                        unidade =True

                        xml_txt_listar_andamentos = resposta_xml_listar_andamentos.text
                        xml_txt_consultar_procedimento = resposta_xml_colsultar_procedimento.text
                        parsed_dict = xmltodict.parse(xml_txt_listar_andamentos)
                        parsed_dict_consultar_procedimento = xmltodict.parse(xml_txt_consultar_procedimento)

                        parametros = parsed_dict['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:listarAndamentosResponse']['parametros']
                        # parametros_consultar_procedimento = parsed_dict_consultar_procedimento['SOAP-ENV:Envelope']['SOAP-ENV:Body']['ns1:listarAndamentosResponse']['parametros']

                        try:
                            itens:list = parametros['item']
                        except Exception:
                            pass

                        try:
                            if isinstance(itens,list):
                                itens:list = parametros['item'][0]

                                descricao = itens['Descricao']['#text']

                                tamanho_da_lista_de_itens:int = len(itens)

                                item = 0
                                while 'Este documento foi excluído' in descricao:
                                    itens: list = parametros['item'][item]
                                    descricao = itens['Descricao']['#text']
                                    item += 1

                            else:
                                itens: list = parametros['item']
                                descricao = itens['Descricao']['#text']
                            
                            lista_id_obra.append(id_obra)
                            lista_id_relacionado.append(id_relacionado)
                            lista_ultimo_documento.append(descricao)
                            lista_data_extracao.append(data_atual_formatada)

                            print(f'{len(lista_id_obra)} | {id_obra} \n {len(lista_id_relacionado)} | {id_relacionado} \n {len(lista_ultimo_documento)} | {descricao} \n {len(lista_data_extracao)} | {data_atual_formatada} \n ')
                            # print(f'{len(lista_id_relacionado)} | {id_relacionado} \n {len(lista_ultimo_documento)} | {descricao} \n {len(lista_data_extracao)} | {data_atual_formatada} \n ')
                            print(f'{len(set(lista_id_relacionado))}')
                        except Exception:
                            pass
                        salvar_progresso(progress_file, i)

                        if len(set(lista_id_relacionado)) == len(set(database_relacionados['id_relacionado'])) -2:
                            finalizou = True
                            break
                        else:
                            finalizou = False
                            break
                    if i_unidade == len(unidades_seiop)-1:
                        lista_relacionados_nao_encontrados.append(id_relacionado)
                        unidade = True
                        break
        except Exception as e:
            print(e)
            time.sleep(5)
            continue        
    dicionario_de_documento_relacionado = {
        'Id Obra' : lista_id_obra,
        'Id relacionado' : lista_id_relacionado,
        'ultimo documento': lista_ultimo_documento,
        'data extracao' : lista_data_extracao
    }

    df_relacionados = pd.DataFrame(dicionario_de_documento_relacionado)



    lista_numero_ultimo_documento_ano = remover_tags_do_nome_do_documento_e_numero_do_documento(df=df_relacionados)

    nome_do_documento = lista_numero_ultimo_documento_ano['ultimo documento']
    numero_do_documento = lista_numero_ultimo_documento_ano['numero do documento']
    ano_do_id_relacionado = lista_numero_ultimo_documento_ano['ano do relacionado']

    df_relacionados['ultimo documento'] = nome_do_documento
    df_relacionados['numero do documento'] = numero_do_documento
    df_relacionados['ano do id relacionado'] = ano_do_id_relacionado
    
    path_database_documentos_relacionados = rf'C:\Users\usuario\Desktop\workspace_patrick\02_pipeline_relacionado\database\database_documentos_relacionados\{data_atual_formatada}-arquivos_relacionados.xlsx'
    exportar_arquivo(df=df_relacionados, path_destino=path_database_documentos_relacionados) # exportando o data frame para a pasta


def comparativo_de_tabelas():
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
    
    # Caminho do documento completo do dia atual da pasta desse arquivo
    path_database_documentos_relacionados = rf'C:\Users\usuario\Desktop\workspace_patrick\02_pipeline_relacionado\database\database_documentos_relacionados\{data_atual_formatada}-arquivos_relacionados.xlsx'
    df_relacionado_do_dia = leitura_database(path_database=path_database_documentos_relacionados)

    # Enviar o df_relacionado do dia para esse caminho onde estao todos as listas de relacionados 
    path_relacionado_do_dia = f'C:/Users/usuario\Desktop/workspace_patrick/00-database/id_relacionado_documento/lista_completa/{data_atual_formatada}_ids_relacionados_lista_completa.xlsx'
    exportar_arquivo(df=df_relacionado_do_dia, path_destino=path_relacionado_do_dia)

    # Caminho do documento completo do dia anterior 
    path_relacionado_do_dia_anterior = f'C:/Users/usuario\Desktop/workspace_patrick/00-database/id_relacionado_documento/lista_completa/{data_anterior_formatada}_ids_relacionados_lista_completa.xlsx'
    # path_relacionado_do_dia_anterior = f'C:/Users/usuario\Desktop/workspace_patrick/00-database/id_relacionado_documento/lista_completa/17-04-2025_ids_relacionados_lista_completa.xlsx'
    # Leitura do documento completo o caminho e la na pasta onde esta todos os arquivos completos
    df_atual = leitura_database(path_database=path_relacionado_do_dia)

    # Leitura do documento completo o caminho e la na pasta onde esta todos os arquivos completos
    df_dia_anterior = leitura_database(path_database=path_relacionado_do_dia_anterior)

    # Caminho da pasta da rede para enviar o arquivo pra la
    path_arquivo_comparativo_pasta_da_rede = f'Z:/SUPGESCO/1. ATUALIZAÇÕES/2. PROCESSO RELACIONADO/relacionados_comparativo_{data_atual_formatada} teste.xlsx'
    # path_arquivo_comparativo = f'C:/Users/usuario/Desktop/workspace_patrick/02_pipeline_documentos_relacionados/data/arquivo_comparativo/{data_atual_formatada} relacionados comparativos teste.xlsx'
    
    # Coluna que vou comparar no merge
    coluna_analisada = 'ultimo documento'

    # Comparação dos arquivos completos do dia atual e do dia anteior
    df_unificado = verificar_arquivo_atual_com_arquivo_anterior(df_dia_atual=df_atual, df_dia_anterior=df_dia_anterior, nome_da_coluna=coluna_analisada)

    # Caminho pasta atual do arquivo com o arquivo comparativo
    path_database_documentos_relacionados_comparativo = rf'C:\Users\usuario\Desktop\workspace_patrick\02_pipeline_relacionado\database\database_documentos_relacionados\{data_atual_formatada}-comparativo_arquivos_relacionados.xlsx'
    exportar_arquivo(df=df_unificado, path_destino=path_database_documentos_relacionados_comparativo)
    
    # Enviar o arquivo comparativo para a rede
    path_arquivo_comparativo_pasta_da_rede = f'Z:/SUPGESCO/1. ATUALIZAÇÕES/2. PROCESSO RELACIONADO/{data_atual_formatada}-comparativo_arquivos_relacionados.xlsx'
    exportar_arquivo(df=df_unificado, path_destino=path_arquivo_comparativo_pasta_da_rede)


    # path_relacionado_dia_anterior = f'C:/Users/usuario\Desktop/workspace_patrick/00-database/id_relacionado_documento/lista_completa/{data_anterior_formatada}_ids_relacionados_lista_completa.xlsx'
    # arquivo_dia_atual = pd.read_excel(path_database_documentos_relacionados)
    # arquivo_dia_atual = pd.read_excel(path_relacionado_do_dia)
    # arquivo_dia_anterior = pd.read_excel(path_relacionado_do_dia_anterior)
    
    # df_relacionados_fase = pd.merge(arquivo_dia_atual, df_database_principal_sem_fase_transferido, how='inner', on=['Id Obra'], suffixes=[None, '_y'])
    
    # exportar_arquivo(df=df_relacionados_fase, path_destino=path_relacionado_do_dia) # exportando o data frame para a pasta
    
