import requests
import time

def xml_listar_andamentos(id_unidade, relacionado):

    url_homologacao = "https://www.hml.sei.rj.gov.br/sei/ws/SeiWS.php"

    url_producao = "https://sei.rj.gov.br/sei/ws/SeiWS.php"

    headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": "SeiAction",
    }

    SIGLA_SISTEMA = 'SEIOP_SEI'

    IDENTIFICACAO_SERVICO = 'SERVICO_SEIOP'

    xml_processo_mae_com_relacionados = f"""
    <x:Envelope
        xmlns:x="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:sei="Sei">
        <x:Header/>
        <x:Body>
            <sei:listarAndamentos>
                <sei:SiglaSistema>{SIGLA_SISTEMA}</sei:SiglaSistema>
                <sei:IdentificacaoServico>{IDENTIFICACAO_SERVICO}</sei:IdentificacaoServico>
                <sei:IdUnidade>{id_unidade}</sei:IdUnidade>
                <sei:ProtocoloProcedimento>{relacionado}</sei:ProtocoloProcedimento>
                <sei:SinRetornarAtributos>N</sei:SinRetornarAtributos>
                <sei:Andamentos/>
                <sei:Tarefas>
                    <sei:IdTarefa>5</sei:IdTarefa>
                    <sei:IdTarefa>13</sei:IdTarefa>
                </sei:Tarefas>
                <sei:TarefasModulos/>
            </sei:listarAndamentos>
        </x:Body>
    </x:Envelope>
    """
    time.sleep(1)
    resposta_api = requests.post(url_producao, data=xml_processo_mae_com_relacionados, headers=headers)
    
    return resposta_api



def xml_consultar_procedimento(id_unidade, id_obra):

    url_homologacao = "https://www.hml.sei.rj.gov.br/sei/ws/SeiWS.php"

    url_producao = "https://sei.rj.gov.br/sei/ws/SeiWS.php"

    headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": "SeiAction",
    }

    SIGLA_SISTEMA = 'SEIOP_SEI'

    IDENTIFICACAO_SERVICO = 'SERVICO_SEIOP'

    xml_processo_mae_com_relacionados = f"""
        <x:Envelope
            xmlns:x="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:sei="Sei">
            <x:Header/>
            <x:Body>
                <sei:consultarProcedimento>
                    <sei:SiglaSistema>{SIGLA_SISTEMA}</sei:SiglaSistema>
                    <sei:IdentificacaoServico>{IDENTIFICACAO_SERVICO}</sei:IdentificacaoServico>
                    <sei:IdUnidade>{id_unidade}</sei:IdUnidade>
                    <sei:ProtocoloProcedimento>{id_obra}</sei:ProtocoloProcedimento>
                    <sei:SinRetornarAssuntos>N</sei:SinRetornarAssuntos>
                    <sei:SinRetornarInteressados>N</sei:SinRetornarInteressados>
                    <sei:SinRetornarObservacoes>N</sei:SinRetornarObservacoes>
                    <sei:SinRetornarAndamentoGeracao>N</sei:SinRetornarAndamentoGeracao>
                    <sei:SinRetornarAndamentoConclusao>N</sei:SinRetornarAndamentoConclusao>
                    <sei:SinRetornarUltimoAndamento>N</sei:SinRetornarUltimoAndamento>
                    <sei:SinRetornarUnidadesProcedimentoAberto>N</sei:SinRetornarUnidadesProcedimentoAberto>
                    <sei:SinRetornarProcedimentosRelacionados>S</sei:SinRetornarProcedimentosRelacionados>
                    <sei:SinRetornarProcedimentosAnexados>N</sei:SinRetornarProcedimentosAnexados>
                </sei:consultarProcedimento>
            </x:Body>
        </x:Envelope>
        """
    time.sleep(1)
    resposta_api = requests.post(url_producao, data=xml_processo_mae_com_relacionados, headers=headers)
    
    return resposta_api