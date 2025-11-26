import sys
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import getpass
import json
import logging
import smtplib
from email.message import EmailMessage

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

NOME_AUTOMACAO = "PUDIARIO"
NOME_SCRIPT = Path(__file__).stem.upper()
NOME_SERVIDOR = "Servidor.py"
TZ = ZoneInfo("America/Sao_Paulo")
INICIO_EXEC_SP = datetime.now(TZ)
DATA_EXEC = INICIO_EXEC_SP.date().isoformat()
HORA_EXEC = INICIO_EXEC_SP.strftime("%H:%M:%S")
NAVEGADOR_ESCONDIDO = False
REGRAVAREXCEL = False
RETCODE_SUCESSO = 0
RETCODE_FALHA = 1
RETCODE_SEMDADOSPARAPROCESSAR = 2
BQ_TABELA_DESTINO = "project.dataset.table"
BQ_TABELA_METRICAS = "datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec"
EMAILS_PRINCIPAL = "carlos.lsilva@c6bank.com; sofia.fernandes@c6bank.com"
EMAILS_CC = ""
PASTA_INPUT = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / NOME_AUTOMACAO / "arquivos input" / NOME_SCRIPT
PASTA_LOGS = Path.home() / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A" / "Mensageria e Cargas Operacionais - 11.CelulaPython" / "graciliano" / "automacoes" / NOME_AUTOMACAO / "logs" / NOME_SCRIPT / DATA_EXEC
XPATHS = {}
COLUNAS_ESPERADAS = ["codigo_ativo", "valor_pu", "fonte", "data_referencia"]
DTYPES_CSV = {"codigo_ativo": "string", "valor_pu": "string", "fonte": "string", "data_referencia": "string"}

class Execucao:
    def __init__(self):
        self.modo = "AUTO"
        self.usuario = getpass.getuser()

    def is_servidor(self):
        return len(sys.argv) > 1 or "SERVIDOR_ORIGEM" in os.environ or "MODO_EXECUCAO" in os.environ

    def abrir_gui(self):
        from PySide6.QtWidgets import QApplication, QLabel, QWidget, QVBoxLayout, QPushButton
        from PySide6.QtCore import Qt
        app = QApplication.instance() or QApplication([])
        escolha = {"modo": None}

        def selecionar(modo):
            escolha["modo"] = modo
            app.quit()

        janela = QWidget()
        janela.setWindowTitle(f"{NOME_SCRIPT} - EXECUCAO LOCAL")
        layout = QVBoxLayout()
        label = QLabel("Selecione o modo de execução")
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)
        botao_auto = QPushButton("AUTO")
        botao_solicitacao = QPushButton("SOLICITACAO")
        botao_auto.clicked.connect(lambda: selecionar("AUTO"))
        botao_solicitacao.clicked.connect(lambda: selecionar("SOLICITACAO"))
        layout.addWidget(botao_auto)
        layout.addWidget(botao_solicitacao)
        janela.setLayout(layout)
        janela.resize(420, 180)
        janela.show()
        app.exec()
        return escolha["modo"]

    def detectar(self):
        if self.is_servidor():
            self.modo = "AUTO"
            self.usuario = getpass.getuser()
            return self
        try:
            modo_escolhido = self.abrir_gui()
            self.modo = modo_escolhido if modo_escolhido else "AUTO"
            self.usuario = getpass.getuser()
        except Exception:
            self.modo = "AUTO"
            self.usuario = getpass.getuser()
        return self

def preparar_pastas():
    PASTA_INPUT.mkdir(parents=True, exist_ok=True)
    PASTA_LOGS.mkdir(parents=True, exist_ok=True)

def configurar_logger():
    preparar_pastas()
    log_path = PASTA_LOGS / f"{NOME_SCRIPT}_{DATA_EXEC}.log"
    logger = logging.getLogger(NOME_SCRIPT)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.info(f"Logger configurado arquivo={log_path} pasta_input={PASTA_INPUT} pasta_logs={PASTA_LOGS}")
    return logger, log_path

def credencial_valida(logger, caminho):
    try:
        with open(caminho, "r", encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        if isinstance(dados, dict) and dados.get("type") == "service_account" and dados.get("project_id"):
            return True
        logger.info(f"Arquivo de credencial ignorado por formato inválido: {caminho}")
        return False
    except Exception as exc:
        logger.info(f"Falha ao ler credencial candidata {caminho}: {exc}")
        return False

def localizar_credenciais(logger):
    cred_env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_env:
        caminho_env = Path(cred_env)
        logger.info(f"Variável GOOGLE_APPLICATION_CREDENTIALS definida em {caminho_env}")
        if caminho_env.exists() and credencial_valida(logger, caminho_env):
            logger.info("Credenciais localizadas via variável de ambiente")
            return caminho_env
        logger.error("Caminho definido em GOOGLE_APPLICATION_CREDENTIALS não encontrado ou inválido")
    logger.info("Buscando credenciais .json na home do usuário")
    possiveis = sorted(Path.home().rglob("*.json"))
    for caminho in possiveis:
        if ("credential" in caminho.name.lower() or "service" in caminho.name.lower()) and credencial_valida(logger, caminho):
            logger.info(f"Credenciais encontradas em {caminho}")
            return caminho
    logger.error("Nenhuma credencial .json encontrada na busca padrão")
    return None

def criar_cliente_bq(logger):
    logger.info("Iniciando criação do cliente BigQuery")
    cred_path = localizar_credenciais(logger)
    if cred_path is None:
        logger.error("Credenciais BigQuery não encontradas")
        raise FileNotFoundError("Credenciais BigQuery não encontradas")
    try:
        credentials = service_account.Credentials.from_service_account_file(str(cred_path))
        project_id = credentials.project_id
        logger.info(f"Cliente BigQuery usando projeto {project_id}")
        return bigquery.Client(credentials=credentials, project=project_id)
    except Exception as exc:
        logger.exception(f"Erro ao criar cliente BigQuery com credenciais em {cred_path}: {exc}")
        raise

def mover_arquivo(logger, origem, destino_dir):
    destino_dir.mkdir(parents=True, exist_ok=True)
    destino = destino_dir / origem.name
    if destino.exists():
        timestamp = datetime.now(TZ).strftime("%H%M%S")
        destino = destino_dir / f"{origem.stem}_{timestamp}{origem.suffix}"
    shutil.move(str(origem), str(destino))
    logger.info(f"Arquivo movido de {origem} para {destino}")
    return destino

def carregar_dataframe(logger, caminho):
    try:
        if caminho.suffix.lower() in [".xlsx", ".xlsm", ".xls"]:
            logger.info(f"Carregando arquivo Excel {caminho.name}")
            df = pd.read_excel(caminho, dtype=DTYPES_CSV)
            logger.info(f"Arquivo {caminho.name} carregado com {len(df)} linhas")
            return df
        if caminho.suffix.lower() == ".csv":
            logger.info(f"Carregando arquivo CSV {caminho.name}")
            df = pd.read_csv(caminho, dtype=DTYPES_CSV)
            logger.info(f"Arquivo {caminho.name} carregado com {len(df)} linhas")
            return df
        logger.info(f"Formato não suportado para {caminho.name}")
        return None
    except Exception as exc:
        logger.exception(f"Erro ao carregar arquivo {caminho.name}: {exc}")
        raise

def validar_colunas(logger, df):
    colunas_encontradas = list(df.columns)
    valido = colunas_encontradas == COLUNAS_ESPERADAS
    if not valido:
        logger.error(f"Schema inválido. Esperado={COLUNAS_ESPERADAS} Encontrado={colunas_encontradas}")
    return valido

def fracionar_dataframe(df, tamanho):
    inicio = 0
    while inicio < len(df):
        yield df.iloc[inicio : inicio + tamanho]
        inicio += tamanho

def carregar_staging(client, logger, df, staging_id):
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    primeira = True
    for bloco in fracionar_dataframe(df, 50000):
        if not primeira:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = client.load_table_from_dataframe(bloco, staging_id, job_config=job_config)
        job.result()
        logger.info(f"Staging carregada job_id={job.job_id} linhas={bloco.shape[0]} destino={staging_id}")
        primeira = False

def merge_dedup(client, logger, staging_id, destino_id, colunas):
    condicoes = [f"T.{c}=S.{c}" for c in colunas]
    on_clause = " AND ".join(condicoes) if condicoes else "FALSE"
    query = f"""
    MERGE `{destino_id}` T
    USING `{staging_id}` S
    ON {on_clause}
    WHEN NOT MATCHED THEN INSERT ({', '.join(colunas)}) VALUES ({', '.join([f'S.{c}' for c in colunas])})
    """
    logger.info(f"Executando merge deduplicado staging={staging_id} destino={destino_id}")
    logger.info(f"Consulta MERGE: {query}")
    job = client.query(query)
    job.result()
    linhas_inseridas = job.num_dml_affected_rows or 0
    logger.info(f"Merge concluído job_id={job.job_id} linhas_inseridas={linhas_inseridas}")
    return linhas_inseridas

def remover_tabela(client, logger, tabela_id):
    try:
        client.delete_table(tabela_id, not_found_ok=True)
        logger.info(f"Tabela removida {tabela_id}")
    except Exception as exc:
        logger.exception(f"Erro ao remover staging {tabela_id}: {exc}")


def registrar_metricas_execucao(client, logger, execucao, status, tempo_exec):
    linhas = [
        {
            "nome_automacao": NOME_AUTOMACAO,
            "metodo_automacao": NOME_SCRIPT,
            "status": status,
            "modo_execucao": execucao.modo,
            "tempo_exec": tempo_exec,
            "data_exec": DATA_EXEC,
            "hora_exec": HORA_EXEC,
            "usuario": f"{execucao.usuario}@c6bank.com",
            "log_completo": None,
            "execucao_do_dia": None,
            "observacao": None,
            "tabela_referencia": None,
        }
    ]
    try:
        logger.info(f"Registrando métricas execucao={status} tempo={tempo_exec} modo={execucao.modo}")
        erros = client.insert_rows_json(table=BQ_TABELA_METRICAS, json_rows=linhas)
        if erros:
            logger.error(f"Erros ao registrar métricas: {erros}")
        else:
            logger.info("Métricas registradas")
    except Exception as exc:
        logger.exception(f"Falha ao registrar métricas: {exc}")


def montar_email(status, hora_fim, linhas_processadas, linhas_inseridas, motivo_sem_dados):
    linhas_ignoradas = linhas_processadas - linhas_inseridas
    texto_sem_dados = ""
    if status == "SEM DADOS PARA PROCESSAR":
        texto_sem_dados = motivo_sem_dados.upper()
    corpo = f"""
    <html><body style='font-family: Montserrat, sans-serif; text-transform: uppercase;'>
    <p>AUTOMACAO: {NOME_AUTOMACAO}</p>
    <p>SCRIPT: {NOME_SCRIPT}</p>
    <p>STATUS: {status}{texto_sem_dados}</p>
    <p>HORA INICIO: {HORA_EXEC} | HORA FIM: {hora_fim} | TEMPO EXECUCAO: {calcular_tempo_execucao(hora_fim)}</p>
    <p>LINHAS PROCESSADAS: {linhas_processadas} | LINHAS INSERIDAS: {linhas_inseridas} | LINHAS IGNORADAS (DUPLICADAS): {linhas_ignoradas}</p>
    </body></html>
    """
    return corpo


def enviar_email(logger, assunto, corpo, log_path, sucesso, cc):
    mensagem = EmailMessage()
    mensagem["Subject"] = assunto
    mensagem["From"] = EMAILS_PRINCIPAL.split(";")[0].strip()
    mensagem["To"] = EMAILS_PRINCIPAL
    if sucesso and cc:
        mensagem["Cc"] = cc
    mensagem.add_alternative(corpo, subtype="html")
    with open(log_path, "rb") as log_file:
        mensagem.add_attachment(log_file.read(), maintype="application", subtype="octet-stream", filename=Path(log_path).name)
    try:
        with smtplib.SMTP("localhost") as smtp:
            smtp.send_message(mensagem)
        logger.info("Email enviado")
    except Exception as exc:
        logger.exception(f"Falha ao enviar email: {exc}")


def calcular_tempo_execucao(hora_fim):
    fim = datetime.strptime(hora_fim, "%H:%M:%S")
    inicio = datetime.strptime(HORA_EXEC, "%H:%M:%S")
    delta = (fim - inicio) if fim >= inicio else (fim + timedelta(days=1) - inicio)
    total_segundos = int(delta.total_seconds())
    horas = total_segundos // 3600
    minutos = (total_segundos % 3600) // 60
    segundos = total_segundos % 60
    return f"{horas:02d}:{minutos:02d}:{segundos:02d}"


def processar_arquivo(client, logger, caminho):
    logger.info(f"Iniciando processamento do arquivo {caminho.name}")
    df = carregar_dataframe(logger, caminho)
    if df is None:
        logger.info(f"Arquivo ignorado por formato: {caminho.name}")
        return None
    if not validar_colunas(logger, df):
        logger.info(f"Arquivo ignorado por schema: {caminho.name}")
        return None
    staging_id = f"{BQ_TABELA_DESTINO}_staging_{NOME_SCRIPT}".replace("`", "")
    remover_tabela(client, logger, staging_id)
    try:
        carregar_staging(client, logger, df, staging_id)
        linhas_processadas = len(df)
        colunas = df.columns.tolist()
        linhas_inseridas = merge_dedup(client, logger, staging_id, BQ_TABELA_DESTINO, colunas)
        logger.info(f"Processamento concluído para {caminho.name} processadas={linhas_processadas} inseridas={linhas_inseridas}")
        return linhas_processadas, linhas_inseridas
    except Exception as exc:
        logger.exception(f"Erro ao processar {caminho.name}: {exc}")
        raise
    finally:
        remover_tabela(client, logger, staging_id)


def main():
    execucao = Execucao().detectar()
    logger, log_path = configurar_logger()
    logger.info(f"Iniciando script {NOME_SCRIPT} modo={execucao.modo} usuario={execucao.usuario}")
    retcode = RETCODE_SUCESSO
    status_email = "SUCESSO"
    linhas_processadas_total = 0
    linhas_inseridas_total = 0
    motivo_sem_dados = ""
    try:
        arquivos = sorted(PASTA_INPUT.iterdir()) if PASTA_INPUT.exists() else []
        logger.info(f"Arquivos encontrados na pasta input: {len(arquivos)}")
        if not arquivos:
            retcode = RETCODE_SEMDADOSPARAPROCESSAR
            status_email = "SEM DADOS PARA PROCESSAR"
            motivo_sem_dados = f" SEM DADOS PARA PROCESSAR, POIS NAO HAVIA ARQUIVOS NA PASTA {PASTA_INPUT}"
            logger.info(motivo_sem_dados.strip())
        else:
            client = criar_cliente_bq(logger)
            arquivos_validos = []
            for arquivo in arquivos:
                if arquivo.is_file():
                    resultado = processar_arquivo(client, logger, arquivo)
                    if resultado is not None:
                        linhas_proc, linhas_ins = resultado
                        linhas_processadas_total += linhas_proc
                        linhas_inseridas_total += linhas_ins
                        mover_arquivo(logger, arquivo, PASTA_LOGS)
                        arquivos_validos.append(arquivo)
                    else:
                        logger.info(f"Arquivo ignorado sem processamento: {arquivo.name}")
            logger.info(f"Arquivos válidos processados: {len(arquivos_validos)} de {len(arquivos)}")
            if not arquivos_validos:
                retcode = RETCODE_SEMDADOSPARAPROCESSAR
                status_email = "SEM DADOS PARA PROCESSAR"
                motivo_sem_dados = f" SEM DADOS PARA PROCESSAR, POIS HAVIA ARQUIVOS EM {PASTA_INPUT} MAS NENHUM COM O DATAFRAME ESPERADO"
                logger.info(motivo_sem_dados.strip())
    except Exception as exc:
        logger.exception(f"Erro na execução: {exc}")
        retcode = RETCODE_FALHA
        status_email = "FALHA"
    hora_fim = datetime.now(TZ).strftime("%H:%M:%S")
    tempo_exec = calcular_tempo_execucao(hora_fim)
    logger.info(f"Resumo da execução status={status_email} linhas_processadas={linhas_processadas_total} linhas_inseridas={linhas_inseridas_total} tempo_execucao={tempo_exec}")
    try:
        client_metricas = criar_cliente_bq(logger)
        registrar_metricas_execucao(client_metricas, logger, execucao, status_email, tempo_exec)
    except Exception as exc:
        logger.exception(f"Falha ao registrar métricas: {exc}")
    try:
        sucesso = status_email == "SUCESSO"
        assunto = f"CÉLULA PYTHON MONITORAÇÃO - {NOME_SCRIPT} - {status_email}"
        corpo = montar_email(status_email, hora_fim, linhas_processadas_total, linhas_inseridas_total, motivo_sem_dados)
        enviar_email(logger, assunto, corpo, log_path, sucesso, EMAILS_CC)
    except Exception as exc:
        logger.exception(f"Falha ao enviar email: {exc}")
    logger.info(f"Fim da execução status={status_email} retcode={retcode}")
    return retcode

if __name__ == "__main__":
    sys.exit(main())
