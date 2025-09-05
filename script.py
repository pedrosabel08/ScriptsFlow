import os
import xml.etree.ElementTree as ET
import pymysql
import subprocess
from datetime import datetime
from ftplib import FTP
import requests
import logging
import re
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

PARENT_FOLDER = r"C:\Backburner_Job"
EXCLUDE_KEYWORD = "ANIMA"

# Arquivo de log
LOG_FILE = os.path.join(PARENT_FOLDER, "processamento.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

def log_and_print(msg, level="info"):
    """Função para logar e imprimir no console"""
    print(msg)
    if level == "info":
        logging.info(msg)
    elif level == "error":
        logging.error(msg)
    elif level == "warning":
        logging.warning(msg)

# Conexão com banco - AGORA LENDO TUDO DO .ENV
conn = pymysql.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    database=os.getenv("DB_NAME"),
    charset='utf8mb4'
)

def get_prefix(name: str) -> str:
    if not name:
        return ""
    
    # Captura número(s) + ponto + espaço opcional + letras + underline + letras
    match = re.match(r'^(\d+\.\s*[A-Za-z]+_[A-Za-z]+)', name)
    if match:
        # Remove espaço depois do ponto, se houver
        return match.group(1).replace(" ", "")
    return name


def upload_to_ftp(local_path, remote_path, ftp_host, ftp_user, ftp_pass):
    try:
        ftp = FTP(ftp_host, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        log_and_print(f"🌐 Conectado ao FTP: {ftp_host}")

        with open(local_path, "rb") as file:
            ftp.storbinary(f"STOR {remote_path}", file)

        ftp.quit()
        log_and_print(f"✅ Upload concluído: {remote_path}")
        return True
    except Exception as e:
        log_and_print(f"❌ Erro no upload FTP: {e}", "error")
        return False


def parse_xml(xml_path):
    log_and_print(f"Lendo XML: {xml_path}")
    tree = ET.parse(xml_path)
    root = tree.getroot()
    job_info = root.find("JobInfo")
    job_flags = root.find("JobFlags")
    output = root.find(".//Output/Name")
    if output is not None:
        log_and_print("Caminho EXR encontrado: " + output.text)
    else:
        log_and_print("⚠ EXR não encontrado no XML", "warning")
    data = {
        "Active": job_flags.find("Active").text if job_flags is not None else None,
        "Complete": job_flags.find("Complete").text if job_flags is not None else None,
        "Computer": job_info.find("Computer").text if job_info is not None else None,
        "Name": job_info.find("Name").text if job_info is not None else None,
        "Submitted": job_info.find("Submitted").text if job_info is not None else None,
        "Description": job_info.find("Description").text if job_info is not None else None,
        "LastUpdated": job_info.find("LastUpdated").text if job_info is not None else None,
        "ExrPath": output.text if output is not None else None
    }
    log_and_print(f"Dados XML: {data}")
    return data

def check_log(log_path):
    log_and_print(f"Lendo log: {log_path}")
    has_error = False
    errors = []
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if "ERR" in line:
                has_error = True
                errors.append(line.strip())
    log_and_print(f"Erros encontrados: {len(errors)}")
    return has_error, "\n".join(errors)

def find_imagem_id(cursor, name):
    log_and_print(f"Buscando imagem no banco: {name}")

    # 1. Busca exata
    cursor.execute(
        "SELECT idimagens_cliente_obra FROM imagens_cliente_obra WHERE imagem_nome=%s",
        (name,)
    )
    result = cursor.fetchone()
    if result:
        log_and_print(f"Imagem encontrada pelo nome exato: {result[0]}")
        return result[0]

    # 2. Busca pelo prefixo normalizado
    prefix = get_prefix(name)
    if prefix:  # só tenta se tiver prefixo válido
        cursor.execute(
        "SELECT idimagens_cliente_obra FROM imagens_cliente_obra WHERE REPLACE(imagem_nome,' ','') LIKE %s",
        (prefix + '%',)
        )
        result = cursor.fetchone()
        if result:
            log_and_print(f"Imagem encontrada pelo prefixo: {result[0]}")
            return result[0]

    log_and_print("Imagem não encontrada", "warning")
    return None


def find_responsavel_id(cursor, imagem_id):
    log_and_print(f"Buscando imagem no banco: {imagem_id}")
    cursor.execute(
        "SELECT colaborador_id FROM funcao_imagem WHERE funcao_id = 4 AND imagem_id = %s",
        (imagem_id,)
    )
    result = cursor.fetchone()
    if result:
        log_and_print(f"Colaborador encontrado: {result[0]}")
        return result[0]
    log_and_print("Imagem não encontrada", "warning")
    return None

def find_status_id(cursor, imagem_id):
    log_and_print(f"Buscando status atual no banco: {imagem_id}")
    cursor.execute(
        "SELECT status_id FROM imagens_cliente_obra WHERE idimagens_cliente_obra = %s",
        (imagem_id,)
    )
    result = cursor.fetchone()
    if result:
        log_and_print(f"Status atual: {result[0]}")
        return result[0]
    log_and_print("Imagem não encontrada", "warning")
    return None


def process_job_folder(cursor, job_folder):
    folder_name = os.path.basename(job_folder)
    if EXCLUDE_KEYWORD in folder_name.upper():
        log_and_print(f"Ignorado (ANIMA): {job_folder}")
        return

    log_and_print(f"\nProcessando pasta: {job_folder}")

    xml_file = None
    log_file = None
    for f in os.listdir(job_folder):
        if f.lower().endswith(".xml"):
            xml_file = os.path.join(job_folder, f)
        if f.lower().endswith(".txt") or f.lower().endswith(".log"):
            log_file = os.path.join(job_folder, f)

    if not xml_file:
        log_and_print(f"⚠ Nenhum XML encontrado em {job_folder}")
        return
    if not log_file:
        log_and_print(f"⚠ Nenhum log encontrado em {job_folder}")
        return

    xml_data = parse_xml(xml_file)
    has_error, errors = check_log(log_file)

    image_name_xml = xml_data.get("Name")
    if not image_name_xml:
        log_and_print("⚠ Campo <Name> não encontrado no XML", "warning")
        return

    imagem_id = find_imagem_id(cursor, image_name_xml)
    if not imagem_id:
        log_and_print(f"Imagem não encontrada para {image_name_xml}")
        return

    caminho_pasta = os.path.dirname(xml_data.get("ExrPath")) if xml_data.get("ExrPath") else None

    # Buscar informações complementares
    cursor.execute("SELECT imagem_nome FROM imagens_cliente_obra WHERE idimagens_cliente_obra = %s", (imagem_id,))
    row = cursor.fetchone()
    image_name_db = row[0] if row else None

    resp_id = find_responsavel_id(cursor, imagem_id)
    status_id = find_status_id(cursor, imagem_id)

    # Buscar status existente
    cursor.execute("""
        SELECT idrender_alta, status, previa_jpg
        FROM render_alta
        WHERE imagem_id = %s AND status_id = %s
        ORDER BY idrender_alta DESC
        LIMIT 1
    """, (imagem_id, status_id))
    existing_status = cursor.fetchone()

    render_id = existing_status[0] if existing_status else None
    ultimo_status = existing_status[1] if existing_status else None
    existing_preview = existing_status[2] if existing_status else None

    # Determinar status customizado
    active = (xml_data.get("Active") or "").strip().lower()
    complete = (xml_data.get("Complete") or "").strip().lower()

    if complete == "yes":
        status_custom = "Em aprovação"
    elif has_error:
        status_custom = "Erro"
    elif active == "yes" and complete == "no":
        status_custom = "Em andamento"
    else:
        status_custom = complete or "Desconhecido"

    # Caminho remoto fixo para todas as imagens
    remote_base_path = "/www/sistema/uploads/renders/"

    # 1️⃣ Se status atual = Em aprovação e não tiver previa_jpg → atualizar só a coluna
    if ultimo_status == "Em aprovação":
        if not existing_preview and caminho_pasta and os.path.exists(caminho_pasta):
            jpgs = [f for f in os.listdir(caminho_pasta) if f.lower().endswith(".jpg")]
            if jpgs:
                preview_name = jpgs[0]

                # Upload da prévia
                local_path = os.path.join(caminho_pasta, preview_name)
                remote_path = remote_base_path + preview_name  # sempre dentro de /www/sistema/uploads/renders/
                ftp_host = os.getenv("FTP_HOST")
                ftp_user = os.getenv("FTP_USER")
                ftp_pass = os.getenv("FTP_PASS")
                upload_ok = upload_to_ftp(local_path, remote_path, ftp_host, ftp_user, ftp_pass)

                if upload_ok:
                    # Atualiza banco apenas se o upload teve sucesso
                    cursor.execute("""
                        UPDATE render_alta
                        SET previa_jpg = %s
                        WHERE idrender_alta = %s
                    """, (preview_name, render_id))
                    log_and_print(f"🖼️ Previa JPG atualizada para {preview_name} (status já era 'Em aprovação')")
                else:
                    log_and_print(f"⚠ Upload falhou — nenhuma alteração foi feita no banco para {preview_name}", "warning")
            return  # não faz mais nada

    # 2️⃣ Se status atual = Aprovado ou Finalizado → não faz nada
    if ultimo_status in ("Aprovado", "Finalizado"):
        log_and_print(f"⏭ Status '{ultimo_status}' detectado — nenhum update realizado.")
        return

    # 3️⃣ Se status estava como Erro e Complete=Yes → mudar para Em aprovação
    if ultimo_status == "Erro" and complete == "yes":
        status_custom = "Em aprovação"
        log_and_print("🔄 Status alterado de 'Erro' para 'Em aprovação' pois Complete=Yes")

    # 4️⃣ Fluxo normal para Em andamento ou novo registro
    previa_val = None
    if caminho_pasta and os.path.exists(caminho_pasta):
        jpgs = [f for f in os.listdir(caminho_pasta) if f.lower().endswith(".jpg")]
        if jpgs:
            previa_val = jpgs[0]

            # Upload da prévia
            local_path = os.path.join(caminho_pasta, previa_val)
            remote_path = f"previas/{previa_val}"
            ftp_host = os.getenv("FTP_HOST")
            ftp_user = os.getenv("FTP_USER")
            ftp_pass = os.getenv("FTP_PASS")
            upload_to_ftp(local_path, remote_path, ftp_host, ftp_user, ftp_pass)

    cursor.execute("""
        INSERT INTO render_alta
        (imagem_id, responsavel_id, status_id, status, data, computer, submitted, last_updated, has_error, errors, job_folder, previa_jpg, numero_bg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            status=VALUES(status),
            previa_jpg=IFNULL(VALUES(previa_jpg), previa_jpg)
    """, (
        imagem_id,
        resp_id,
        status_id,
        status_custom,
        datetime.now(),
        xml_data.get("Computer"),
        xml_data.get("Submitted"),
        xml_data.get("LastUpdated"),
        has_error,
        errors,
        caminho_pasta,
        previa_val,
        xml_data.get("Description")
    ))

    log_and_print(f"✅ Render atualizado/inserido — status={status_custom}, previa_jpg={previa_val}")


    
    # 🔹 Buscar render_id com imagem_id e status_id
    cursor.execute(
        "SELECT idrender_alta FROM render_alta WHERE imagem_id = %s AND status_id = %s",
        (imagem_id, status_id)
    )
    render_row = cursor.fetchone()
    render_id = render_row[0] if render_row else None

    # 🔹 Buscar obra_id pela imagem_id (não precisa mudar nada aqui)
    cursor.execute(
        "SELECT obra_id FROM imagens_cliente_obra WHERE idimagens_cliente_obra = %s",
        (imagem_id,)
    )
    obra_row = cursor.fetchone()
    obra_id = obra_row[0] if obra_row else None

    # 🔹 Buscar responsável da pós-produção (funcao_id = 5)
    cursor.execute(
        "SELECT colaborador_id FROM funcao_imagem WHERE funcao_id = 5 AND imagem_id = %s",
        (imagem_id,)
    )
    pos_row = cursor.fetchone()
    responsavel_pos_id = pos_row[0] if pos_row else None

    # 🔹 Inserir ou atualizar na tabela pós-produção
    cursor.execute("""
        INSERT INTO pos_producao
        (render_id, imagem_id, obra_id, colaborador_id, caminho_pasta, numero_bg, status_id, responsavel_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            obra_id = VALUES(obra_id),
            colaborador_id = VALUES(colaborador_id),
            caminho_pasta = VALUES(caminho_pasta),
            numero_bg = VALUES(numero_bg),
            status_id = VALUES(status_id),
            responsavel_id = VALUES(responsavel_id)
    """, (
        render_id,
        imagem_id,
        obra_id,
        resp_id,
        caminho_pasta,
        xml_data.get("Description"),
        status_id,
        responsavel_pos_id
    ))

    log_and_print(f"📌 Pós-produção vinculada: render_id={render_id}, imagem_id={imagem_id}, obra_id={obra_id}")
    
    
    # AGORA LENDO TUDO DO .ENV
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    flow_token = os.getenv("FLOW_TOKEN")

    # 1️⃣ Enviar mensagem para o canal de renders via webhook
    def send_webhook_message(message):
        payload = {"text": message}
        try:
            response = requests.post(slack_webhook_url, json=payload)
            if response.status_code == 200:
                log_and_print(f"✅ Mensagem enviada para o canal de renders!")
            else:
                log_and_print(f"❌ Erro ao enviar para o canal de renders: {response.text}")
        except Exception as e:
            log_and_print(f"❌ Exceção ao enviar webhook: {e}")

    # 2️⃣ Buscar ID real do usuário pelo nome
    def get_user_id_by_name(user_name):
        url = "https://slack.com/api/users.list"
        headers = {"Authorization": f"Bearer {flow_token}"}
        try:
            response = requests.get(url, headers=headers)
            data = response.json()
            if not data.get("ok"):
                log_and_print(f"❌ Erro na API users.list: {data.get('error')}")
                return None
            for member in data.get("members", []):
                if "real_name" in member and member["real_name"].lower() == user_name.lower():
                    return member["id"]
            log_and_print(f"❌ Usuário {user_name} não encontrado no Slack.")
            return None
        except Exception as e:
            log_and_print(f"❌ Exceção ao buscar usuário {user_name}: {e}")
            return None

    # 3️⃣ Enviar DM usando API Slack
    def send_dm_to_user(user_id, message):
        url = "https://slack.com/api/chat.postMessage"
        headers = {
            "Authorization": f"Bearer {flow_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "channel": user_id,
            "text": message
        }
        try:
            response = requests.post(url, json=payload, headers=headers)
            data = response.json()
            if response.status_code == 200 and data.get("ok"):
                log_and_print(f"✅ DM enviada para {user_id} com sucesso!")
            else:
                log_and_print(f"❌ Erro ao enviar DM para {user_id}: {data.get('error', response.text)}")
        except Exception as e:
            log_and_print(f"❌ Exceção ao enviar DM para {user_id}: {e}")

    # -------------------------------
    # Lógica de notificação
    # -------------------------------
    if resp_id and status_custom != ultimo_status:
        if status_custom == "Erro":
            msg = f"O render da imagem: {image_name_db} deu erro, favor verificar!"
        elif status_custom == "Em aprovação":
            msg = f"O render da imagem: {image_name_db} foi concluído com sucesso, favor aprovar!"
        elif status_custom == "Em andamento":
            msg = f"O render da imagem: {image_name_db} está em andamento."
        else:
            msg = None

        if msg:
            # Enviar para canal de renders
            send_webhook_message(msg)

            # Buscar usuário e enviar DM
            cursor.execute("SELECT nome_slack FROM usuario WHERE idcolaborador = %s", (resp_id,))
            slack_names = cursor.fetchall()
            for slack_name_tuple in slack_names:
                slack_name = slack_name_tuple[0]
                user_id = get_user_id_by_name(slack_name)
                if user_id:
                    send_dm_to_user(user_id, msg)

            # Inserir notificação no banco
            cursor.execute(
                "INSERT INTO notificacoes (colaborador_id, mensagem) VALUES (%s, %s)",
                (resp_id, msg)
            )
            log_and_print(f"🔔 Notificação enviada para colaborador {resp_id} e canal de renders.")

        # Atualizar função e imagem
    if status_custom == "Em aprovação":
        cursor.execute("""
            UPDATE funcao_imagem
            SET status = 'Finalizado', prazo = NOW()
            WHERE imagem_id = %s AND funcao_id = 4
        """, (imagem_id,))
        log_and_print(f"Função atualizada para Finalizado para imagem_id {imagem_id}")

        cursor.execute("""
            UPDATE imagens_cliente_obra
            SET substatus_id = 5
            WHERE idimagens_cliente_obra = %s
        """, (imagem_id,))
        log_and_print(f"Imagem atualizada para status = REN na imagem_id {imagem_id}")

def main():
    log_and_print(f"Iniciando processamento da pasta: {PARENT_FOLDER}")
    with conn.cursor() as cursor:
        # Percorre todas as subpastas dentro da pasta raiz
        for root, dirs, files in os.walk(PARENT_FOLDER):
            for d in dirs:
                job_folder = os.path.join(root, d)
                process_job_folder(cursor, job_folder)
        conn.commit()
    log_and_print("Processamento concluído!")

if __name__ == "__main__":
    main()