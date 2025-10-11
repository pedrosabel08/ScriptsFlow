import os
from ftplib import FTP
import pymysql
from pathlib import Path

PASTA_LOCAL = "./imagens_colaboradores"
PASTA_REMOTA = "/www/sistema/uploads/colaboradores"

FTP_CONFIG = {
    "host": "ftp.improov.com.br",
    "port": 21,
    "user": "improov",
    "passwd": "Impr00v"
}

DB_CONFIG = {
    "host": "mysql.improov.com.br",
    "user": "improov",
    "password": "Impr00v",
    "database": "improov",
    "charset": "utf8mb4"
}

def enviar_imagens():
    # --- Conecta ao FTP ---
    ftp = FTP()
    ftp.connect(FTP_CONFIG["host"], FTP_CONFIG["port"])
    ftp.login(FTP_CONFIG["user"], FTP_CONFIG["passwd"])
    ftp.cwd(PASTA_REMOTA)

    # --- Conecta ao banco ---
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        arquivos = [f for f in os.listdir(PASTA_LOCAL) if f.lower().endswith((".jpg", ".jpeg", ".png"))]

        for arquivo in arquivos:
            nome_base = Path(arquivo).stem
            caminho_local = os.path.join(PASTA_LOCAL, arquivo)
            caminho_remoto = f"{PASTA_REMOTA}/{arquivo}"

            # --- Envia o arquivo via FTP ---
            with open(caminho_local, "rb") as f:
                ftp.storbinary(f"STOR {arquivo}", f)
            print(f"✅ Enviado: {arquivo}")

            # caminho relativo que vai para o banco
            caminho_banco = f"uploads/colaboradores/{arquivo}"

            # --- Atualiza/inserir no banco ---
            cursor.execute("SELECT idcolaborador FROM colaborador WHERE nome_colaborador = %s", (nome_base,))
            existe = cursor.fetchone()

            if existe:
                cursor.execute("UPDATE colaborador SET imagem = %s WHERE nome_colaborador = %s", (caminho_banco, nome_base))
                print(f"🧩 Atualizado no banco: {nome_base}")
            else:
                cursor.execute("INSERT INTO colaborador (nome_colaborador, imagem) VALUES (%s, %s)", (nome_base, caminho_banco))
                print(f"🆕 Inserido no banco: {nome_base}")

            conn.commit()

    except Exception as e:
        print("❌ Erro:", e)
    finally:
        cursor.close()
        conn.close()
        ftp.quit()

if __name__ == "__main__":
    enviar_imagens()
