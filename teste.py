import os
from ftplib import FTP
from dotenv import load_dotenv

# Função de log simples
def log_and_print(msg, level="info"):
    print(msg)

# Carrega variáveis do .env
load_dotenv()

FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")

# Arquivo de teste (pode ser qualquer arquivo pequeno, ex: teste.txt)
local_test_file = r"C:\Backburner_Job\processamento.log"  # certifique-se que existe
remote_test_path = "/www/sistema/uploads/renders/processamento.log"

def test_ftp_upload(local_path, remote_path, ftp_host, ftp_user, ftp_pass):
    try:
        ftp = FTP(ftp_host, timeout=30)
        ftp.login(ftp_user, ftp_pass)
        log_and_print(f"🌐 Conectado ao FTP: {ftp_host}")

        with open(local_path, "rb") as file:
            ftp.storbinary(f"STOR {remote_path}", file)

        ftp.quit()
        log_and_print(f"✅ Upload de teste concluído: {remote_path}")
        return True
    except Exception as e:
        log_and_print(f"❌ Erro no upload de teste FTP: {e}")
        return False

# Executa o teste
if __name__ == "__main__":
    if os.path.exists(local_test_file):
        test_ftp_upload(local_test_file, remote_test_path, FTP_HOST, FTP_USER, FTP_PASS)
    else:
        log_and_print(f"⚠ Arquivo de teste não encontrado: {local_test_file}")
