import os
import unicodedata
import re

# Caminho da pasta que você quer processar
pasta = r"N:\CEG_RES\mapas"

def normalizar_nome(nome):
    # Remove acentos
    nome = unicodedata.normalize('NFD', nome)
    nome = nome.encode('ascii', 'ignore').decode('utf-8')

    # Substitui espaços por underline e remove caracteres especiais
    nome = re.sub(r'[^A-Za-z0-9._-]', '_', nome)

    # Evita múltiplos underlines seguidos
    nome = re.sub(r'_+', '_', nome)

    return nome

for arquivo in os.listdir(pasta):
    caminho_antigo = os.path.join(pasta, arquivo)
    if os.path.isfile(caminho_antigo):
        novo_nome = normalizar_nome(arquivo)
        caminho_novo = os.path.join(pasta, novo_nome)
        
        if caminho_antigo != caminho_novo:
            os.rename(caminho_antigo, caminho_novo)
            print(f"Renomeado: {arquivo} -> {novo_nome}")

print("\n✅ Finalizado!")