import os
import re

pasta = r"C:\Users\usuario\Documents\Urban Construcode\Plantas"

def formatar_nome(campo):
    campo = campo.strip()
    # Remover o caractere superscrito '²' se presente
    campo = campo.replace('²', '')
    # Abreviações personalizadas
    # Substitui por 'PH_' para garantir underscore após PH
    campo = campo.replace("Planta humanizada do apartamento", "PH_")
    campo = campo.replace("Planta humanizada do pavimento", "PH_")
    campo = campo.replace("Planta humanizada", "PH_")
    # Substitui ' - ' por underscore e remove espaços
    campo = campo.replace(" - ", "_")
    campo = campo.replace(" ", "")  # remove espaços
    # Normaliza múltiplos underscores para apenas um e remove underscores nas bordas
    campo = re.sub(r"_+", "_", campo)
    campo = campo.strip("_")
    return campo

for arquivo in os.listdir(pasta):
    if arquivo.lower().endswith((".jpg", ".jpeg")):
        # Regex para pegar tudo que vem depois de XX.LD9_URB
        match = re.search(r"\d+\.LD9_URB\s*(.*)\.jpe?g", arquivo, re.IGNORECASE)
        if match:
            campo_personalizado = match.group(1)
            campo_formatado = formatar_nome(campo_personalizado)
            novo_nome = f"URB-IMG-F01-IMG-{campo_formatado}-XXX.jpg"
            
            caminho_antigo = os.path.join(pasta, arquivo)
            caminho_novo = os.path.join(pasta, novo_nome)
            
            print(f"Renomeando:\n{arquivo}\n--> {novo_nome}\n")
            os.rename(caminho_antigo, caminho_novo)
