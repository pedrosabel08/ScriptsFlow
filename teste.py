import re

def get_prefix(name: str) -> str:
    if not name:
        return ""
    
    # Captura número + ponto + duas partes com underscore
    match = re.match(r'^(\d+\.[A-Za-z0-9]+_[A-Za-z0-9]+)', name)
    if match:
        return match.group(1)
    return name



print(get_prefix("26.LD9_URB Suite do apartamento Loft 4301 A_EF"))  # 26.LD9_URB
print(get_prefix("22.LD9_URB_Living_Dup_4301B_015"))                 # 22.LD9_URB_Living_Dup_4301B_015 -> mas podemos limitar até o segundo underscore
print(get_prefix("25.LD9_URB_Living Apto 4301 A_EF"))                 # 25.LD9_URB_Living
print(get_prefix("24.LD9_URB Living Apto Tipo unidade 4402A_EF"))    # 24.LD9_URB
