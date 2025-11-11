import os
from pathlib import Path

# Caminho onde estão os JPGs — ajuste se necessário
TARGET_FOLDER = r"C:\Users\usuario\Documents\Urban Construcode\Plantas"
# Modo dry-run: True = apenas mostra o que seria renomeado; False = executa os renames
DRY_RUN = False

replacements = {'²': ''}

conflicts = []
changes = []

p = Path(TARGET_FOLDER)
if not p.exists():
    print(f"Pasta não encontrada: {TARGET_FOLDER}")
    raise SystemExit(1)

for entry in p.iterdir():
    if entry.is_file():
        name = entry.name
        new_name = name
        for old, new in replacements.items():
            if old in new_name:
                new_name = new_name.replace(old, new)
        if new_name != name:
            src = entry
            dst = p / new_name
            if dst.exists():
                conflicts.append((name, new_name))
                print(f"⚠ Conflito: destino já existe -> {name} -> {new_name}")
            else:
                changes.append((name, new_name))
                print(f"{name} -> {new_name}")

if not changes and not conflicts:
    print("Nenhum arquivo com '²' encontrado.")
else:
    print(f"\nResumo: {len(changes)} alteração(ões) possíveis, {len(conflicts)} conflito(s).")

if not DRY_RUN and changes:
    print("\nExecutando renomeações...")
    for name, new_name in changes:
        src = p / name
        dst = p / new_name
        try:
            os.rename(src, dst)
            print(f"Renomeado: {name} -> {new_name}")
        except Exception as e:
            print(f"Erro ao renomear {name}: {e}")

if not DRY_RUN and conflicts:
    print("\nObserve que alguns arquivos não foram renomeados devido a conflitos. Revise manualmente.")

print("Concluído.")
