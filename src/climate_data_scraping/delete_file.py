from pathlib import Path

def safe_delete(path: str) -> None:
    p = Path(path)
    if p.is_file():
        p.unlink()
    else:
        print(f"{p.name} não encontrado.")