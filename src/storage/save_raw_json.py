import json
from pathlib import Path
from datetime import datetime


def save_raw_json(data: dict | list, base_folder: str, prefix: str) -> str:
    folder = Path(base_folder)
    folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = folder / f"{prefix}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return str(file_path)