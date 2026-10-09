from pathlib import Path
from typing import Union

RAW_DIR = Path("/mnt/d/b3_datalake/raw/cotahist")

def get_raw_dir(year: int, base_dir: Union[str, Path] = RAW_DIR) -> str:
    return str(Path(base_dir) / f"ano={year}" / f"COTAHIST_A{year}.TXT")

def list_raw_dirs(
    start_year: int,
    end_year: int,
    base_dir: Union[str, Path] = RAW_DIR,
) -> list[str]:
    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")

    raw_dirs = [
        get_raw_dir(year, base_dir)
        for year in range(start_year, end_year + 1)
    ]
    missing_files = [raw_dir for raw_dir in raw_dirs if not Path(raw_dir).is_file()]

    if missing_files:
        raise FileNotFoundError(
            "Arquivos RAW ausentes para o intervalo solicitado: "
            + ", ".join(missing_files)
        )

    return raw_dirs