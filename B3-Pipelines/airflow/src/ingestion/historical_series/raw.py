import os

def get_raw_dir(year) -> str:
    raw_dir = f"D:/b3_datalake/raw/cotahist/ano={year}/COTAHIST_A{year}.TXT"

    return raw_dir

def list_raw_dirs(start_year, end_year) -> list[str]:
    raw_dirs = []

    for year in range(start_year, end_year + 1):
        raw_dir = get_raw_dir(year)

        if os.path.exists(raw_dir):
            raw_dirs.append(raw_dir)

    return raw_dirs