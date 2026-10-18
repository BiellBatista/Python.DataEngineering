"""Localização e validação dos arquivos RAW históricos da B3."""

from pathlib import Path
from typing import Union

from config import RAW_COTAHIST_DIR, RAW_FILENAME_TEMPLATE, RAW_YEAR_DIRECTORY_TEMPLATE
from common.exceptions import RawError


def get_raw_dir(year: int, base_dir: Union[str, Path] = RAW_COTAHIST_DIR) -> str:
    """Monta o caminho do arquivo COTAHIST para um ano.

    Args:
        year: Ano dos dados históricos.
        base_dir: Diretório raiz da camada RAW.

    Returns:
        Caminho completo do arquivo de entrada.
    """
    return str(
        Path(base_dir)
        / RAW_YEAR_DIRECTORY_TEMPLATE.format(year=year)
        / RAW_FILENAME_TEMPLATE.format(year=year)
    )


def list_raw_dirs(
    start_year: int,
    end_year: int,
    base_dir: Union[str, Path] = RAW_COTAHIST_DIR,
) -> list[str]:
    """Retorna os arquivos RAW existentes para o intervalo inclusivo.

    Args:
        start_year: Primeiro ano a localizar.
        end_year: Último ano a localizar.
        base_dir: Diretório raiz da camada RAW.

    Returns:
        Caminhos dos arquivos RAW, em ordem crescente de ano.

    Raises:
        ValueError: Se ``start_year`` for maior que ``end_year``.
        RawError: Se faltar algum arquivo do intervalo solicitado.
    """
    if start_year > end_year:
        raise ValueError("start_year deve ser menor ou igual a end_year.")

    raw_dirs = [
        get_raw_dir(year, base_dir)
        for year in range(start_year, end_year + 1)
    ]
    missing_files = [raw_dir for raw_dir in raw_dirs if not Path(raw_dir).is_file()]

    if missing_files:
        raise RawError(
            "Arquivos RAW ausentes para o intervalo solicitado: "
            + ", ".join(missing_files)
        )

    return raw_dirs