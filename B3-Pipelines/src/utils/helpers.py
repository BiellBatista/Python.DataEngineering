"""Utilitários para validar diretórios e pontos de montagem."""

from pathlib import Path
from typing import Union


def check_directories_exist(directories: Union[str, list[str]]) -> list[str]:
    """Encontra caminhos ausentes ou indisponíveis no disco.

    Para caminhos em Windows ou sob ``/mnt``, valida primeiro a unidade ou o
    ponto de montagem correspondente.

    Args:
        directories: Um caminho ou uma lista de caminhos para validar.

    Returns:
        Caminhos que não existem ou cuja unidade/ponto de montagem está
        indisponível. Uma lista vazia indica que todos foram encontrados.
    """
    if isinstance(directories, str):
        directories = [directories]

    missing_directories = []

    for directory in directories:
        path = Path(directory)

        # 1. Validação para Windows (ex: 'D:\...')
        if path.drive:
            drive = Path(path.drive + "\\")

            if not drive.exists():
                missing_directories.append(directory)
                continue

        # 2. Validação para WSL / Linux (ex: '/mnt/d/...')
        elif len(path.parts) >= 3 and path.parts[1] == "mnt":
            mount_point = Path(f"/{path.parts[1]}/{path.parts[2]}")

            if not mount_point.exists():
                missing_directories.append(directory)
                continue

        # 3. Validação do diretório em si
        if not path.exists():
            missing_directories.append(directory)

    return missing_directories

def ensure_directory_available(directory: str) -> None:
    """Valida o ponto de montagem e cria o diretório, se necessário.

    Args:
        directory: Caminho completo do diretório a preparar.

    Raises:
        FileNotFoundError: Se a unidade ou o ponto de montagem estiver ausente.
        NotADirectoryError: Se o caminho resultante não for um diretório.
        OSError: Se o diretório não puder ser criado ou acessado.
    """
    path = Path(directory)

    # 1. Validação para Windows (ex: 'D:\...')
    if path.drive:
        drive = Path(path.drive + "\\")

        if not drive.exists():
            raise FileNotFoundError(f"Unidade indisponível: {drive}")

    # 2. Validação para WSL / Linux (ex: '/mnt/d/...')
    elif len(path.parts) >= 3 and path.parts[1] == "mnt":
        mount_point = Path(f"/{path.parts[1]}/{path.parts[2]}")

        if not mount_point.exists():
            raise FileNotFoundError(f"Ponto de montagem indisponível: {mount_point}")

    # Cria o diretório (e subpastas) caso não exista; deixa erros explícitos.
    path.mkdir(parents=True, exist_ok=True)

    if not path.is_dir():
        raise NotADirectoryError(f"O caminho não é um diretório: {path}")
