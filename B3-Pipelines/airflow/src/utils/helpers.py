from pathlib import Path
from typing import Union

def check_directories_exist(directories: Union[str, list[str]]) -> list[str]:
    """
    Verifica se uma ou mais unidades/pontos de montagem estão disponíveis
    E se os diretórios especificados realmente existem no disco.

    Parâmetros
    ----------
    directories : Union[str, list[str]]
        Caminho(s) completo(s) do(s) diretório(s).

    Retorna
    -------
    list[str]
        Lista com os diretórios inexistentes. 
        Retorna uma lista vazia [] se todos existirem e forem acessíveis.
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
    """
    Verifica se a unidade/ponto de montagem está disponível
    e garante que o diretório exista (criando-o se necessário).

    Parâmetros
    ----------
    directory : str
        Caminho completo do diretório.

    Retorna
    -------
    None
        Retorna normalmente se o diretório estiver disponível. Erros de
        montagem, permissão ou sistema são propagados ao chamador.
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
