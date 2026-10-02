from pathlib import Path


def ensure_directory_available(directory: str) -> bool:
    """
    Verifica se a unidade do caminho está disponível e garante
    que o diretório exista.

    Parameters
    ----------
    directory : str
        Caminho completo do diretório.

    Returns
    -------
    bool
        True  -> unidade disponível e diretório existente/criado.
        False -> unidade indisponível ou ocorreu alguma exceção.
    """
    try:
        path = Path(directory)

        # Verifica se o caminho possui uma unidade/drive.
        if not path.drive:
            return False

        # Verifica se a unidade está disponível.
        drive = Path(path.drive + "\\")
        if not drive.exists():
            return False

        # Cria o diretório caso ele ainda não exista.
        path.mkdir(parents=True, exist_ok=True)

        return True

    except Exception:
        return False