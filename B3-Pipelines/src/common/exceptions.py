"""Exceções de domínio compartilhadas pelas camadas do pipeline."""


class PipelineError(Exception):
    """Classe base para falhas conhecidas do pipeline de dados."""


class RawError(FileNotFoundError, PipelineError):
    """Indica que os arquivos de entrada RAW esperados não estão disponíveis."""


class BronzeError(FileNotFoundError, PipelineError):
    """Indica que os dados ou arquivos esperados da camada Bronze não existem."""


class SilverError(NotADirectoryError, PipelineError):
    """Indica um destino inválido ou uma falha de estrutura na camada Silver."""


class GoldError(PipelineError):
    """Indica uma falha específica da camada Gold."""


class DataQualityError(PipelineError, ValueError):
    """Indica que os dados violaram uma regra de qualidade."""
