"""Utilitários para adicionar contexto de parâmetros aos logs."""

import inspect
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import structlog

P = ParamSpec("P")
R = TypeVar("R")


def log_parameters(function: Callable[P, R]) -> Callable[P, R]:
    """Vincula os argumentos da chamada ao contexto dos logs durante a execução.

    Os argumentos, incluindo valores padrão, ficam disponíveis para os
    processadores structlog enquanto a função estiver ativa e são restaurados
    após seu retorno ou exceção.
    """
    signature = inspect.signature(function)

    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        with structlog.contextvars.bound_contextvars(**bound_arguments.arguments):
            return function(*args, **kwargs)

    return wrapper
