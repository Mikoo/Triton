"""
Módulo de Sanitización y Validación Declarativa de Entradas CLI para Triton.

Este módulo implementa funciones callable personalizadas compatibles con argparse
para asegurar la frontera del sistema antes de iniciar el bucle de eventos asíncrono.
"""

import argparse
import re

# Expresión regular estricta para el identificador de clúster: cluster-<region>-<numero>
CLUSTER_ID_REGEX = re.compile(r"^cluster-[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*-\d+$")

MIN_TIMEOUT_SECONDS: float = 0.1
MAX_TIMEOUT_SECONDS: float = 5.0


def validate_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(
            f"El valor de timeout debe ser un número decimal válido. Se recibió: '{value}'"
        )

    if not (MIN_TIMEOUT_SECONDS <= timeout <= MAX_TIMEOUT_SECONDS):
        raise argparse.ArgumentTypeError(
            f"El timeout debe estar estrictamente acotado entre {MIN_TIMEOUT_SECONDS} y "
            f"{MAX_TIMEOUT_SECONDS} segundos. Se recibió: {timeout}"
        )

    return timeout


def validate_cluster_id(value: str) -> str:
    if not isinstance(value, str) or not CLUSTER_ID_REGEX.match(value.strip()):
        raise argparse.ArgumentTypeError(
            f"Identificador de clúster inválido: '{value}'. "
            f"Debe respetar estrictamente el formato 'cluster-<region>-<numero>' "
            f"(ej.: 'cluster-us-east-01', 'cluster-eu-west-02')."
        )

    return value.strip()
