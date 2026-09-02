"""
Triton Telemetry — Paquete Público del Monitor de Resiliencia Asíncrona.

Este módulo centraliza la exportación de la API pública de Triton (Integrante 5).
El CLI (app_operator.py) y la suite de pruebas consumen únicamente los símbolos
declarados en __all__, garantizando una superficie de contrato estable y legible.
"""

from .core import (
    CHAOS_ENDPOINTS,
    NOMINAL_ENDPOINTS,
    scan_all_providers,
    scan_provider,
)
from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)
from .logging_engine import (
    AsyncJSONFormatter,
    NonBlockingQueueHandler,
    setup_logging,
    shutdown_logging,
)
from .sanitizer import (
    MAX_TIMEOUT_SECONDS,
    MIN_TIMEOUT_SECONDS,
    validate_cluster_id,
    validate_timeout,
)

__all__ = [
    # Excepciones semánticas de dominio (Integrante 1)
    "TritonError",
    "ProviderTimeoutError",
    "CorruptedPayloadError",
    "NetworkPeeringError",
    # Telemetría HTTP asíncrona (Integrante 2)
    "scan_provider",
    "scan_all_providers",
    "NOMINAL_ENDPOINTS",
    "CHAOS_ENDPOINTS",
    # Formateo JSON y pipeline no bloqueante (Integrantes 3 y 4)
    "AsyncJSONFormatter",
    "NonBlockingQueueHandler",
    "setup_logging",
    "shutdown_logging",
    # Validadores declarativos de la frontera CLI (Integrante 1)
    "validate_timeout",
    "validate_cluster_id",
    "MIN_TIMEOUT_SECONDS",
    "MAX_TIMEOUT_SECONDS",
]
