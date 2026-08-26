"""
Módulo de Excepciones Semánticas de Dominio para Triton Cloud Services.
Este módulo define la jerarquía de excepciones personalizadas para el monitor
de telemetría multicloud.
Reglas de Hardening:
- Todas las excepciones heredan de Exception (nunca de BaseException) para evitar
  la captura accidental de señales críticas del sistema operativo (e.g., KeyboardInterrupt, SystemExit).
"""

from typing import Optional

class TritonError(Exception):
    def __init__(self, message: str, provider: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.provider = provider

    def __str__(self) -> str:
        if self.provider:
            return f"[{self.provider}] {self.message}"
        return self.message

class ProviderTimeoutError(TritonError):
    pass

class CorruptedPayloadError(TritonError):
    pass

class NetworkPeeringError(TritonError):
    pass
