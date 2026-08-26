"""
Pruebas Unitarias de Jerarquía de Excepciones Semánticas (Integrante 1).
"""

import pytest

from src.triton_telemetry.exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)


class TestTritonExceptions:
    """Verificación del diseño semántico de excepciones y Hard Gates."""

    def test_base_exception_inheritance_rule(self):
        """
        HARDENING RULE: Prohibido heredar directamente de BaseException.
        TritonError y todas sus subclases deben heredar de Exception.
        """
        assert issubclass(TritonError, Exception)
        assert TritonError.__base__ is Exception

        for sub_cls in [ProviderTimeoutError, CorruptedPayloadError, NetworkPeeringError]:
            assert issubclass(sub_cls, TritonError)
            assert issubclass(sub_cls, Exception)

    def test_exception_instantiation_and_provider_attribute(self):
        """Verifica que las excepciones almacenen y formateen el proveedor."""
        err = ProviderTimeoutError("Tiempo de espera agotado", provider="AWS")
        assert err.provider == "AWS"
        assert "[AWS] Tiempo de espera agotado" in str(err)

        err_no_provider = CorruptedPayloadError("Payload inválido")
        assert err_no_provider.provider is None
        assert str(err_no_provider) == "Payload inválido"

    def test_add_note_compatibility(self):
        """Verifica la compatibilidad con el mecanismo de notas forenses (PEP 678)."""
        err = NetworkPeeringError("Fallo de resolución DNS", provider="GCP")
        err.add_note("Nota forense 1: Servidor no responde en puerto 443")
        err.add_note("Nota forense 2: Reintento fallido tras 100ms")

        assert hasattr(err, "__notes__")
        assert len(err.__notes__) == 2
        assert "Nota forense 1" in err.__notes__[0]