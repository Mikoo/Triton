import pytest

from triton_telemetry.core import scan_all_providers

from triton_telemetry.exceptions import (
    NetworkPeeringError,
    ProviderTimeoutError,
    CorruptedPayloadError
)
@pytest.mark.asyncio
async def test_timeout_real():
    """
    Verifica que un timeout sea transformado en
    ProviderTimeoutError y agrupado por TaskGroup.
    """
    try:
        await scan_all_providers(
            providers=["AWS"],
            timeout=0.1,
            chaos=True,
        )

        pytest.fail("Se esperaba un ExceptionGroup")

    except* ProviderTimeoutError as exception_group:
        assert len(exception_group.exceptions) == 1

        error = exception_group.exceptions[0]

        assert isinstance(error, ProviderTimeoutError)
        assert error.provider == "AWS"
        
@pytest.mark.asyncio
async def test_network_error_real():
    """
    Verifica que un fallo de conexión/DNS sea transformado
    en NetworkPeeringError.
    """
    try:
        await scan_all_providers(
            providers=["AWS"],
            timeout=2.0,
            custom_urls={
                "AWS": "https://dominio-que-no-existe-triton.invalid"
            },
        )

        pytest.fail("Se esperaba un ExceptionGroup")

    except* NetworkPeeringError as exception_group:
        assert len(exception_group.exceptions) == 1

        error = exception_group.exceptions[0]

        assert isinstance(error, NetworkPeeringError)
        assert error.provider == "AWS"
        
@pytest.mark.asyncio
async def test_http_error_real():
    """
    Verifica que un error HTTP 504 sea transformado
    en CorruptedPayloadError.
    """
    try:
        await scan_all_providers(
            providers=["Azure"],
            timeout=5.0,
            chaos=True,
        )

        pytest.fail("Se esperaba un ExceptionGroup")

    except* CorruptedPayloadError as exception_group:
        assert len(exception_group.exceptions) == 1

        error = exception_group.exceptions[0]

        assert isinstance(error, CorruptedPayloadError)
        assert error.provider == "Azure"
        
@pytest.mark.asyncio
async def test_multiple_failures_concurrently():
    """
    Verifica que múltiples fallos concurrentes sean agrupados
    correctamente por asyncio.TaskGroup en un ExceptionGroup.
    """
    try:
        await scan_all_providers(
            providers=["AWS", "Azure", "GCP"],
            timeout=0.1,
            chaos=True,
        )

        pytest.fail("Se esperaba un ExceptionGroup")

    except* ProviderTimeoutError as exception_group:
        assert len(exception_group.exceptions) >= 1

    except* CorruptedPayloadError as exception_group:
        assert len(exception_group.exceptions) >= 1