import pytest
import asyncio
import sys
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
        
@pytest.mark.asyncio
async def test_cli_concurrent_chaos():
    """
    Verifica que múltiples ejecuciones de la CLI
    puedan ejecutarse concurrentemente con --chaos.
    """

    command = [
        sys.executable,
        "src/app_operator.py",
        "AWS",
        "Azure",
        "GCP",
        "-c",
        "cluster-us-west-02",
        "-t",
        "0.1",
        "--chaos",
    ]

    async def run_cli():
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        return (
            process.returncode,
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace"),
        )

    # Ejecutamos tres instancias de la CLI simultáneamente.
    results = await asyncio.gather(
        run_cli(),
        run_cli(),
        run_cli(),
    )

    # Deben finalizar las tres ejecuciones.
    assert len(results) == 3

    for returncode, stdout, stderr in results:
        output = stdout + stderr

        # La CLI debe haber producido fallos por el timeout.
        assert "Timeout de red" in output

        # Debe existir evidencia del fallo.
        assert "FALLO" in output