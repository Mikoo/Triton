"""
Pruebas Unitarias y de Concurrencia para el Motor Asíncrono (Integrante 2).
"""

import httpx
import pytest

from src.triton_telemetry.core import (
    scan_all_providers,
    scan_provider,
)
from src.triton_telemetry.exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
)


@pytest.mark.asyncio
class TestCoreTelemetry:
    """Pruebas para las corrutinas de escaneo de telemetría."""

    async def test_scan_provider_nominal(self):
        """Verifica una petición nominal exitosa."""
        transport = httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"userId": 1, "id": 1, "title": "AWS Telemetry OK"}
            )
        )
        async with httpx.AsyncClient(transport=transport) as client:
            result = await scan_provider(
                client=client,
                provider="AWS",
                timeout=2.0,
                custom_url="https://fake-endpoint.test/status",
            )
            assert result["provider"] == "AWS"
            assert result["status"] == "NOMINAL"
            assert result["status_code"] == 200
            assert result["latency_ms"] >= 0

    async def test_scan_provider_timeout_with_notes(self):
        """Verifica el mapeo a ProviderTimeoutError y la inyección de add_note()."""

        def timeout_handler(request):
            raise httpx.ReadTimeout("Socket timeout simulated", request=request)

        transport = httpx.MockTransport(timeout_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(ProviderTimeoutError) as exc_info:
                await scan_provider(
                    client=client,
                    provider="AWS",
                    timeout=1.0,
                    custom_url="https://fake-timeout.test/delay",
                )

            err = exc_info.value
            assert err.provider == "AWS"
            assert issubclass(type(err), ProviderTimeoutError)
            assert isinstance(err.__cause__, httpx.TimeoutException)
            assert hasattr(err, "__notes__")
            assert any("Timeout superado" in note for note in err.__notes__)

    async def test_scan_provider_http_status_error_chaining(self):
        """Verifica el mapeo de HTTP 504 a CorruptedPayloadError con encadenamiento explícito."""
        transport = httpx.MockTransport(
            lambda request: httpx.Response(504, text="Gateway Timeout from Proxy")
        )
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(CorruptedPayloadError) as exc_info:
                await scan_provider(
                    client=client,
                    provider="Azure",
                    timeout=2.0,
                    custom_url="https://fake-error.test/status/504",
                )

            err = exc_info.value
            assert err.provider == "Azure"
            assert issubclass(type(err), CorruptedPayloadError)
            assert isinstance(err.__cause__, httpx.HTTPStatusError)
            assert err.__cause__.response.status_code == 504

    async def test_scan_provider_network_peering_error(self):
        """Verifica el mapeo de fallos de red / DNS a NetworkPeeringError."""

        def connect_error_handler(request):
            raise httpx.ConnectError("Failed to resolve host name", request=request)

        transport = httpx.MockTransport(connect_error_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(NetworkPeeringError) as exc_info:
                await scan_provider(
                    client=client,
                    provider="GCP",
                    timeout=2.0,
                    custom_url="https://unreachable-dns.test",
                )

            err = exc_info.value
            assert err.provider == "GCP"
            assert issubclass(type(err), NetworkPeeringError)
            assert isinstance(err.__cause__, httpx.ConnectError)

    async def test_scan_all_providers_task_group_parallel_success(self):
        """Verifica que scan_all_providers orqueste correctamente con TaskGroup."""

        def mock_router(request):
            return httpx.Response(
                200, json={"status": "active", "path": request.url.path}
            )

        custom_urls = {
            "AWS": "https://test.local/aws",
            "Azure": "https://test.local/azure",
            "GCP": "https://test.local/gcp",
        }

        original_async_client = httpx.AsyncClient
        try:
            transport = httpx.MockTransport(mock_router)
            httpx.AsyncClient = lambda **kwargs: original_async_client(
                transport=transport, **kwargs
            )

            results = await scan_all_providers(
                providers=["AWS", "Azure", "GCP"],
                timeout=3.0,
                custom_urls=custom_urls,
            )
            assert len(results) == 3
            assert {r["provider"] for r in results} == {"AWS", "Azure", "GCP"}
        finally:
            httpx.AsyncClient = original_async_client

    async def test_scan_all_providers_task_group_exception_group(self):
        """Verifica que TaskGroup empaquete los fallos concurrentes en un ExceptionGroup."""

        def failing_router(request):
            if "aws" in str(request.url):
                raise httpx.ReadTimeout("Timeout in AWS", request=request)
            elif "azure" in str(request.url):
                return httpx.Response(504, text="Gateway Timeout")
            return httpx.Response(200, json={"status": "ok"})

        custom_urls = {
            "AWS": "https://test.local/aws",
            "Azure": "https://test.local/azure",
            "GCP": "https://test.local/gcp",
        }

        original_async_client = httpx.AsyncClient
        try:
            transport = httpx.MockTransport(failing_router)
            httpx.AsyncClient = lambda **kwargs: original_async_client(
                transport=transport, **kwargs
            )

            with pytest.raises(ExceptionGroup) as exc_info:
                await scan_all_providers(
                    providers=["AWS", "Azure", "GCP"],
                    timeout=1.0,
                    custom_urls=custom_urls,
                )

            eg = exc_info.value
            assert len(eg.exceptions) >= 2
            timeout_excs = [
                e for e in eg.exceptions if isinstance(e, ProviderTimeoutError)
            ]
            payload_excs = [
                e for e in eg.exceptions if isinstance(e, CorruptedPayloadError)
            ]
            assert len(timeout_excs) == 1
            assert len(payload_excs) == 1
        finally:
            httpx.AsyncClient = original_async_client
