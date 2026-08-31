"""
Módulo de Concurrencia y Telemetría Asíncrona de Triton Cloud Services.

"""

import asyncio
import time
from typing import Any

import httpx

from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
)

# Mapeo de proveedores a los endpoints nominales reales en internet
NOMINAL_ENDPOINTS: dict[str, str] = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "Azure": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3",
}

# Mapeo de proveedores a los endpoints de inyección de caos y fallos reales
CHAOS_ENDPOINTS: dict[str, str] = {
    # AWS colapsa por latencia excesiva (3 segundos de retardo en httpbin)
    "AWS": "https://httpbin.org/delay/3",
    # Azure responde con código HTTP 504 (Gateway Timeout)
    "Azure": "https://httpbin.org/status/504",
    # GCP responde con código HTTP 422 (Unprocessable Entity)
    "GCP": "https://httpbin.org/status/422",
}


async def scan_provider(
    client: httpx.AsyncClient,
    provider: str,
    timeout: float,
    chaos: bool = False,
    custom_url: str | None = None,
) -> dict[str, Any]:
    """
    Realiza una consulta de telemetría HTTP asíncrona hacia un nodo cloud específico.

    """
    if custom_url:
        target_endpoint_url = custom_url
    elif chaos:
        target_endpoint_url = CHAOS_ENDPOINTS.get(
            provider, NOMINAL_ENDPOINTS.get(provider, "")
        )
    else:
        target_endpoint_url = NOMINAL_ENDPOINTS.get(provider, "")

    request_start_timestamp = time.perf_counter()

    try:
        telemetry_response = await client.get(
            target_endpoint_url,
            timeout=timeout,
        )
        measured_latency_ms = (time.perf_counter() - request_start_timestamp) * 1000

        # Dispara httpx.HTTPStatusError si el estatus es 4xx o 5xx
        telemetry_response.raise_for_status()

        try:
            parsed_payload = telemetry_response.json()
        except Exception as json_decode_error:
            raise CorruptedPayloadError(
                f"El contenido recibido desde {provider} no es un formato JSON válido",
                provider=provider,
            ) from json_decode_error

        return {
            "provider": provider,
            "status": "NOMINAL",
            "latency_ms": round(measured_latency_ms, 2),
            "status_code": telemetry_response.status_code,
            "endpoint": target_endpoint_url,
            "payload": parsed_payload,
        }

    except httpx.TimeoutException as native_timeout_error:
        timeout_failure = ProviderTimeoutError(
            f"Timeout de red ({timeout}s) superado al consultar el nodo de {provider} en {target_endpoint_url}",
            provider=provider,
        )
        timeout_failure.add_note(
            f"Timeout superado en el nodo de telemetría de respaldo ({provider})"
        )
        timeout_failure.add_note(
            f"Endpoint consultado: {target_endpoint_url} | Latencia umbral: {timeout}s"
        )
        raise timeout_failure from native_timeout_error

    except httpx.HTTPStatusError as native_http_error:
        http_status_failure = CorruptedPayloadError(
            f"Estatus HTTP no esperado recibido: {native_http_error.response.status_code} "
            f"({native_http_error.response.reason_phrase}) para el proveedor {provider}",
            provider=provider,
        )
        http_status_failure.add_note(
            f"HTTP Status Error en {provider}: {native_http_error.response.status_code}"
        )
        http_status_failure.add_note(
            f"URL de respuesta: {native_http_error.request.url}"
        )
        raise http_status_failure from native_http_error

    except (
        httpx.ConnectError,
        httpx.NetworkError,
        httpx.RequestError,
    ) as native_network_error:
        network_peering_failure = NetworkPeeringError(
            f"Fallo crítico de resolución de peering o conexión DNS para {provider}: {native_network_error}",
            provider=provider,
        )
        network_peering_failure.add_note(
            f"Fallo de la conectividad hacia el nodo de telemetría {target_endpoint_url}"
        )
        raise network_peering_failure from native_network_error


async def scan_all_providers(
    providers: list[str],
    timeout: float,
    chaos: bool = False,
    custom_urls: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Orquesta la consulta concurrente de múltiples proveedores cloud utilizando
    un bloque asyncio.TaskGroup es la Concurrencia Estructurada.

    Si una o más corrutinas fallan concurrentemente, asyncio.TaskGroup empaqueta
    todas las excepciones en un ExceptionGroup nativo y lo propaga limpiamente.

    """
    collected_telemetry_results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=timeout) as async_http_client:
        async with asyncio.TaskGroup() as telemetry_task_group:
            active_telemetry_tasks = []
            for current_provider in providers:
                specific_url = (
                    custom_urls.get(current_provider) if custom_urls else None
                )
                scheduled_task = telemetry_task_group.create_task(
                    scan_provider(
                        client=async_http_client,
                        provider=current_provider,
                        timeout=timeout,
                        chaos=chaos,
                        custom_url=specific_url,
                    ),
                    name=f"TelemetryTask-{current_provider}",
                )
                active_telemetry_tasks.append(scheduled_task)

        # Al salir del TaskGroup sin excepciones, recolectamos los resultados
        for completed_task in active_telemetry_tasks:
            collected_telemetry_results.append(completed_task.result())

    return collected_telemetry_results
