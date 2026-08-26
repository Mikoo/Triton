"""Telemetría HTTP asíncrona para los proveedores de Triton.

El ``ExceptionGroup`` producido por ``TaskGroup`` se deja escapar a propósito:
el punto de entrada CLI debe procesarlo selectivamente mediante ``except*``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)


PROVIDER_ENDPOINTS: dict[str, str] = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "AZURE": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3",
}


def _forensic_note(provider: str, cluster: str, endpoint: str) -> str:
    return f"provider={provider}; cluster={cluster}; endpoint={endpoint}"


def _domain_error(
    error_type: type[TritonError],
    message: str,
    *,
    provider: str,
    cluster: str,
    endpoint: str,
) -> TritonError:
    error = error_type(message, provider=provider)
    error.add_note(_forensic_note(provider, cluster, endpoint))
    return error


async def fetch_provider_telemetry(
    provider: str,
    cluster: str,
    timeout: float,
    *,
    client: httpx.AsyncClient,
    endpoint: str | None = None,
) -> dict[str, Any]:
    """Consulta un proveedor y traduce los errores HTTP al dominio Triton."""

    normalized_provider = provider.upper()
    target = endpoint or PROVIDER_ENDPOINTS.get(normalized_provider)
    if target is None:
        raise ValueError(f"Proveedor no soportado: {provider}")

    note = _forensic_note(normalized_provider, cluster, target)
    try:
        response = await client.get(
            target,
            params={"cluster": cluster, "provider": normalized_provider},
            timeout=timeout,
        )
        response.raise_for_status()
    except httpx.TimeoutException as cause:
        error = ProviderTimeoutError(
            f"La consulta superó el límite de {timeout:.2f} segundos",
            provider=normalized_provider,
        )
        error.add_note(note)
        raise error from cause
    except (httpx.RequestError, httpx.HTTPStatusError) as cause:
        error = NetworkPeeringError(
            f"No fue posible obtener telemetría: {cause}",
            provider=normalized_provider,
        )
        error.add_note(note)
        raise error from cause

    try:
        payload = response.json()
    except ValueError as cause:
        raise _domain_error(
            CorruptedPayloadError,
            "El proveedor devolvió un cuerpo que no es JSON válido",
            provider=normalized_provider,
            cluster=cluster,
            endpoint=target,
        ) from cause

    if not isinstance(payload, Mapping):
        raise _domain_error(
            CorruptedPayloadError,
            "La telemetría debe ser un objeto JSON",
            provider=normalized_provider,
            cluster=cluster,
            endpoint=target,
        )

    return {
        "provider": normalized_provider,
        "cluster": cluster,
        "status": "operational",
        "telemetry": dict(payload),
    }


async def scan_all_providers(
    providers: Sequence[str],
    cluster: str,
    timeout: float,
    *,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """Escanea proveedores concurrentemente y conserva el orden solicitado."""

    normalized = [provider.upper() for provider in providers]
    unsupported = [name for name in normalized if name not in PROVIDER_ENDPOINTS]
    if unsupported:
        raise ValueError(f"Proveedores no soportados: {', '.join(unsupported)}")

    async def run(active_client: httpx.AsyncClient) -> list[dict[str, Any]]:
        tasks: list[asyncio.Task[dict[str, Any]]] = []
        async with asyncio.TaskGroup() as group:
            for provider in normalized:
                tasks.append(
                    group.create_task(
                        fetch_provider_telemetry(
                            provider, cluster, timeout, client=active_client
                        ),
                        name=f"telemetry-{provider}",
                    )
                )
        return [task.result() for task in tasks]

    if client is not None:
        return await run(client)

    async with httpx.AsyncClient() as active_client:
        return await run(active_client)


__all__ = ["PROVIDER_ENDPOINTS", "fetch_provider_telemetry", "scan_all_providers"]
