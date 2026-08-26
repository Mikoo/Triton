import asyncio
import unittest

import httpx

from src.triton_telemetry.core import scan_all_providers
from src.triton_telemetry.exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
)


class CoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_scan_returns_results_in_requested_order(self):
        async def handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0)
            return httpx.Response(200, json={"id": request.url.path})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            results = await scan_all_providers(
                ["GCP", "AWS"], "cluster-us-east-01", 1.0, client=client
            )

        self.assertEqual([item["provider"] for item in results], ["GCP", "AWS"])
        self.assertTrue(all(item["status"] == "operational" for item in results))

    async def test_timeout_is_mapped_and_contains_forensic_note(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("simulated timeout", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(ExceptionGroup) as captured:
                await scan_all_providers(
                    ["AWS"], "cluster-us-east-01", 0.2, client=client
                )

        error = captured.exception.exceptions[0]
        self.assertIsInstance(error, ProviderTimeoutError)
        self.assertIn("provider=AWS", error.__notes__[0])
        self.assertIn("cluster=cluster-us-east-01", error.__notes__[0])

    async def test_network_error_is_mapped(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("peering down", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(ExceptionGroup) as captured:
                await scan_all_providers(
                    ["AZURE"], "cluster-us-east-01", 1.0, client=client
                )

        self.assertIsInstance(captured.exception.exceptions[0], NetworkPeeringError)

    async def test_invalid_json_is_mapped(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"not-json")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaises(ExceptionGroup) as captured:
                await scan_all_providers(
                    ["GCP"], "cluster-us-east-01", 1.0, client=client
                )

        self.assertIsInstance(captured.exception.exceptions[0], CorruptedPayloadError)

    async def test_unsupported_provider_fails_before_starting_tasks(self):
        with self.assertRaisesRegex(ValueError, "OPENSTACK"):
            await scan_all_providers(["OPENSTACK"], "cluster-us-east-01", 1.0)
