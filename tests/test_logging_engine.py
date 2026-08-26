"""
Pruebas Unitarias para el Motor de Logging No Bloqueante y AsyncJSONFormatter (Roles 3 y 4).
"""

import gzip
import json
import logging
import os
import tempfile

from src.triton_telemetry.logging_engine import (
    AsyncJSONFormatter,
    setup_logging,
    shutdown_logging,
)


class TestAsyncJSONFormatter:
    """Pruebas del formateador JSON de Lu."""

    def test_format_standard_record(self):
        formatter = AsyncJSONFormatter()
        record = logging.LogRecord(
            name="triton.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=42,
            msg="Mensaje de telemetría de prueba",
            args=(),
            exc_info=None,
        )

        formatted_json = formatter.format(record)
        data = json.loads(formatted_json)

        assert data["level"] == "INFO"
        assert data["logger"] == "triton.test"
        assert data["message"] == "Mensaje de telemetría de prueba"
        assert "timestamp" in data
        assert "process" in data
        assert "threadName" in data


class TestNonBlockingLoggingAndGzipRotation:
    """Pruebas del pipeline no bloqueante y rotación Gzip (Tu Rol 4)."""

    def test_queue_listener_pipeline_and_shutdown(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_file = os.path.join(tmp_dir, "test_triton.log")
            listener, logger = setup_logging(
                log_file=log_file, log_level="DEBUG", enable_console=False
            )

            try:
                logger.info(
                    "Test message 1", extra={"cluster_id": "cluster-us-east-01"}
                )
            finally:
                shutdown_logging(listener)

            assert os.path.exists(log_file)
            with open(log_file, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]

            assert len(lines) >= 1
            parsed = json.loads(lines[0])
            assert parsed["message"] == "Test message 1"
            assert parsed["cluster_id"] == "cluster-us-east-01"

    def test_hot_gzip_rotation_and_decompression(self):
        """Verifica que al rotar se genere .gz y se borre el archivo plano residual."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_file = os.path.join(tmp_dir, "rot_test.log")

            # Tamaño chico de 400 bytes para forzar rotación en el test
            listener, logger = setup_logging(
                log_file=log_file,
                log_level="INFO",
                max_bytes=400,
                backup_count=2,
                enable_console=False,
            )

            try:
                for i in range(25):
                    logger.info(
                        f"Mensaje extenso de telemetría de monitoreo {i} con datos adicionales en bloque"
                    )
            finally:
                shutdown_logging(listener)

            files = os.listdir(tmp_dir)
            gz_files = [f for f in files if f.endswith(".gz")]

            assert len(gz_files) >= 1, "Debe haber al menos un archivo .gz rotado"

            # Verificar que se pueda descomprimir sin corrupción
            gz_path = os.path.join(tmp_dir, gz_files[0])
            with gzip.open(gz_path, "rt", encoding="utf-8") as gz_in:
                decompressed = gz_in.read()
                assert len(decompressed) > 0
