"""
Pruebas del Coordinador de Integración y Flujo CLI (Integrante 5).

Cubren el contrato completo del punto 5:
- Frontera declarativa de argparse (validadores del Integrante 1 integrados).
- choices de modos operativos (nominal / debug / emergency).
- Grupos mutuamente excluyentes de salida de texto (--tabla / --json).
- Configuración declarativa de logging con dictConfig.
- Captura quirúrgica except* sobre ExceptionGroup y notas forenses en consola.
- Cumplimiento PEP 765 a nivel de AST (finally sin return/break/continue).
- API pública del paquete (__init__ + __all__).
"""

import argparse
import ast
import json
import logging
import sys
from pathlib import Path

import pytest

from src import app_operator
from src.triton_telemetry import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)
from src import triton_telemetry


# ---------------------------------------------------------------------------
# Helpers de fakes (sin red, sin I/O real)
# ---------------------------------------------------------------------------

def fake_nominal_scan():
    """Corrutina fake que simula 3 respuestas nominales."""

    async def _fake_scan(providers, timeout, chaos=False, custom_urls=None):
        return [
            {
                "provider": provider,
                "status": "NOMINAL",
                "latency_ms": 42.5,
                "status_code": 200,
                "endpoint": f"https://fake.local/{provider.lower()}",
                "payload": {"id": 1, "title": f"{provider} telemetry ok"},
            }
            for provider in providers
        ]

    return _fake_scan


def fake_failing_scan(*errors):
    """Corrutina fake que propaga un ExceptionGroup con los errores dados."""

    async def _fake_scan(providers, timeout, chaos=False, custom_urls=None):
        raise ExceptionGroup("Simulación concurrente de fallos", list(errors))

    return _fake_scan


def with_forensic_notes(error, *notes):
    """Añade notas forenses (PEP 678) a un error, como hace core.py."""
    for note in notes:
        error.add_note(note)
    return error


# ---------------------------------------------------------------------------
# Frontera CLI: build_parser
# ---------------------------------------------------------------------------

class TestBuildParser:
    """Contrato declarativo de la frontera CLI."""

    def test_parser_acepta_argumentos_validos(self):
        parsed = app_operator.build_parser().parse_args(
            ["AWS", "GCP", "-c", "cluster-us-east-01", "-t", "3.0"]
        )
        assert parsed.providers == ["AWS", "GCP"]
        assert parsed.cluster == "cluster-us-east-01"
        assert parsed.timeout == 3.0

    def test_parser_rechaza_proveedor_fuera_de_choices(self):
        with pytest.raises(SystemExit) as excinfo:
            app_operator.build_parser().parse_args(["ORACLE", "-c", "cluster-us-east-01"])
        assert excinfo.value.code == 2

    def test_parser_cluster_invalido_sale_con_codigo_2(self):
        # Escenario B: argparse aborta con 2 sin iniciar asyncio
        with pytest.raises(SystemExit) as excinfo:
            app_operator.build_parser().parse_args(
                ["AWS", "-c", "cluster-invalido-id"]
            )
        assert excinfo.value.code == 2

    def test_parser_timeout_fuera_de_rango_sale_con_codigo_2(self):
        with pytest.raises(SystemExit) as excinfo:
            app_operator.build_parser().parse_args(
                ["AWS", "-c", "cluster-us-east-01", "-t", "9.5"]
            )
        assert excinfo.value.code == 2

    def test_parser_cluster_es_obligatorio(self):
        with pytest.raises(SystemExit) as excinfo:
            app_operator.build_parser().parse_args(["AWS"])
        assert excinfo.value.code == 2

    def test_modos_operativos_choices(self):
        parser = app_operator.build_parser()
        mode_action = next(
            a for a in parser._actions if a.dest == "mode"
        )
        assert set(mode_action.choices) == {"nominal", "debug", "emergency"}
        # default obligatorio
        parsed = parser.parse_args(["AWS", "-c", "cluster-us-east-01"])
        assert parsed.mode == "nominal"

    def test_modo_invalido_sale_con_codigo_2(self):
        with pytest.raises(SystemExit) as excinfo:
            app_operator.build_parser().parse_args(
                ["AWS", "-c", "cluster-us-east-01", "-m", "apocalipsis"]
            )
        assert excinfo.value.code == 2

    def test_salidas_de_texto_mutuamente_excluyentes(self):
        parser = app_operator.build_parser()
        groups = [g for g in parser._mutually_exclusive_groups]
        assert len(groups) == 1
        dests = {a.dest for a in groups[0]._group_actions}
        assert dests == {"output_format"}

    def test_tabla_y_json_juntos_son_error(self):
        with pytest.raises(SystemExit) as excinfo:
            app_operator.build_parser().parse_args(
                [
                    "AWS",
                    "-c", "cluster-us-east-01",
                    "--tabla",
                    "--json",
                ]
            )
        assert excinfo.value.code == 2

    def test_output_format_default_tabla(self):
        parsed = app_operator.build_parser().parse_args(
            ["AWS", "-c", "cluster-us-east-01"]
        )
        assert parsed.output_format == "tabla"

    def test_json_seleccionable(self):
        parsed = app_operator.build_parser().parse_args(
            ["AWS", "-c", "cluster-us-east-01", "--json"]
        )
        assert parsed.output_format == "json"

    def test_codigos_de_exit_del_contrato(self):
        assert app_operator.EXIT_OK == 0
        assert app_operator.EXIT_FAILURE == 1
        assert app_operator.EXIT_BAD_ARGS == 2


# ---------------------------------------------------------------------------
# Interpretación de modos operativos
# ---------------------------------------------------------------------------

class TestModosOperativos:
    """Semántica de nominal / debug / emergency."""

    def test_modo_nominal_sin_caos_nivel_info(self):
        settings = app_operator.resolve_mode_settings(
            mode="nominal", explicit_log_level=None
        )
        assert settings["chaos"] is False
        assert settings["log_level"] == "INFO"

    def test_modo_debug_nivel_debug_sin_caos(self):
        settings = app_operator.resolve_mode_settings(
            mode="debug", explicit_log_level=None
        )
        assert settings["log_level"] == "DEBUG"
        assert settings["chaos"] is False

    def test_modo_emergency_activa_caos(self):
        settings = app_operator.resolve_mode_settings(
            mode="emergency", explicit_log_level=None
        )
        assert settings["chaos"] is True

    def test_log_level_explicito_gana_sobre_modo(self):
        settings = app_operator.resolve_mode_settings(
            mode="debug", explicit_log_level="INFO"
        )
        assert settings["log_level"] == "INFO"


# ---------------------------------------------------------------------------
# Configuración declarativa de logging con dictConfig
# ---------------------------------------------------------------------------

class TestDictConfig:
    """Esquema declarativo de logging inyectado por dictConfig."""

    def test_esquema_tiene_version_1_y_handlers_esperados(self, tmp_path):
        schema = app_operator.build_logging_config(
            log_file=str(tmp_path / "forense.log"),
            log_level="INFO",
        )
        assert schema["version"] == 1
        assert "formatters" in schema
        assert "handlers" in schema
        for handler_name in ("archivo_forense", "consola", "cola_memoria"):
            assert handler_name in schema["handlers"]

    def test_esquema_desactiva_loggers_existentes_false(self, tmp_path):
        schema = app_operator.build_logging_config(
            log_file=str(tmp_path / "forense.log"), log_level="INFO"
        )
        assert schema["disable_existing_loggers"] is False

    def test_formateador_forense_es_json(self, tmp_path):
        schema = app_operator.build_logging_config(
            log_file=str(tmp_path / "forense.log"), log_level="INFO"
        )
        formatter = schema["formatters"]["forense_json"]
        assert isinstance(formatter, dict)

    def test_setup_declarativo_crea_listener_y_logger(self, tmp_path):
        listener, logger = app_operator.setup_declarative_logging(
            log_file=str(tmp_path / "forense.log"), log_level="INFO"
        )
        try:
            assert listener is not None
            assert logger.name == "triton"
            assert len(logger.handlers) == 1  # único: el QueueHandler
            from logging.handlers import QueueHandler

            assert isinstance(logger.handlers[0], QueueHandler)
        finally:
            app_operator.shutdown_logging(listener)

    def test_esquema_dicconfig_produce_log_json_valido(self, tmp_path, monkeypatch):
        """Green path: el log forense es JSON parseable con timestamp ISO UTC."""
        log_path = tmp_path / "forense.log"

        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_nominal_scan()
        )
        exit_code = app_operator.main(
            [
                "AWS",
                "GCP",
                "-c", "cluster-us-east-01",
                "-t", "3.0",
                "-o", str(log_path),
            ]
        )
        assert exit_code == 0

        lines = [l.strip() for l in log_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(lines) >= 3
        for line in lines:
            payload = json.loads(line)
            assert payload["level"] in ("INFO", "ERROR")
            assert "timestamp" in payload
            assert payload["timestamp"].endswith("Z")


# ---------------------------------------------------------------------------
# Escenarios funcionales (A, B, C)
# ---------------------------------------------------------------------------

class TestEscenariosCI:

    def test_escenario_a_nominal_exit_0(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_nominal_scan()
        )
        exit_code = app_operator.main(
            [
                "AWS",
                "GCP",
                "-c", "cluster-us-east-01",
                "-t", "3.0",
                "-o", str(tmp_path / "a.log"),
            ]
        )
        assert exit_code == 0
        out = capsys.readouterr().out
        assert "NOMINAL" in out
        assert "AWS" in out

    def test_escenario_a_json_report_valido(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_nominal_scan()
        )
        exit_code = app_operator.main(
            [
                "AWS",
                "-c", "cluster-us-east-01",
                "--json",
                "-o", str(tmp_path / "a.json.log"),
            ]
        )
        assert exit_code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["results"][0]["provider"] == "AWS"

    def test_escenario_b_entrada_invalida_exit_2(self, monkeypatch, capsys):
        # No debe llamarse NUNCA a scan_all_providers
        llamado = {"n": 0}

        async def _booby(providers, timeout, chaos=False, custom_urls=None):
            llamado["n"] += 1
            raise AssertionError("No se debe invocar asyncio en Escenario B")

        monkeypatch.setattr(app_operator, "scan_all_providers", _booby)
        with pytest.raises(SystemExit) as excinfo:
            app_operator.main(
                ["AWS", "-c", "cluster-invalido-id", "-t", "9.5"]
            )
        assert excinfo.value.code == 2
        assert llamado["n"] == 0

    def test_escenario_c_timeout_exit_1_con_notas_forenses(
        self, tmp_path, monkeypatch, capsys
    ):
        errors = [
            with_forensic_notes(
                ProviderTimeoutError("Timeout de red (3.0s) AWS", provider="AWS"),
                "Timeout superado en el nodo de telemetría de respaldo (AWS)",
                "Endpoint consultado: https://fake.local/aws",
            )
        ]
        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_failing_scan(*errors)
        )
        exit_code = app_operator.main(
            [
                "AWS",
                "-c", "cluster-us-east-01",
                "-t", "3.0",
                "-o", str(tmp_path / "c.log"),
            ]
        )
        assert exit_code == 1
        err_out = capsys.readouterr().err
        assert "Timeout de red" in err_out
        assert "nota forense" in err_out.lower() or "Timeout superado" in err_out

    def test_escenario_c_http_5xx_exit_1(self, tmp_path, monkeypatch, capsys):
        errors = [
            CorruptedPayloadError(
                "Estatus HTTP no esperado: 504 para Azure", provider="Azure"
            )
        ]
        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_failing_scan(*errors)
        )
        exit_code = app_operator.main(
            [
                "Azure",
                "-c", "cluster-us-east-01",
                "-o", str(tmp_path / "c2.log"),
            ]
        )
        assert exit_code == 1
        assert "FALLO" in capsys.readouterr().err

    def test_escenario_c_network_peering_exit_1(self, tmp_path, monkeypatch, capsys):
        errors = [
            NetworkPeeringError(
                "Fallo de resolución DNS para GCP", provider="GCP"
            )
        ]
        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_failing_scan(*errors)
        )
        exit_code = app_operator.main(
            [
                "GCP",
                "-c", "cluster-us-east-01",
                "-o", str(tmp_path / "c3.log"),
            ]
        )
        assert exit_code == 1

    def test_escenario_c_fallas_mezcladas_se_capturan_todas(
        self, tmp_path, monkeypatch, capsys
    ):
        errors = [
            ProviderTimeoutError("timeout AWS", provider="AWS"),
            CorruptedPayloadError("504 Azure", provider="Azure"),
            NetworkPeeringError("DNS GCP", provider="GCP"),
        ]
        monkeypatch.setattr(
            app_operator, "scan_all_providers", fake_failing_scan(*errors)
        )
        exit_code = app_operator.main(
            [
                "AWS",
                "Azure",
                "GCP",
                "-c", "cluster-us-west-02",
                "--chaos",
                "-o", str(tmp_path / "c4.log"),
            ]
        )
        assert exit_code == 1
        err_out = capsys.readouterr().err
        for marca in ("AWS", "Azure", "GCP"):
            assert marca in err_out

    def test_emergency_mode_equivalente_a_chaos(
        self, tmp_path, monkeypatch, capsys
    ):
        """-m emergency debe llegar al core con chaos activado."""
        capturado = {"chaos": None}

        async def spy(providers, timeout, chaos=False, custom_urls=None):
            capturado["chaos"] = chaos
            return []

        monkeypatch.setattr(app_operator, "scan_all_providers", spy)
        exit_code = app_operator.main(
            [
                "AWS",
                "-c", "cluster-us-east-01",
                "-m", "emergency",
                "-o", str(tmp_path / "em.log"),
            ]
        )
        assert exit_code == 0
        assert capturado["chaos"] is True


# ---------------------------------------------------------------------------
# PEP 765: AST-level enforcement
# ---------------------------------------------------------------------------

class TestPEP765:
    """HARD GATE: ningún finally contiene return/break/continue."""

    def test_finally_limpios_a_nivel_ast(self):
        source = Path(app_operator.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        violations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Try) and node.finalbody:
                for stmt in node.finalbody:
                    if isinstance(stmt, (ast.Return, ast.Break, ast.Continue)):
                        violations.append((node.lineno, type(stmt).__name__))
                # también sentencias anidadas dentro del finally
                for nested in ast.walk(node):
                    if nested is node:
                        continue
                    if isinstance(nested, (ast.Return, ast.Break, ast.Continue)):
                        violations.append((nested.lineno, type(nested).__name__))
        assert violations == [], f"Violaciones PEP 765: {violations}"

    def test_finally_contiene_shutdown_logging(self):
        """El finally debe apagar ordenadamente el listener de hilos."""
        source = Path(app_operator.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        found = False
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Try)
                and node.finalbody
                and isinstance(node.finalbody[0], ast.Expr)
            ):
                call = node.finalbody[0].value
                if isinstance(call, ast.Call) and isinstance(
                    call.func, ast.Name
                ):
                    if call.func.id == "shutdown_logging":
                        found = True
        assert found, "El finally de main() debe invocar shutdown_logging(listener)"


# ---------------------------------------------------------------------------
# API pública del paquete
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """__init__.py expone el contrato público del paquete."""

    def test_simbolos_publicos_presentes(self):
        for symbol in (
            "TritonError",
            "ProviderTimeoutError",
            "CorruptedPayloadError",
            "NetworkPeeringError",
            "scan_provider",
            "scan_all_providers",
            "AsyncJSONFormatter",
            "NonBlockingQueueHandler",
            "setup_logging",
            "shutdown_logging",
            "validate_timeout",
            "validate_cluster_id",
        ):
            assert hasattr(triton_telemetry, symbol), f"Falta {symbol}"
            assert symbol in triton_telemetry.__all__, f"Falta {symbol} en __all__"

    def test_all_es_lista_de_strings(self):
        assert all(isinstance(s, str) for s in triton_telemetry.__all__)
        assert len(triton_telemetry.__all__) == len(set(triton_telemetry.__all__))

    def test_excepciones_de_dominio_heredan_de_exception(self):
        assert TritonError.__base__ is Exception
        for cls in (ProviderTimeoutError, CorruptedPayloadError, NetworkPeeringError):
            assert issubclass(cls, TritonError)
