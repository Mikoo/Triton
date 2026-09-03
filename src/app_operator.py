"""
app_operator.py — Punto de Entrada CLI de Triton Cloud Services (Integrante 5).

Coordinador de Integración y Flujo CLI:
1. Define la frontera declarativa con argparse, delegando la validación
   semántica a los callables de sanitizer.py (Integrante 1) y restringe
   modos operativos (nominal / debug / emergency) con choices, más un
   grupo mutuamente excluyente de salida de texto (--tabla / --json).
2. Inyecta el esquema completo de logging de forma declarativa con
   logging.config.dictConfig() al inicio del script.
3. Inicia el bucle de eventos asíncrono SOLO si la entrada es válida
   (Escenario B: argparse aborta con código 2 antes de arrancar asyncio).
4. Ejecuta la telemetría concurrente de core.py (Integrante 2) mediante
   asyncio.run() y captura quirúrgicamente el ExceptionGroup propagado
   por asyncio.TaskGroup con bloques except* (PEP 654), imprimiendo las
   notas forenses (PEP 678) en consola.
5. Persiste el volcado forense en JSON estructurado a través del pipeline
   no bloqueante de logging_engine.py (Integrantes 3 y 4).

Hard Gates cumplidos:
- PEP 765: los bloques finally no contienen return/break/continue.
- Sin silenciamiento ciego: toda excepción queda registrada en el log forense.
- Las excepciones de dominio heredan de Exception (nunca de BaseException).
"""

import argparse
import asyncio
import json
import logging
import logging.config
import queue
import sys
from logging.handlers import QueueListener, RotatingFileHandler
from typing import Any, Sequence, TypeVar

# Generic para tipar la factory de registro sin perder el subtipo concreto
_T = TypeVar("_T", bound=logging.Handler)

# Soporte de importación dual:
# 1) Ejecución directa: python src/app_operator.py  -> 'triton_telemetry' visible
# 2) Importación desde la raíz del repo (pytest):   -> 'src.triton_telemetry'
try:
    from triton_telemetry import (
        CorruptedPayloadError,
        NetworkPeeringError,
        ProviderTimeoutError,
        TritonError,
        scan_all_providers,
        shutdown_logging,
        validate_cluster_id,
        validate_timeout,
    )
    from triton_telemetry.logging_engine import (
        AsyncJSONFormatter,
        NonBlockingQueueHandler,
        gzip_namer,
        gzip_rotator,
    )
except ModuleNotFoundError:  # pragma: no cover - ruta alternativa de import
    from src.triton_telemetry import (
        CorruptedPayloadError,
        NetworkPeeringError,
        ProviderTimeoutError,
        TritonError,
        scan_all_providers,
        shutdown_logging,
        validate_cluster_id,
        validate_timeout,
    )
    from src.triton_telemetry.logging_engine import (
        AsyncJSONFormatter,
        NonBlockingQueueHandler,
        gzip_namer,
        gzip_rotator,
    )

# Proveedores cloud soportados por el monitor (superconjunto de los endpoints)
AVAILABLE_PROVIDERS: tuple[str, ...] = ("AWS", "Azure", "GCP")

# Modos operativos del monitor, restringidos por choices en la frontera CLI
OPERATION_MODES: tuple[str, ...] = ("nominal", "debug", "emergency")

# Códigos de salida del proceso (contrato observable del CLI)
EXIT_OK: int = 0          # Escenario A: telemetría nominal exitosa
EXIT_FAILURE: int = 1     # Escenario C: fallos de dominio capturados
EXIT_BAD_ARGS: int = 2    # Escenario B: entrada inválida (argparse)

# Tamaños por defecto del archivo forense rotativo (contrato con Responsabilidad 4)
_LOG_MAX_BYTES: int = 2 * 1024 * 1024  # 2 MB estrictos
_LOG_BACKUP_COUNT: int = 3             # máximo 3 backups históricos
_LOG_QUEUE_MAXSIZE: int = 10000        # cola RAM acotada no bloqueante

# Registro de instancias creadas por las factories declarativas de dictConfig:
# dictConfig instancia los handlers, pero NO puede crear el QueueListener
# (no es un Handler); por eso las factories devuelven y REGISTRAN sus
# objetos para que setup_declarative_logging ensamble el listener con ellos.
_REGISTERED_HANDLERS: dict[str, logging.Handler] = {}


# ---------------------------------------------------------------------------
# Configuración declarativa de logging (dictConfig)
# ---------------------------------------------------------------------------

def build_logging_config(
    log_file: str,
    log_level: str,
    max_bytes: int = _LOG_MAX_BYTES,
    backup_count: int = _LOG_BACKUP_COUNT,
    queue_maxsize: int = _LOG_QUEUE_MAXSIZE,
) -> dict[str, Any]:
    """
    Construye el ESQUEMA DECLARATIVO completo de logging.

    Estructura del esquema:
    - version: 1 (formato dictConfig obligatorio).
    - formatters: 'forense_json' (AsyncJSONFormatter del Integrante 3) y
      'consola_texto' (formato human-readable para stdout).
    - handlers: 'archivo_forense' (RotatingFileHandler con compresión Hot
      Gzip), 'consola' (StreamHandler) y 'cola_memoria' (QueueHandler no
      bloqueante que desacopla la I/O del event loop).
    - loggers: queue 'triton' enruta SOLO a la cola de memoria RMS.

    El esquema se inyecta íntegramente mediante logging.config.dictConfig()
    en setup_declarative_logging(): declarativo, sin configuraciones
    dispersas a lo largo del script.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "forense_json": {"()": AsyncJSONFormatter},
            "consola_texto": {
                "format": "%(asctime)s [%(levelname)s] %(message)s"
            },
        },
        "handlers": {
            "archivo_forense": {
                "()": _build_file_handler,
                "filename": log_file,
                "max_bytes": max_bytes,
                "backup_count": backup_count,
                "formatter": "forense_json",
                "level": log_level,
            },
            "consola": {
                "()": _build_console_handler,
                "formatter": "consola_texto",
                "level": log_level,
            },
            "cola_memoria": {
                "()": _build_queue_handler,
                "queue_maxsize": queue_maxsize,
            },
        },
        "loggers": {
            "triton": {
                "level": log_level,
                "handlers": ["cola_memoria"],
                "propagate": False,
            }
        },
    }


def _register_handler(name: str, handler: _T) -> _T:
    """Registra y devuelve una instancia creada por una factory dictConfig."""
    _REGISTERED_HANDLERS[name] = handler
    return handler


def _build_file_handler(
    filename: str,
    max_bytes: int,
    backup_count: int,
) -> RotatingFileHandler:
    """Factory del RotatingFileHandler con compresión atómica Hot Gzip."""
    handler = RotatingFileHandler(
        filename=filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.namer = gzip_namer
    handler.rotator = gzip_rotator
    return _register_handler("archivo_forense", handler)


def _build_console_handler() -> logging.StreamHandler:
    """
    Factory del handler de consola (destino: stderr).

    stdout queda SÓLO para el reporte oficial (tabulado o --json) y el
    stream de logs va a stderr: el contrato de salida permanece limpio
    y maquinable incluso con verbosidad elevada (modo debug).
    """
    return _register_handler("consola", logging.StreamHandler(sys.stderr))


def _build_queue_handler(queue_maxsize: int) -> NonBlockingQueueHandler:
    """Factory del QueueHandler no bloqueante sobre la cola de memoria."""
    queue_handler = NonBlockingQueueHandler(queue.Queue(maxsize=queue_maxsize))
    return _register_handler("cola_memoria", queue_handler)


def setup_declarative_logging(
    log_file: str,
    log_level: str,
) -> tuple[QueueListener, logging.Logger]:
    """
    Inyecta el esquema de logging con dictConfig y levanta el QueueListener.

    dictConfig NO puede instanciar un QueueListener (no es un Handler):
    - El esquema declara todos los Handlers y formatters (la configuración).
    - Aquí se recupera la cola y el listener con los handlers destino,
      respetando el respeto de niveles por handler (respect_handler_level).
    Devuelve (listener, logger) para que main() gestione su ciclo de vida.
    """
    schema = build_logging_config(log_file=log_file, log_level=log_level)
    logging.config.dictConfig(schema)

    queue_handler = _resolve_queue_handler()
    log_queue = queue_handler.queue

    listener = QueueListener(
        log_queue,
        _resolve_file_handler(),
        _resolve_console_handler(),
        respect_handler_level=True,
    )
    listener.start()

    return listener, logging.getLogger("triton")


def _resolve_queue_handler() -> NonBlockingQueueHandler:
    """Recupera la instancia del QueueHandler instalada por dictConfig."""
    return logging.getLogger("triton").handlers[0]  # type: ignore[return-value]


def _resolve_file_handler() -> RotatingFileHandler:
    """Recupera el RotatingFileHandler registrado por su factory dictConfig."""
    return _require_registered_handler("archivo_forense", RotatingFileHandler)


def _resolve_console_handler() -> logging.StreamHandler:
    """Recupera el StreamHandler registrado por su factory dictConfig."""
    return _require_registered_handler("consola", logging.StreamHandler)


def _require_registered_handler(name: str, expected_type: type) -> Any:
    """
    Devuelve la instancia registrada por la factory declarativa del esquema.

    Si la factory no fue invocada (p. ej. un esquema instanciado fuera de
    dictConfig), falla en voz alta: jamás se silencia un mal ensamblado.
    """
    handler = _REGISTERED_HANDLERS.get(name)
    if not isinstance(handler, expected_type):
        raise RuntimeError(
            f"Handler '{name}' no registrado por dictConfig: "
            f"el esquema declarativo es obligatorio."
        )
    return handler


# ---------------------------------------------------------------------------
# Interpretación de modos operativos
# ---------------------------------------------------------------------------

def resolve_mode_settings(
    mode: str, explicit_log_level: str | None
) -> dict[str, Any]:
    """
    Traduce el modo operativo a parámetros concretos del monitor.

    - nominal:  operación estándar (caos desactivado, nivel INFO).
    - debug:    diagnóstico profundo (nivel DEBUG, caos desactivado);
                útil para auditar el volcado forense completo.
    - emergency: simulación de contingencia (caos ACTIVADO): se dispara
                la inyección de fallos para ejercitar la resiliencia.
    El log-level explícito del usuario SIEMPRE tiene prioridad sobre el
    nivel derivado del modo (defensa de la intención explícita).
    """
    settings: dict[str, Any] = {
        "level_by_mode": {
            "nominal": "INFO",
            "debug": "DEBUG",
            "emergency": "INFO",
        },
        "chaos_by_mode": {
            "nominal": False,
            "debug": False,
            "emergency": True,
        },
    }
    return {
        "chaos": settings["chaos_by_mode"][mode],
        "log_level": explicit_log_level or settings["level_by_mode"][mode],
    }


# ---------------------------------------------------------------------------
# Parser declarativo de la frontera CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """
    Construye el parser declarativo de la frontera CLI.

    - Los tipos de --cluster y --timeout son los callables validadores de
      sanitizer.py: ante un valor inválido, argparse imprime el error y
      ejecuta sys.exit(2) SIN que se inicie el bucle de eventos asíncrono.
    - --mode restringe con choices los modos operativos: nominal / debug /
      emergency (default: nominal).
    - --tabla / --json forman un grupo MUTUAMENTE EXCLUYENTE de salida de
      texto: elegir ambos es un error de frontera (código 2).
    """
    parser = argparse.ArgumentParser(
        prog="TritonMonitor",
        description=(
            "Monitor de resiliencia asíncrona y observabilidad forense de "
            "Triton Cloud Services (AWS, Azure, GCP)."
        ),
        epilog=(
            "Ejemplo nominal: python src/app_operator.py AWS GCP "
            "-c cluster-us-east-01 -t 3.0\n"
            "Ejemplo con inyección de caos: python src/app_operator.py "
            "AWS Azure GCP -c cluster-us-west-02 -t 1.5 --chaos"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "providers",
        nargs="+",
        choices=AVAILABLE_PROVIDERS,
        metavar="PROVIDER",
        help="Proveedores a monitorear (selección de %(choices)s), mínimo uno.",
    )

    parser.add_argument(
        "-c",
        "--cluster",
        required=True,
        type=validate_cluster_id,
        metavar="CLUSTER_ID",
        help="Identificador del clúster: 'cluster-<region>-<numero>' (ej.: cluster-us-east-01).",
    )

    parser.add_argument(
        "-t",
        "--timeout",
        type=validate_timeout,
        default="3.0",
        metavar="SECONDS",
        help="Límite de tiempo por petición HTTP entre 0.1 y 5.0 segundos (default: 3.0).",
    )

    parser.add_argument(
        "-m",
        "--mode",
        choices=OPERATION_MODES,
        default="nominal",
        metavar="MODE",
        help="Modo operativo: %(choices)s (default: %(default)s).",
    )

    parser.add_argument(
        "--chaos",
        action="store_true",
        help="Activa la inyección de caos: latencias extremas, HTTP 504 y 422.",
    )

    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--tabla",
        dest="output_format",
        action="store_const",
        const="tabla",
        help="Reporte human-readable tabulado en stdout (default).",
    )
    output_group.add_argument(
        "--json",
        dest="output_format",
        action="store_const",
        const="json",
        help="Reporte estructurado JSON en stdout (excluyente con --tabla).",
    )
    parser.set_defaults(output_format="tabla")

    parser.add_argument(
        "-o",
        "--log-file",
        default="triton_services.log",
        metavar="PATH",
        help="Ruta del log forense JSON no bloqueante (default: triton_services.log).",
    )

    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default=None,
        metavar="LEVEL",
        help="Nivel de verbosidad (default: derivado del modo operativo, INFO).",
    )

    return parser


# ---------------------------------------------------------------------------
# Reportes y apoyo a la captura quirúrgica
# ---------------------------------------------------------------------------

def _print_failure_summary(provider: str | None, message: str) -> None:
    """Imprime en stderr el resumen human-readable de un fallo de dominio."""
    prefix = f"[{provider}] " if provider else ""
    print(f"{prefix}FALLO: {message}", file=sys.stderr)


def _print_forensic_notes(failure: BaseException) -> None:
    """
    Imprime en stderr las NOTAS FORENSES (PEP 678) de una excepción.

    core.py inyecta notas con add_note() (ej.: endpoint consultado o
    latencia umbral) que permiten reconstruir el contexto de cada fallo;
    el cliente del CLI las recibe en consola sin abrir el log forense.
    """
    for note in getattr(failure, "__notes__", []):
        print(f"    nota forense: {note}", file=sys.stderr)


def _log_exception_group(
    exception_group: ExceptionGroup,
    logger: logging.Logger,
    cluster_id: str,
    log_level: int,
    template: str,
) -> None:
    """
    Registra cada miembro de un ExceptionGroup y su descendencia anidada.

    Desanida recursivamente subgrupos (defensivo: TaskGroup puede anidar
    fallos), imprime el resumen human-readable y las notas forenses, y
    persiste el volcado forense completo (mensaje, notas, causa y children)
    vía exc_info, que el AsyncJSONFormatter serializa a JSON estructurado.
    """
    for failure in exception_group.exceptions:
        if isinstance(failure, ExceptionGroup):
            _log_exception_group(
                failure, logger, cluster_id, log_level, template
            )
            continue

        logger.log(
            log_level,
            template.format(failure=failure),
            exc_info=failure,
            extra={"cluster_id": cluster_id},
        )
        _print_failure_summary(
            getattr(failure, "provider", None), str(failure)
        )
        _print_forensic_notes(failure)


def _print_nominal_report(
    results: Sequence[dict[str, Any]],
    cluster_id: str,
    chaos_mode: bool,
    output_format: str,
) -> None:
    """
    Imprime el reporte oficial de telemetría nominal (Escenario A / stdout).

    - 'tabla': formato human-readable alineado con el contrato del README.
    - 'json':  estructura JSON maquinable para integración con pipelines.
    """
    if output_format == "json":
        print(
            json.dumps(
                {
                    "cluster_id": cluster_id,
                    "chaos": chaos_mode,
                    "status": "OK",
                    "results": list(results),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    print()
    print("=" * 72)
    print("TRITON MONITOR — TELEMETRÍA MULTICLOUD")
    print(f"Clúster: {cluster_id} | Modo: {'CAOS' if chaos_mode else 'NOMINAL'}")
    print("=" * 72)

    print(
        f"{'PROVEEDOR':<12} {'ESTADO':<10} {'HTTP':<6} "
        f"{'LATENCIA (ms)':<16} ENDPOINT"
    )
    print("-" * 72)

    for result in sorted(results, key=lambda item: item["provider"]):
        latency = f"{result['latency_ms']:.2f}"
        print(
            f"{result['provider']:<12} "
            f"{result['status']:<10} "
            f"{result['status_code']:<6} "
            f"{latency:<16} "
            f"{result['endpoint']}"
        )

    print("-" * 72)
    print(f"Resumen: {len(results)}/{len(results)} nodos nominales — sin fallos.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Ejecución del escaneo asíncrono y captura quirúrgica
# ---------------------------------------------------------------------------

def _run_scan(
    parsed_args: argparse.Namespace,
    logger: logging.Logger,
    mode_settings: dict[str, Any],
) -> int:
    """
    Ejecuta la telemetría asíncrona y aplica la captura quirúrgica except*.

    IMPORTANTE (PEP 654): dentro de un bloque except* está prohibido usar
    return/break/continue; por eso la salida se transporta en la variable
    exit_code y el return se deja fuera del bloque de captura.
    """
    chaos_active: bool = parsed_args.chaos or mode_settings["chaos"]

    logger.info(
        "Iniciando escaneo de telemetría",
        extra={
            "cluster_id": parsed_args.cluster,
            "providers": list(parsed_args.providers),
            "timeout": parsed_args.timeout,
            "chaos": chaos_active,
            "mode": parsed_args.mode,
        },
    )

    exit_code: int = EXIT_OK
    collected_results: list[dict[str, Any]] | None = None

    try:
        collected_results = asyncio.run(
            scan_all_providers(
                providers=list(parsed_args.providers),
                timeout=parsed_args.timeout,
                chaos=chaos_active,
            )
        )

    except* ProviderTimeoutError as timeout_group:
        _log_exception_group(
            timeout_group, logger, parsed_args.cluster,
            logging.ERROR, "Timeout de red: {failure}",
        )
        exit_code = EXIT_FAILURE

    except* CorruptedPayloadError as payload_group:
        _log_exception_group(
            payload_group, logger, parsed_args.cluster,
            logging.ERROR, "Payload corrupto: {failure}",
        )
        exit_code = EXIT_FAILURE

    except* NetworkPeeringError as peering_group:
        _log_exception_group(
            peering_group, logger, parsed_args.cluster,
            logging.CRITICAL, "Fallo de peering: {failure}",
        )
        exit_code = EXIT_FAILURE

    except* TritonError as residual_group:
        # Guardián de dominio: excepciones Triton no clasificadas. Nunca se
        # silencian ni se propagan crudas: se registran y degradan la salida.
        _log_exception_group(
            residual_group, logger, parsed_args.cluster,
            logging.ERROR, "Fallo de dominio no clasificado: {failure}",
        )
        exit_code = EXIT_FAILURE

    except* Exception as unexpected_group:
        # Guardián de integridad: lo que no es dominio Triton (p. ej. un
        # ParseError de httpx) NO se traga: se registra y falla alto.
        _log_exception_group(
            unexpected_group, logger, parsed_args.cluster,
            logging.CRITICAL, "Fallo inesperado fuera del dominio: {failure!r}",
        )
        exit_code = EXIT_FAILURE

    if exit_code == EXIT_OK and collected_results is not None:
        # Flujo nominal alcanzado SOLO si ninguna corrutina falló
        _print_nominal_report(
            results=collected_results,
            cluster_id=parsed_args.cluster,
            chaos_mode=chaos_active,
            output_format=parsed_args.output_format,
        )

        for result in collected_results:
            logger.info(
                "Telemetría nominal recibida",
                extra={
                    "cluster_id": parsed_args.cluster,
                    "provider": result["provider"],
                    "status_code": result["status_code"],
                    "latency_ms": result["latency_ms"],
                    "endpoint": result["endpoint"],
                },
            )

        logger.info(
            "Escaneo de telemetría finalizado sin fallos",
            extra={"cluster_id": parsed_args.cluster},
        )

    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    """
    Coordina el flujo completo del monitor y devuelve el código de salida.

    Flujo:
    1. parse_args() valida la frontera (salida 2 ante entradas inválidas).
    2. resolve_mode_settings() traduce nominal/debug/emergency.
    3. setup_declarative_logging() inyecta el esquema dictConfig y levanta
       el pipeline no bloqueante (QueueHandler + QueueListener).
    4. _run_scan() ejecuta la telemetría concurrente con TaskGroup y
       aplica la captura quirúrgica except*.
    5. shutdown_logging() drena la cola y libera los recursos en finally.
    """
    parsed_args = build_parser().parse_args(argv)

    mode_settings = resolve_mode_settings(
        mode=parsed_args.mode,
        explicit_log_level=parsed_args.log_level,
    )

    listener, logger = setup_declarative_logging(
        log_file=parsed_args.log_file,
        log_level=mode_settings["log_level"],
    )

    try:
        exit_code = _run_scan(parsed_args, logger, mode_settings)
    finally:
        # PEP 765: este finally va limpio (sin return/break/continue):
        # solo drena la cola y libera descriptores de archivo.
        shutdown_logging(listener)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
