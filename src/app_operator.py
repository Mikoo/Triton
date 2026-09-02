"""
app_operator.py — Punto de Entrada CLI de Triton Cloud Services (Integrante 5).

Coordinador de Integración y Flujo CLI:
1. Define la frontera declarativa con argparse, delegando la validación
   semántica a los callables de sanitizer.py (Integrante 1).
2. Inicia el bucle de eventos asíncrono SOLO si la entrada es válida
   (Escenario B: argparse aborta con código 2 antes de arrancar asyncio).
3. Ejecuta la telemetría concurrente de core.py (Integrante 2) mediante
   asyncio.run() y captura quirúrgicamente el ExceptionGroup propagado
   por asyncio.TaskGroup con bloques except* (PEP 654).
4. Persiste el volcado forense en JSON estructurado a través del pipeline
   no bloqueante de logging_engine.py (Integrantes 3 y 4).

Hard Gates cumplidos:
- PEP 765: los bloques finally no contienen return/break/continue.
- Sin silenciamiento ciego: toda excepción queda registrada en el log forense.
- Las excepciones de dominio heredan de Exception (nunca de BaseException).
"""

import argparse
import asyncio
import logging
import sys
from typing import Any, Sequence

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
        setup_logging,
        shutdown_logging,
        validate_cluster_id,
        validate_timeout,
    )
except ModuleNotFoundError:  # pragma: no cover - ruta alternativa de import
    from src.triton_telemetry import (
        CorruptedPayloadError,
        NetworkPeeringError,
        ProviderTimeoutError,
        TritonError,
        scan_all_providers,
        setup_logging,
        shutdown_logging,
        validate_cluster_id,
        validate_timeout,
    )

# Proveedores cloud soportados por el monitor (superconjunto de los endpoints)
AVAILABLE_PROVIDERS: tuple[str, ...] = ("AWS", "Azure", "GCP")

# Códigos de salida del proceso (contrato observable del CLI)
EXIT_OK: int = 0          # Escenario A: telemetría nominal exitosa
EXIT_FAILURE: int = 1     # Escenario C: fallos de dominio capturados
EXIT_BAD_ARGS: int = 2    # Escenario B: entrada inválida (argparse)


def build_parser() -> argparse.ArgumentParser:
    """
    Construye el parser declarativo de la frontera CLI.

    Los tipos delegados --cluster y --timeout son los callables validadores
    de sanitizer.py: ante un valor inválido, argparse imprime el error y
    ejecuta sys.exit(2) sin que se inicie el bucle de eventos asíncrono.
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
        "--chaos",
        action="store_true",
        help="Activa la inyección de caos: latencias extremas, HTTP 504 y 422.",
    )

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
        default="INFO",
        help="Nivel de verbosidad del log forense (default: INFO).",
    )

    return parser


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
    fallos), imprime el resumen human-readable y persiste el volcado forense
    completo (mensaje, notas, causa y children) vía exc_info, que el
    AsyncJSONFormatter serializa a JSON estructurado.
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


def _print_nominal_report(
    results: Sequence[dict[str, Any]],
    cluster_id: str,
    chaos_mode: bool,
) -> None:
    """Imprime el reporte tabulado de telemetría nominal (Escenario A / stdout)."""
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


def _print_failure_summary(provider: str | None, message: str) -> None:
    """Imprime en stderr el resumen human-readable de un fallo de dominio."""
    prefix = f"[{provider}] " if provider else ""
    print(f"{prefix}FALLO: {message}", file=sys.stderr)


def _run_scan(
    parsed_args: argparse.Namespace, logger: logging.Logger
) -> int:
    """
    Ejecuta la telemetría asíncrona y aplica la captura quirúrgica except*.

    IMPORTANTE (PEP 654): dentro de un bloque except* está prohibido usar
    return/break/continue; por eso la salida se transporta en la variable
    exit_code y el return se deja fuera del bloque de captura.
    """
    logger.info(
        "Iniciando escaneo de telemetría",
        extra={
            "cluster_id": parsed_args.cluster,
            "providers": list(parsed_args.providers),
            "timeout": parsed_args.timeout,
            "chaos": parsed_args.chaos,
        },
    )

    exit_code: int = EXIT_OK
    collected_results: list[dict[str, Any]] | None = None

    try:
        collected_results = asyncio.run(
            scan_all_providers(
                providers=list(parsed_args.providers),
                timeout=parsed_args.timeout,
                chaos=parsed_args.chaos,
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
            chaos_mode=parsed_args.chaos,
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
    2. setup_logging() levanta el pipeline no bloqueante (QueueHandler).
    3. _run_scan() ejecuta la telemetría concurrente con TaskGroup y
       aplica la captura quirúrgica except*.
    4. shutdown_logging() drena la cola y libera los recursos en finally.
    """
    parsed_args = build_parser().parse_args(argv)

    listener, logger = setup_logging(
        log_file=parsed_args.log_file,
        log_level=parsed_args.log_level,
    )

    try:
        exit_code = _run_scan(parsed_args, logger)
    finally:
        # PEP 765: este finally va limpio (sin return/break/continue):
        # solo drena la cola y libera descriptores de archivo.
        shutdown_logging(listener)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
