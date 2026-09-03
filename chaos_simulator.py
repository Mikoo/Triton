import asyncio
import gzip
import json
import traceback
from pathlib import Path

from triton_telemetry.core import scan_all_providers


async def run_chaos_scenario() -> None:
    print("=== INICIO DE SIMULACIÓN DE CAOS ===")

    try:
        await scan_all_providers(
            providers=["AWS"],
            timeout=0.1,
            chaos=True,
        )
    except* Exception as error_group:
        print("Se detectó un fallo durante la simulación:")
        traceback.print_exception(error_group)

    print("=== FIN DE SIMULACIÓN DE CAOS ===")


async def run_network_error_scenario() -> None:
    print("\n=== ESCENARIO DE ERROR DE RED ===")

    try:
        await scan_all_providers(
            providers=["AWS"],
            timeout=5.0,
            chaos=False,
            custom_urls={
                "AWS": "https://dominio-que-no-existe-triton.invalid"
            },
        )
    except* Exception as error_group:
        print("Se detectó un fallo de red:")
        traceback.print_exception(error_group)

    print("=== FIN DEL ESCENARIO DE RED ===")


async def run_http_error_scenario() -> None:
    print("\n=== ESCENARIO DE ERROR HTTP ===")

    try:
        await scan_all_providers(
            providers=["Azure"],
            timeout=5.0,
            chaos=True,
        )
    except* Exception as error_group:
        print("Se detectó un error HTTP:")
        traceback.print_exception(error_group)

    print("=== FIN DEL ESCENARIO HTTP ===")


async def run_multiple_failures_scenario() -> None:
    print("\n=== ESCENARIO DE MÚLTIPLES FALLOS ===")

    try:
        await scan_all_providers(
            providers=["AWS", "Azure", "GCP"],
            timeout=0.1,
            chaos=True,
        )
    except* Exception as error_group:
        print("Se detectaron múltiples fallos concurrentes:")
        traceback.print_exception(error_group)

    print("=== FIN DEL ESCENARIO DE MÚLTIPLES FALLOS ===")


def validate_forensic_exception(data: dict) -> None:
    """
    Valida la estructura forense de una excepción
    serializada dentro del registro JSON.
    """

    exception = data.get("exception")

    # Si el registro no contiene una excepción,
    # no hay nada que auditar.
    if not exception:
        return

    # La excepción debe tener tipo y mensaje.
    if "type" not in exception:
        raise ValueError("La excepción no contiene el tipo")

    if "message" not in exception:
        raise ValueError("La excepción no contiene el mensaje")

    # Auditar la causa original.
    cause = exception.get("cause")

    if cause:
        if "type" not in cause:
            raise ValueError(
                "La causa no contiene el tipo de excepción"
            )

        if "message" not in cause:
            raise ValueError(
                "La causa no contiene el mensaje"
            )

        # Si la causa es un error HTTP de httpx,
        # verificar que conserve un código HTTP.
        if cause.get("type") == "HTTPStatusError":
            cause_message = cause.get("message", "")

            if not any(
                str(status) in cause_message
                for status in [
                    400,
                    401,
                    403,
                    404,
                    422,
                    500,
                    502,
                    503,
                    504,
                ]
            ):
                raise ValueError(
                    "El HTTPStatusError no contiene "
                    "un código HTTP válido"
                )

    # Verificar las notas forenses.
    notes = exception.get("notes", [])

    if not isinstance(notes, list):
        raise ValueError(
            "Las notas forenses deben estar en formato de lista"
        )

    # Si existe una nota HTTP, debe contener
    # un código de estado válido.
    for note in notes:
        if "HTTP Status Error" in note:
            if not any(
                str(status) in note
                for status in [
                    400,
                    401,
                    403,
                    404,
                    422,
                    500,
                    502,
                    503,
                    504,
                ]
            ):
                raise ValueError(
                    "La nota HTTP no contiene "
                    "un código de estado válido"
                )


def validate_json_log(log_file: str) -> None:
    """
    Audita el log JSON actual y los históricos Gzip.
    """

    path = Path(log_file)

    if not path.exists():
        print(f"No existe el archivo de log: {log_file}")
        return

    def validate_lines(lines) -> bool:
        for line_number, line in enumerate(lines, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                data = json.loads(line)

                required_fields = [
                    "timestamp",
                    "level",
                    "logger",
                    "message",
                ]

                for field in required_fields:
                    if field not in data:
                        raise ValueError(
                            f"Falta el campo obligatorio '{field}'"
                        )

                # Auditoría forense de excepciones.
                validate_forensic_exception(data)

            except (json.JSONDecodeError, ValueError) as error:
                print(
                    f"Registro JSON inválido en línea "
                    f"{line_number}: {error}"
                )
                return False

        return True

    # ---------------------------------------------------------
    # 1. Validar el log JSON actual
    # ---------------------------------------------------------

    with path.open("r", encoding="utf-8") as file:
        if not validate_lines(file):
            return

    # ---------------------------------------------------------
    # 2. Buscar históricos comprimidos .gz
    # ---------------------------------------------------------

    gz_files = list(
        path.parent.glob(f"{path.name}.*.gz")
    )

    for gz_path in gz_files:
        try:
            # Descompresión y validación del histórico.
            with gzip.open(
                gz_path,
                "rt",
                encoding="utf-8",
            ) as gz_file:

                if not validate_lines(gz_file):
                    print(
                        f"Error en archivo Gzip: "
                        f"{gz_path.name}"
                    )
                    return

        except (OSError, gzip.BadGzipFile) as error:
            print(
                f"Gzip inválido: "
                f"{gz_path.name}: {error}"
            )
            return

    print("Telemetría JSON validada correctamente.")


if __name__ == "__main__":
    asyncio.run(run_chaos_scenario())
    asyncio.run(run_network_error_scenario())
    asyncio.run(run_http_error_scenario())
    asyncio.run(run_multiple_failures_scenario())

    validate_json_log("triton_services.log")