import gzip
import json
import logging
import os
import queue
import shutil
import sys
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import Any


class AsyncJSONFormatter(logging.Formatter):
    """Formateador que convierte LogRecord en JSON estructurado."""

    def format(self, record: logging.LogRecord) -> str:
        dt_utc = datetime.fromtimestamp(record.created, tz=timezone.utc)
        timestamp = dt_utc.isoformat().replace("+00:00", "Z")

        log_payload = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "threadName": record.threadName,
            "taskName": getattr(record, "taskName", "None"),  # Python 3.12+
            "filename": record.filename,
            "line": record.lineno,
            "function": record.funcName,
        }
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            if exc_value:
                log_payload["exception"] = self._serialize_exception(exc_value)
                log_payload["traceback"] = self.formatException(record.exc_info)

        reserved_fields = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in reserved_fields and not key.startswith("_"):
                log_payload[key] = value
        return json.dumps(log_payload, default=str)

    def _serialize_exception(self, exc: BaseException) -> dict[str, Any]:
        """
        Convierte una excepción (y sus anidaciones) en un diccionario.
        Esta función es recursiva para manejar ExceptionGroup.
        """

        exc_data = {
            "type": exc.__class__.__name__,
            "module": exc.__class__.__module__,
            "message": str(exc),
            "notes": getattr(exc, "__notes__", []),  # Captura notas con add_note()
        }

        if hasattr(exc, "exceptions") and isinstance(exc, ExceptionGroup):
            exc_data["children"] = [
                self._serialize_exception(child) for child in exc.exceptions
            ]

        if exc.__cause__ is not None:
            exc_data["cause"] = self._serialize_exception(exc.__cause__)

        return exc_data


def gzip_namer(default_name: str) -> str:
    """
    Callback de rotación: añade la extensión .gz al archivo de log cerrado.
    Ejemplo: 'triton_services.log.1' -> 'triton_services.log.1.gz'
    """
    return default_name + ".gz"


def gzip_rotator(source: str, dest: str) -> None:
    """
    Callback de rotación: comprime atómicamente el archivo histórico cerrado
    a formato .gz y elimina de forma segura el archivo plano residual.
    """
    if os.path.exists(source):
        # 1. Compresión atómica en caliente
        with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        # 2. Eliminación segura del archivo plano residual
        try:
            os.remove(source)
        except OSError:
            pass


class NonBlockingQueueHandler(QueueHandler):
    """
    QueueHandler que preserva el objeto LogRecord y exc_info intactos
    para que el AsyncJSONFormatter de Lu pueda inspeccionarlo en el hilo secundario.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


def setup_logging(
    log_file: str = "triton_services.log",
    log_level: str = "INFO",
    max_bytes: int = 2 * 1024 * 1024,  # Límite estricto de 2 MB
    backup_count: int = 3,  # Máximo 3 backups históricos
    enable_console: bool = True,
) -> tuple[QueueListener, logging.Logger]:
    """
    Inicializa el pipeline desacoplado:
    - Las corrutinas envían logs a memoria RAM (QueueHandler) en microsegundos.
    - Un hilo secundario (QueueListener) escribe en disco usando el AsyncJSONFormatter de Lu.
    - Se aplica rotación acotada a 2 MB con compresión Hot Gzip.
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    # 1. Cola sincronizada e hilo-segura en memoria RAM
    log_queue: queue.Queue = queue.Queue(maxsize=10000)
    # 2. Instanciamos el formateador JSON de Lu
    json_formatter = AsyncJSONFormatter()
    # 3. Manejador rotativo con compresión Gzip
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(json_formatter)
    file_handler.setLevel(numeric_level)
    file_handler.namer = gzip_namer
    file_handler.rotator = gzip_rotator
    handlers: list[logging.Handler] = [file_handler]
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        console_handler.setLevel(numeric_level)
        handlers.append(console_handler)
    # 4. Listener que corre en un hilo secundario desatendido
    listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    listener.start()
    # 5. Logger raíz de Triton con el QueueHandler no bloqueante
    logger = logging.getLogger("triton")
    logger.setLevel(numeric_level)
    logger.handlers.clear()

    queue_handler = NonBlockingQueueHandler(log_queue)
    logger.addHandler(queue_handler)
    return listener, logger


def shutdown_logging(listener: QueueListener | None) -> None:
    """
    Detiene de forma limpia el QueueListener procesando eventos pendientes
    y liberando los descriptores de archivo en el sistema operativo.
    """
    if listener is not None:
        try:
            listener.stop()
        except Exception:
            pass
        for handler in getattr(listener, "handlers", []):
            try:
                handler.close()
            except Exception:
                pass
