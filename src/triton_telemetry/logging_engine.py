import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict

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
                "function": record.funcName
            }
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            if exc_value:
                log_payload["exception"] = self._serialize_exception(exc_value)
                log_payload["traceback"] = self.formatException(record.exc_info)
        
        reserved_fields = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message",
            "taskName"
        }
        for key, value in record.__dict__.items():
            if key not in reserved_fields and not key.startswith('_'):
                log_payload[key] = value
        return json.dumps(log_payload, default=str)

    def _serialize_exception(self, exc: BaseException) -> Dict[str, Any]:
        """
            Convierte una excepción (y sus anidaciones) en un diccionario.
            Esta función es recursiva para manejar ExceptionGroup.
        """

        exc_data = {
            "type": exc.__class__.__name__,
            "module": exc.__class__.__module__,
            "message": str(exc),
            "notes": getattr(exc, "__notes__", [])  # Captura notas con add_note()
        }
    
        if hasattr(exc, "exceptions") and isinstance(exc, ExceptionGroup):
            exc_data["children"] = [
                self._serialize_exception(child) 
                for child in exc.exceptions
        ]
    
        if exc.__cause__ is not None:
            exc_data["cause"] = self._serialize_exception(exc.__cause__)
        
        return exc_data
