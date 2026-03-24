import json
import logging
import uuid


class MerginLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = dict(self.extra or {})
        extra.update(kwargs.get("extra", {}))
        kwargs["extra"] = extra
        return msg, kwargs


class JsonFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        base = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", None),
            "msg": record.getMessage(),
        }

        standard_attrs = {
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
            "asctime",
        }

        extra = {k: v for k, v in record.__dict__.items() if k not in standard_attrs}
        base.update(extra)

        return json.dumps(base, default=str)


def setup_logging() -> logging.LoggerAdapter:
    """Configura logging estruturado para saída em stdout.
    usa `run_id` para correlacionar entradas de log no mesmo processo.
    """
    run_id = uuid.uuid4().hex
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    root.propagate = False

    return MerginLoggerAdapter(logging.getLogger(__name__), {"run_id": run_id})
