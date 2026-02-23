import logging

from pythonjsonlogger import jsonlogger


def init_console_log(level: str = "INFO") -> logging.Logger:
    for logger_name in logging.root.manager.loggerDict:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(0)

    console_handler = logging.StreamHandler()
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Incorrect logging level.")
    console_handler.setLevel(numeric_level)

    format_string = " ".join(
        "%(" + log_field_name + ")s"
        for log_field_name in [
            "asctime",
            "created",
            "filename",
            "funcName",
            "levelname",
            "lineno",
            "module",
            "message",
            "process",
            "thread",
            "threadName",
        ]
    )
    console_handler.setFormatter(jsonlogger.JsonFormatter(format_string))
    logger.addHandler(console_handler)
    logger.info("A console logger has been created.")
    return logger
