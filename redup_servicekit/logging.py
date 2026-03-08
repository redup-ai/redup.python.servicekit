"""Structured JSON console logging for production and log aggregators.

The :mod:`redup_servicekit.logging` module contains:

- :func:`redup_servicekit.logging.init_console_log`
"""
import logging

from pythonjsonlogger import jsonlogger


def init_console_log(level: str = "INFO") -> logging.Logger:
    r"""Create the root logger with a single JSON console handler.

    All other loggers are set to CRITICAL so only this handler's output
    is visible. JSON fields include asctime, created, filename, funcName,
    levelname, lineno, module, message, process, thread, threadName.

    :param level: Log level name (e.g. ``"INFO"``, ``"DEBUG"``). Default ``"INFO"``.
    :type level: str
    :return: The root logger instance.
    :rtype: logging.Logger
    :raises ValueError: If the level string is not a valid level name.

    Example:

    >>> from redup_servicekit.logging import init_console_log
    >>> logger = init_console_log(level="INFO")
    >>> logger.info("Service started")
    """
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
