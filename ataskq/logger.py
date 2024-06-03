from typing import Union
import logging


class Logger:
    def __init__(self, logger: Union[str, logging.Logger, None]):
        if logger is None:
            self._logger = logging.getLogger("ataskq")
        elif isinstance(logger, str):
            self._logger = logging.getLogger(logger)
        else:
            self._logger = logger

    def exception(self, *args, **kwargs):
        self._logger.exception(*args, **kwargs)

    def critical(self, *args, **kwargs):
        self._logger.critical(*args, **kwargs)

    def error(self, *args, **kwargs):
        self._logger.error(*args, **kwargs)

    def warning(self, *args, **kwargs):
        self._logger.warning(*args, **kwargs)

    def info(self, *args, **kwargs):
        self._logger.info(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self._logger.debug(*args, **kwargs)
