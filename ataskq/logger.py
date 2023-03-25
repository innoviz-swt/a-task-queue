import logging

class Logger:
    def __init__(self, logger: logging.Logger or None):
        self._logger = logger or logging.getLogger('ataskq')
    
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