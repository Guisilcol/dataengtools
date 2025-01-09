import logging
from sys import stdout

class Logger:

    @classmethod
    def get_instance(cls, logger_name: str = "dataengtools_logger") -> logging.Logger:
        if logging.Logger.manager.loggerDict.get(logger_name) is None:
            logger = logging.getLogger(logger_name)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler = logging.StreamHandler(stdout)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            return logger
        
        return logging.getLogger(logger_name)
