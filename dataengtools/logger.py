import logging
from threading import Lock

class Logger(logging.Logger):
    _instance: 'Logger' = None
    _lock: Lock = Lock()

    def __new__(cls, logger_name: str) -> 'Logger':
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Logger, cls).__new__(cls)
                cls._instance._initialize_logger(logger_name)
            return cls._instance

    def _initialize_logger(self, logger_name: str) -> None:
        if not hasattr(self, '_initialized') or not self._initialized:
            super().__init__(logger_name)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.addHandler(handler)
            self.setLevel(logging.DEBUG)
            self._initialized = True
            
    def info(self, msg: str) -> None:
        print(f'INFO: {msg}')

    @classmethod
    def get_instance(cls, logger_name: str = "dataengtools_logger") -> 'Logger':
        return cls(logger_name)
