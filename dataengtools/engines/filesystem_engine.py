
from typing import List, Generator, overload, Any
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine, FileMetadata
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.io.reader import Reader, ReaderOptions

from duckdb import DuckDBPyRelation
import polars as pl

class DuckDBFilesystemEngine(FilesystemEngine[DuckDBPyRelation]):
    def __init__(self, handler: FilesystemHandler, reader: Reader[DuckDBPyRelation]):
        self._handler = handler
        self._reader = reader
        
    def get_files(self, prefix: str) -> List[str]:
        return self._handler.get_files(prefix)
        
    def delete_files(self, files: List[str]) -> None:
        self._handler.delete_files(files)
        
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> DuckDBPyRelation:
        data = self._reader.read(prefix, reader_options)
        return data
    
class PolarsFilesystemEngine(DuckDBFilesystemEngine):

    @overload
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> pl.DataFrame:
        ...

    @overload
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}, *, batch_size: int = 100_000) -> Generator[pl.DataFrame, None, None]:
        ...


    def read_files(self, prefix: str, reader_options: ReaderOptions = {}, *, batch_size: int = 100_000) -> Any:
        data = super().read_files(prefix, reader_options)
        
        if batch_size:
            for batch in data.record_batch(batch_size):
                yield pl.from_arrow(batch)

        else:
            return data.pl()
