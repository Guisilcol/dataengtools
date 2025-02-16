
from typing import List, Generator, overload, Any, Optional
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.io.reader import Reader, ReaderOptions
from dataengtools.core.interfaces.io.writer import Writer, WriterOptions

from duckdb import DuckDBPyRelation, from_arrow
import polars as pl

class DuckDBFilesystemEngine(FilesystemEngine[DuckDBPyRelation]):
    def __init__(self, handler: FilesystemHandler, reader: Reader[DuckDBPyRelation], writer: Writer[DuckDBPyRelation]):
        self._handler = handler
        self._reader = reader
        self._writer = writer
        
    def get_files(self, prefix: str) -> List[str]:
        return self._handler.get_files(prefix)
        
    def delete_files(self, files: List[str]) -> None:
        self._handler.delete_files(files)
        
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> DuckDBPyRelation:
        data = self._reader.read(prefix, reader_options)
        return data
    
    def write_files(self, data: DuckDBPyRelation, path: str, writer_options: WriterOptions = {}) -> None:
        self._writer.write(data, path, writer_options)
    
class PolarsFilesystemEngine(DuckDBFilesystemEngine):

    @overload
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}) -> pl.DataFrame:
        ...

    @overload
    def read_files(self, prefix: str, reader_options: ReaderOptions = {}, *, batch_size: int) -> Generator[pl.DataFrame, None, None]:
        ...

    def read_files(
        self, 
        prefix: str, 
        reader_options: ReaderOptions = {}, 
        *, 
        batch_size: Optional[int] = None
    ) -> Any:
        data = super().read_files(prefix, reader_options)
        if batch_size:
            return (pl.from_arrow(batch) for batch in data.record_batch(batch_size))
        else:
            return data.pl()
        
    def write_files(self, data: pl.DataFrame, path: str, writer_options: WriterOptions = {}) -> None:
        return super().write_files(data.to_arrow(), path, writer_options)
