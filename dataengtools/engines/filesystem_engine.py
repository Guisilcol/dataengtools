
from typing import List, Generator, Optional, Generator
from dataengtools.core.interfaces.engine_layer.filesystem import FilesystemEngine, FileMetadata
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.core.interfaces.io.reader import Reader

from duckdb import DuckDBPyRelation

class DuckDBFilesystemEngine(FilesystemEngine[DuckDBPyRelation]):
    def __init__(self, handler: FilesystemHandler, reader: Reader[DuckDBPyRelation]):
        self._handler = handler
        self._reader = reader
        
    def get_files(self, prefix: str) -> List[str]:
        return self._handler.get_files(prefix)
        
    def delete_files(self, files: List[str]) -> None:
        self._handler.delete_files(files)
        
    def read_files(self, prefix: str, filetype: str, file_metadata: FileMetadata = {}) -> DuckDBPyRelation:
        data, _ = self._reader.read(
            path = prefix,
            have_header=file_metadata.get('has_header', False),
            delimiter=file_metadata.get('separator', ','),
            file_type=filetype,
            columns=file_metadata.get('columns')
        )

        return data
    
    def read_files_batched(
            self, 
            prefix: str, 
            filetype: str, 
            file_metadata: FileMetadata = {},
            columns: Optional[List[str]] = None, 
            order_by: Optional[List[str]] = None,
            batch_size: int = 10000
    ) -> Generator[DuckDBPyRelation, None, None]:

        current_offset = 0
        while True:
            batch, count = self._reader.read(
                path=prefix,
                columns=columns,
                file_type=filetype,
                delimiter=file_metadata.get('separator', ','),
                have_header=file_metadata.get('has_header', False),
                offset=current_offset,
                limit=batch_size,
                order_by=order_by
            )
            
            if count == 0:
                break

            yield batch
            current_offset += batch_size