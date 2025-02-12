from dataengtools.core.interfaces.io.reader import Reader
from dataengtools.io.duckdb_io.string_builder import StringBuilder
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from typing import Optional, List, Tuple

class DuckDBReader(Reader[DuckDBPyRelation]):
    def __init__(self, connection: DuckDBPyConnection, sql_configurator: SQLProviderConfigurator[DuckDBPyConnection]):
        self.connection = connection
        self.sql_configurator = sql_configurator
        self.sql_configurator.configure_connection(connection)

    def wrap_path_in_reader_function(self, 
                                     path: str, 
                                     file_type: Optional[str] = None, 
                                     have_header: Optional[bool] = None, 
                                     separator: Optional[str] = None
    ) -> str:
        if file_type is None:
            return f"'{path}'"

        if file_type == "csv":
            return f"read_csv('{path}', delim = '{separator}', header = {have_header})"
        
        if file_type == "parquet":
            return f"read_parquet('{path}')"
        
        raise ValueError(f"Unsupported file type: {file_type}")        
        

    def read(self, 
            path: str, 
            have_header: Optional[bool] = None,
            delimiter: Optional[str] = None,
            file_type: Optional[str] = None,
            columns: Optional[List[str]] = None, 
            condition: Optional[str] = None,
            order_by: Optional[List[str]] = None, 
            offset: Optional[int] = None,
            limit: Optional[int] = None
    ) -> Tuple[DuckDBPyRelation, int]:
        

        path = self.wrap_path_in_reader_function(path, file_type, have_header, delimiter)

        sql = (
            StringBuilder()
            .append(f"SELECT {', '.join(columns or ['*'])}")
            .append(f"FROM {path}")
            .append(f"WHERE {condition or '1 = 1'}")
        )

        if order_by:
            sql.append(f"ORDER BY {', '.join(order_by)}")

        if limit:
            sql.append(f"LIMIT {limit}")

        if offset:
            sql.append(f"OFFSET {offset}")

        sql = sql.build()
        result_set = self.connection.sql(sql)

        return result_set, result_set.__len__()
    
