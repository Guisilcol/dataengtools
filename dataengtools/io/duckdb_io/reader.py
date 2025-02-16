from dataengtools.core.interfaces.io.reader import Reader, ReaderOptions
from dataengtools.io.duckdb_io.string_builder import StringBuilder
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from typing import Optional

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
        
        if file_type == 'txt':
            unique_separator = separator or chr(31)
            have_header = have_header or False
            return f"read_csv('{path}', delim = '{unique_separator}', header = {have_header}, columns = {{'value': 'VARCHAR'}})"

        if file_type == "csv":
            return f"read_csv('{path}', delim = '{separator}', header = {have_header})"
        
        if file_type == "parquet":
            return f"read_parquet('{path}')"
        
        raise ValueError(f"Unsupported file type: {file_type}")        
        

    def read(self, 
            path: str, 
            reader_options: ReaderOptions = {}
    ) -> DuckDBPyRelation:
        

        path = self.wrap_path_in_reader_function(
            path, 
            reader_options.get('file_type'),
            reader_options.get('has_header'),
            reader_options.get('separator')
        )

        sql = (
            StringBuilder()
            .append(f"SELECT {', '.join(reader_options.get('columns') or ['*'])}")
            .append(f"FROM {path}")
            .append(f"WHERE {reader_options.get('condition') or '1 = 1'}")
        )

        if (order_by := reader_options.get('order_by')):
            sql.append(f"ORDER BY {', '.join(order_by)}")

        if (limit := reader_options.get('limit')):
            sql.append(f"LIMIT {limit}")

        if (offset := reader_options.get('offset')):
            sql.append(f"OFFSET {offset}")

        return self.connection.sql(sql.build())
    
