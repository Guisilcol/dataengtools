from dataengtools.core.interfaces.io.writer import Writer, WriterOptions
from dataengtools.io.duckdb_io.string_builder import StringBuilder
from dataengtools.core.interfaces.integration_layer.sql_configurator import SQLProviderConfigurator
from duckdb import DuckDBPyRelation, DuckDBPyConnection

class DuckDBWriter(Writer[DuckDBPyRelation]):
    def __init__(self, connection: DuckDBPyConnection, sql_configurator: SQLProviderConfigurator[DuckDBPyConnection]):
        self.connection = connection
        self.sql_configurator = sql_configurator
        self.sql_configurator.configure_connection(connection)

    
    def write(self, data: DuckDBPyRelation, path: str, writer_options: WriterOptions = ...):
        self.connection.register('source', data)
        
        columns = ", ".join(writer_options.get('columns') or ['*'])
        filetype = writer_options.get('file_type') or 'parquet'
        partition_by = writer_options.get('partition_by')
        mode = writer_options.get('mode') or 'OVERWRITE'

        if not partition_by:
            path = path + '/data.' + filetype.lower()

        sql = (
            StringBuilder()
            .append('COPY')
            .append(f'(SELECT {columns} FROM source)')
            .append(f'TO \'{path}\'')
            .append(f'(FORMAT {filetype}')
            .append(f', PARTITION_BY ({", ".join(partition_by)})' if partition_by else '')
            .append(f', {mode});')
            .build()
        )

        self.connection.sql(sql)
        self.connection.unregister('source')
    
