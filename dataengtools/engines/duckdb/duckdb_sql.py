

import duckdb.typing
from dataengtools.core.interfaces.engine_layer.sql import SQLEngine
from dataengtools.core.interfaces.integration_layer.catalog_metadata import DatabaseMetadataRetriever, TableMetadataRetriver

import duckdb
from duckdb import DuckDBPyConnection
from polars import DataFrame

class DuckDBEngine(SQLEngine[DuckDBPyConnection, DataFrame]):
    def __init__(self, connection: DuckDBPyConnection, database_metadata_retriever: DatabaseMetadataRetriever, table_metadata_retriever: TableMetadataRetriver):
        self.connection = connection
        self.database_metadata_retriever = database_metadata_retriever
        self.table_metadata_retriever = table_metadata_retriever


    def get_connection(self) -> DuckDBPyConnection:
        self.connection = duckdb.connect(self.connection)
        return self.connection

    def configure_metadata(self, **kwargs) -> None:
        databases = self.database_metadata_retriever.get_all_databases()

        for database in databases:
            create_database_stmt = f'CREATE SCHEMA IF NOT EXISTS "{database.name}"'
            print('Creating schema:', create_database_stmt)
            self.connection.execute(create_database_stmt)


            for table_metadata in self.table_metadata_retriever.get_all_tables(database.name):
                if table_metadata.files_extension == 'csv':
                    select_stmt = f"""
                        SELECT * FROM read_csv(
                        '{table_metadata.location}/**/*.{table_metadata.files_extension}',
                        delim = '{table_metadata.columns_separator}',
                        header = {table_metadata.files_have_header}
                        )

                    """
                
                if table_metadata.files_extension == 'parquet':
                    select_stmt = f"""
                        SELECT * FROM read_parquet(
                        '{table_metadata.location}/**/*.{table_metadata.files_extension}'
                        )
                    """

                create_view_stmt = f'CREATE VIEW IF NOT EXISTS "{database.name}".{table_metadata.table} AS {select_stmt}'
                print('Creating view:', create_view_stmt)
                self.connection.execute(create_view_stmt)

    def execute(self, query: str, params: dict = {}) -> None:
        self.connection.execute(query, params)

    def execute_and_fetch(self, query: str, params: dict = {}) -> DataFrame:
        return self.connection.execute(query, params).pl()