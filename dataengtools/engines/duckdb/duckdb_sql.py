from dataclasses import dataclass
from typing import Dict, Optional
import logging

import duckdb
from duckdb import DuckDBPyConnection
from polars import DataFrame
from dataengtools.core.interfaces.engine_layer.sql import SQLEngine
from dataengtools.core.interfaces.integration_layer.catalog_metadata import (
    DatabaseMetadataRetriever,
    TableMetadataRetriver,
    TableMetadata
)
from dataengtools.utils.logger import Logger

LOGGER = Logger.get_instance()


@dataclass
class FileConfig:
    """Configuration for different file types"""
    extension: str
    query_template: str
    is_hive_partitioning: bool = False

class DuckDBEngine(SQLEngine[DuckDBPyConnection, DataFrame]):
    """DuckDB engine implementation for handling database operations"""
    
    FILE_CONFIGS = {
        'csv': FileConfig(
            extension='csv',
            query_template=(
                "SELECT * FROM read_csv("
                    "'{location}/**/*.{extension}', "
                    "delim = '{separator}', "
                    "header = {has_header}, "
                    "hive_partitioning = {is_hive_partitioning}"
                ")"
            )
        ),
        'parquet': FileConfig(
            extension='parquet',
            query_template="SELECT * FROM read_parquet('{location}/**/*.{extension}')"
        )
    }

    def __init__(
        self,
        connection: DuckDBPyConnection,
        database_metadata_retriever: DatabaseMetadataRetriever,
        table_metadata_retriever: TableMetadataRetriver
    ):
        """Initialize DuckDB engine with necessary components"""
        self._connection = connection
        self._database_metadata_retriever = database_metadata_retriever
        self._table_metadata_retriever = table_metadata_retriever

        self._configure_connection_to_run_in_aws()

    def _configure_connection_to_run_in_aws(self) -> None:
        self._connection.sql("SET home_directory='/tmp';")
        self._connection.sql("SET secret_directory='/tmp/my_secrets_dir';")
        self._connection.sql("SET extension_directory='/tmp/my_extension_dir';")
        self._connection.sql('CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN);')

    def get_connection(self) -> DuckDBPyConnection:
        """Get or create a DuckDB connection"""
        return self._connection

    def _create_schema(self, database_name: str) -> None:
        """Create a database schema if it doesn't exist"""
        create_stmt = f'CREATE SCHEMA IF NOT EXISTS "{database_name}"'
        LOGGER.info(f"creating schema using the following sql: {create_stmt}")
        self._connection.sql(create_stmt)

    def _get_select_statement(self, table_metadata: TableMetadata) -> Optional[str]:
        """Generate SELECT statement based on file type"""
        file_config = self.FILE_CONFIGS.get(table_metadata.files_extension)
        if not file_config:
            return None

        return file_config.query_template.format(
            location=table_metadata.location,
            extension=table_metadata.files_extension,
            separator=getattr(table_metadata, 'columns_separator', ','),
            has_header=getattr(table_metadata, 'files_have_header', True),
            is_hive_partitioning=len(table_metadata.partition_columns) > 0
        )

    def _create_view(self, database_name: str, table_metadata: TableMetadata) -> None:
        """Create a view for the given table metadata"""
        select_stmt = self._get_select_statement(table_metadata)
        if not select_stmt:
            LOGGER.warning(f"unsupported file extension: {table_metadata.files_extension}. Skipping SQL to view creation for {table_metadata.database}.{table_metadata.table}")
            return

        create_view_stmt = (
            f'CREATE VIEW IF NOT EXISTS "{database_name}"."{table_metadata.table}" '
            f'AS {select_stmt}'
        )

        LOGGER.info(f"creating view using the following sql: {create_view_stmt}")
        self._connection.sql(create_view_stmt)

    def configure_metadata(self, **kwargs) -> None:
        """Configure database metadata and create necessary schemas and views"""
        databases = self._database_metadata_retriever.get_all_databases()
        
        for database in databases:
            self._create_schema(database.name)
            
            for table_metadata in self._table_metadata_retriever.get_all_tables(database.name):
                self._create_view(database.name, table_metadata)

    def execute(self, query: str, params: Dict = None) -> None:
        """Execute a query with optional parameters"""
        params = params or {}
        self._connection.sql(query, params=params)

    def execute_and_fetch(self, query: str, params: Dict = None) -> DataFrame:
        """Execute a query and return results as a DataFrame"""
        params = params or {}
        return self._connection.sql(query, params=params).pl()