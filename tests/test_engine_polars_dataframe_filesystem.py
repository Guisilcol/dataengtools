import pytest
import polars as pl
import tempfile
import os
from unittest.mock import Mock, patch
from pathlib import Path

from dataengtools.core.interfaces.engine_layer.filesystem import FileMetadata
from dataengtools.core.interfaces.integration_layer.filesystem_handler import FilesystemHandler
from dataengtools.engines.polars.dataframe_filesystem import PolarsFilesystem 

class TestPolarsFilesystem:
    @pytest.fixture
    def handler_mock(self):
        return Mock(spec=FilesystemHandler)
        
    @pytest.fixture
    def filesystem(self, handler_mock):
        return PolarsFilesystem(handler_mock)
        
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir
            
    def test_get_files(self, filesystem, handler_mock):
        expected_files = ["file1.csv", "file2.csv"]
        handler_mock.get_files.return_value = expected_files
        
        result = filesystem.get_files("prefix")
        
        handler_mock.get_files.assert_called_once_with("prefix")
        assert result == expected_files
        
    def test_delete_files(self, filesystem, handler_mock):
        files = ["file1.csv", "file2.csv"]
        
        filesystem.delete_files(files)
        
        handler_mock.delete_files.assert_called_once_with(files)
        
    def test_read_files_csv(self, filesystem, handler_mock, temp_dir):
        # Preparar dados de teste
        data = {"col1": [1, 2], "col2": ["a", "b"]}
        df = pl.DataFrame(data)
        csv_path = Path(temp_dir) / "test.csv"
        df.write_csv(csv_path)
        
        # Configurar mock
        handler_mock.get_files.return_value = [str(csv_path)]
        handler_mock.open_file = lambda *_: open(csv_path, 'rb')
        
        # Executar teste
        result = filesystem.read_files("prefix", "csv")
        
        # Verificações
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (2, 2)
        assert result.columns == ["col1", "col2"]
        
    def test_read_files_parquet(self, filesystem, handler_mock, temp_dir):
        # Preparar dados de teste
        data = {"col1": [1, 2], "col2": ["a", "b"]}
        df = pl.DataFrame(data)
        parquet_path = Path(temp_dir) / "test.parquet"
        df.write_parquet(parquet_path)
        
        # Configurar mock
        handler_mock.get_files.return_value = [str(parquet_path)]
        handler_mock.open_file = lambda *_: open(parquet_path, 'rb')
        
        # Executar teste
        result = filesystem.read_files("prefix", "parquet")
        
        # Verificações
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (2, 2)
        assert result.columns == ["col1", "col2"]
        
    def test_read_files_with_metadata(self, filesystem, handler_mock, temp_dir):
        # Preparar dados de teste
        data = {"col1": [1, 2], "col2": ["a", "b"], "col3": [True, False]}
        df = pl.DataFrame(data)
        csv_path = Path(temp_dir) / "test.csv"
        df.write_csv(csv_path)
        
        # Configurar mock
        handler_mock.get_files.return_value = [str(csv_path)]
        handler_mock.open_file = lambda *_: open(csv_path, 'rb')
        
        # Metadata para seleção de colunas
        metadata: FileMetadata = {
            "columns": ["col1", "col2"],
            "separator": ",",
            "header": True,
            "encoding": "utf-8"
        }
        
        # Executar teste
        result = filesystem.read_files("prefix", "csv", metadata)
        
        # Verificações
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (2, 2)
        assert result.columns == ["col1", "col2"]
        
    def test_read_files_empty_result(self, filesystem, handler_mock):
        # Configurar mock para retornar lista vazia de arquivos
        handler_mock.get_files.return_value = []
        
        # Executar teste
        result = filesystem.read_files("prefix", "csv")
        
        # Verificações
        assert isinstance(result, pl.DataFrame)
        assert result.shape == (0, 0)
        
    def test_read_files_invalid_filetype(self, filesystem, handler_mock):
        with pytest.raises(ValueError) as exc_info:
            handler_mock.get_files.return_value = ['file1.csv']
            filesystem.read_files("prefix", "invalid")
            
        assert str(exc_info.value) == "Unsupported file type: invalid"
