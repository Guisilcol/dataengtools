import pytest
from unittest.mock import Mock, patch
from io import TextIOWrapper
from s3fs import S3FileSystem
from dataengtools.providers.aws.s3_filesystem_handler import AWSS3FilesystemHandler

@pytest.fixture
def mock_s3fs():
    """Fixture para criar um mock do S3FileSystem"""
    return Mock(spec=S3FileSystem)

@pytest.fixture
def s3_handler(mock_s3fs):
    """Fixture para criar uma instância do AWSS3FilesystemHandler com S3FileSystem mockado"""
    return AWSS3FilesystemHandler(fs=mock_s3fs)

class TestAWSS3FilesystemHandler:

    def test_normalize_s3_path(self, s3_handler):
        """Testa normalização de paths S3"""
        # Cenário 1: Path já normalizado
        assert s3_handler._normalize_s3_path("s3://bucket/path") == "s3://bucket/path"
        
        # Cenário 2: Path sem prefixo s3://
        assert s3_handler._normalize_s3_path("bucket/path") == "s3://bucket/path"
        
        # Cenário 3: Path vazio
        assert s3_handler._normalize_s3_path("") == "s3://"

    def test_get_files_basic(self, s3_handler, mock_s3fs):
        """Testa listagem básica de arquivos"""
        mock_s3fs.ls.return_value = ["s3://bucket/file1.txt", "s3://bucket/file2.csv"]
        
        files = s3_handler.get_files("bucket")
        
        mock_s3fs.ls.assert_called_once_with("s3://bucket", detail=False)
        assert len(files) == 2
        assert "s3://bucket/file1.txt" in files
        assert "s3://bucket/file2.csv" in files

    def test_get_files_with_pattern(self, s3_handler, mock_s3fs):
        """Testa listagem de arquivos com filtro de padrão"""
        mock_s3fs.ls.return_value = ["s3://bucket/file1.txt", "s3://bucket/file2.csv"]
        
        files = s3_handler.get_files(
            "bucket",
            additional_configs={"pattern": r"\.csv$"}
        )
        
        assert len(files) == 1
        assert "s3://bucket/file2.csv" in files

    def test_get_files_with_detail(self, s3_handler, mock_s3fs):
        """Testa listagem de arquivos com informações detalhadas"""
        mock_s3fs.ls.return_value = [
            {"name": "s3://bucket/file1.txt", "type": "file"},
            {"name": "s3://bucket/dir", "type": "directory"},
            {"name": "s3://bucket/file2.csv", "type": "file"}
        ]
        
        files = s3_handler.get_files(
            "bucket",
            additional_configs={"detail": True}
        )
        
        assert len(files) == 2
        assert "s3://bucket/file1.txt" in files
        assert "s3://bucket/file2.csv" in files

    def test_get_files_error(self, s3_handler, mock_s3fs):
        """Testa erro na listagem de arquivos"""
        mock_s3fs.ls.side_effect = Exception("S3 Error")
        
        with pytest.raises(Exception) as exc_info:
            s3_handler.get_files("bucket/test")
        
        assert "Error listing files from S3" in str(exc_info.value)

    def test_delete_files_basic(self, s3_handler, mock_s3fs):
        """Testa deleção básica de arquivos"""
        files = ["bucket/file1.txt", "bucket/file2.csv"]
        s3_handler.delete_files(files)
        
        mock_s3fs.rm.assert_called_once_with(
            ["s3://bucket/file1.txt", "s3://bucket/file2.csv"],
            recursive=False
        )

    def test_delete_files_empty_list(self, s3_handler, mock_s3fs):
        """Testa deleção com lista vazia"""
        s3_handler.delete_files([])
        mock_s3fs.rm.assert_not_called()

    def test_delete_files_with_batch(self, s3_handler, mock_s3fs):
        """Testa deleção em lotes"""
        files = [f"bucket/file{i}.txt" for i in range(1500)]  # Cria 1500 arquivos
        s3_handler.delete_files(files, additional_configs={"batch_size": 1000})
        
        assert mock_s3fs.rm.call_count == 2
        # Verifica primeira chamada com 1000 arquivos
        first_call_args = mock_s3fs.rm.call_args_list[0][0][0]
        assert len(first_call_args) == 1000
        # Verifica segunda chamada com 500 arquivos restantes
        second_call_args = mock_s3fs.rm.call_args_list[1][0][0]
        assert len(second_call_args) == 500

    def test_delete_files_error(self, s3_handler, mock_s3fs):
        """Testa erro na deleção de arquivos"""
        mock_s3fs.rm.side_effect = Exception("S3 Error")
        
        with pytest.raises(Exception) as exc_info:
            s3_handler.delete_files(["bucket/file1.txt"])
        
        assert "Error deleting files from S3" in str(exc_info.value)

    def test_open_file_basic(self, s3_handler, mock_s3fs):
        """Testa abertura básica de arquivo"""
        mock_file = Mock(spec=TextIOWrapper)
        mock_s3fs.open.return_value = mock_file
        
        result = s3_handler.open_file("bucket/file.txt", "r")
        
        assert result == mock_file
        mock_s3fs.open.assert_called_once_with(
            "s3://bucket/file.txt",
            mode="r",
            encoding=None,
            compression=None,
            buffer_size=None
        )

    def test_open_file_with_configs(self, s3_handler, mock_s3fs):
        """Testa abertura de arquivo com configurações adicionais"""
        mock_file = Mock(spec=TextIOWrapper)
        mock_s3fs.open.return_value = mock_file
        
        result = s3_handler.open_file(
            "bucket/file.txt",
            "r",
            additional_configs={
                "encoding": "utf-8",
                "compression": "gzip",
                "buffer_size": 1024
            }
        )
        
        assert result == mock_file
        mock_s3fs.open.assert_called_once_with(
            "s3://bucket/file.txt",
            mode="r",
            encoding="utf-8",
            compression="gzip",
            buffer_size=1024
        )

    def test_open_file_error(self, s3_handler, mock_s3fs):
        """Testa erro na abertura de arquivo"""
        mock_s3fs.open.side_effect = Exception("S3 Error")
        
        with pytest.raises(Exception) as exc_info:
            s3_handler.open_file("bucket/file.txt", "r")
        
        assert "Error opening file from S3" in str(exc_info.value)