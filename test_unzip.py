import pytest
from unittest.mock import patch, MagicMock
import zipfile
import common  # adjust if your module has a different import path

def test_unzip_success_standard_zip():
    file_name = "data.zip"
    vol_path = "/mnt/data/"
    expected_folder = "data"

    with patch("common.zipfile.ZipFile") as mock_zipfile, \
         patch("builtins.print") as mock_print:
        
        mock_context = mock_zipfile.return_value.__enter__.return_value

        common.unzip(file_name, "catalog", vol_path)

        mock_zipfile.assert_called_once_with(f"{vol_path}{file_name}", 'r')
        mock_context.extractall.assert_called_once_with(f"{vol_path}{expected_folder}")
        mock_print.assert_any_call(f'Started unzipping the file: {vol_path}{file_name}')
        mock_print.assert_any_call("Unzipping finished")

def test_unzip_uppercase_zip():
    file_name = "data.ZIP"
    vol_path = "/mnt/data/"
    expected_folder = "data"

    with patch("common.zipfile.ZipFile") as mock_zipfile, \
         patch("builtins.print"):
        
        mock_context = mock_zipfile.return_value.__enter__.return_value

        common.unzip(file_name, "catalog", vol_path)

        mock_zipfile.assert_called_once_with(f"{vol_path}{file_name}", 'r')
        mock_context.extractall.assert_called_once_with(f"{vol_path}{expected_folder}")

def test_unzip_without_zip_extension():
    file_name = "datafile"
    vol_path = "/mnt/data/"
    expected_folder = "datafile"

    with patch("common.zipfile.ZipFile") as mock_zipfile, \
         patch("builtins.print"):
        
        mock_context = mock_zipfile.return_value.__enter__.return_value

        common.unzip(file_name, "catalog", vol_path)

        mock_zipfile.assert_called_once_with(f"{vol_path}{file_name}", 'r')
        mock_context.extractall.assert_called_once_with(f"{vol_path}{expected_folder}")

def test_unzip_file_not_found():
    file_name = "missing.zip"
    vol_path = "/mnt/data/"

    with patch("common.zipfile.ZipFile", side_effect=FileNotFoundError("File not found")), \
         patch("builtins.print") as mock_print:

        with pytest.raises(FileNotFoundError, match="File not found"):
            common.unzip(file_name, "catalog", vol_path)

        mock_print.assert_any_call(f'Started unzipping the file: {vol_path}{file_name}')

def test_unzip_bad_zip_file():
    file_name = "corrupt.zip"
    vol_path = "/mnt/data/"

    with patch("common.zipfile.ZipFile", side_effect=zipfile.BadZipFile("Bad zip")), \
         patch("builtins.print") as mock_print:

        with pytest.raises(zipfile.BadZipFile, match="Bad zip"):
            common.unzip(file_name, "catalog", vol_path)

        mock_print.assert_any_call(f'Started unzipping the file: {vol_path}{file_name}')
