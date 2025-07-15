import pytest
from unittest.mock import MagicMock, call
import common  # adjust as needed for your module

def test_work_to_arch_file_found():
    mock_dbutils = MagicMock()

    # Mock file list containing the target file
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name='file1', name_attr='file1.csv'),
        MagicMock(name='file2', name_attr='target_file.csv')
    ]
    # Manually set .name for each mock file
    mock_dbutils.fs.ls.return_value[0].name = 'file1.csv'
    mock_dbutils.fs.ls.return_value[1].name = 'target_file.csv'

    result = common.work_to_arch("runid", "cntrtid", "target_file.csv", mock_dbutils)

    assert result is True
    mock_dbutils.fs.mv.assert_called_once_with(
        '/mnt/tp-source-data/WORK/target_file.csv',
        '/mnt/tp-source-data/ARCH/target_file.csv'
    )

def test_work_to_arch_file_not_found():
    mock_dbutils = MagicMock()

    # Mock file list without the target file
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name='file1', name_attr='file1.csv'),
        MagicMock(name='file2', name_attr='another_file.csv')
    ]
    mock_dbutils.fs.ls.return_value[0].name = 'file1.csv'
    mock_dbutils.fs.ls.return_value[1].name = 'another_file.csv'

    result = common.work_to_arch("runid", "cntrtid", "missing_file.csv", mock_dbutils)

    assert result is False
    mock_dbutils.fs.mv.assert_not_called()

def test_work_to_arch_empty_directory():
    mock_dbutils = MagicMock()

    # Empty file list
    mock_dbutils.fs.ls.return_value = []

    result = common.work_to_arch("runid", "cntrtid", "any_file.csv", mock_dbutils)

    assert result is False
    mock_dbutils.fs.mv.assert_not_called()

def test_work_to_arch_multiple_files_only_first_match_moves():
    mock_dbutils = MagicMock()

    # Multiple files, target one exists
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name='file1', name_attr='file1.csv'),
        MagicMock(name='target', name_attr='target_file.csv'),
        MagicMock(name='file2', name_attr='another_file.csv')
    ]
    for idx, fname in enumerate(['file1.csv', 'target_file.csv', 'another_file.csv']):
        mock_dbutils.fs.ls.return_value[idx].name = fname

    result = common.work_to_arch("runid", "cntrtid", "target_file.csv", mock_dbutils)

    assert result is True
    mock_dbutils.fs.mv.assert_called_once_with(
        '/mnt/tp-source-data/WORK/target_file.csv',
        '/mnt/tp-source-data/ARCH/target_file.csv'
    )

