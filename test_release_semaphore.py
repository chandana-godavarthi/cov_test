import pytest
from unittest.mock import MagicMock, call
import common  # assuming your function is in common.py

def test_release_semaphore_calls_spark_sql_correctly():
    mock_spark = MagicMock()
    catalog_name = "test_catalog"
    run_id = "test_run_id"
    lock_path = "/test/path"

    common.release_semaphore(catalog_name, run_id, lock_path, mock_spark)

    expected_sql = (
        f"DELETE FROM {catalog_name}.internal_tp.tp_run_lock_plc "
        "WHERE run_id = :run_id AND lock_path IN (':lock_path')"
    )
    expected_params = {"run_id": run_id, "lock_path": lock_path}

    mock_spark.sql.assert_called_once_with(expected_sql, expected_params)


def test_release_semaphore_propagates_exception():
    mock_spark = MagicMock()
    catalog_name = "test_catalog"
    run_id = "test_run_id"
    lock_path = "/test/path"

    mock_spark.sql.side_effect = Exception("Spark SQL error")

    with pytest.raises(Exception, match="Spark SQL error"):
        common.release_semaphore(catalog_name, run_id, lock_path, mock_spark)
