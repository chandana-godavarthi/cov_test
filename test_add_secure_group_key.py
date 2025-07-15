import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import LongType
from pyspark.sql import DataFrame
import common  

def test_add_secure_group_key_with_contract_match():
    mock_df = MagicMock(spec=DataFrame)
    mock_df.withColumn.return_value = "final_df"

    contract_id = "C123"
    schema = "public"
    jdbc_url = "jdbc_url"
    dbname = "testdb"
    user = "testuser"
    pwd = "testpwd"

    # Mocks for query results
    contract_sgk = [{"secure_group_key": 42}]
    default_sgk = [{"secure_group_key": 99}]

    with patch("common.read_query_from_postgres") as mock_read_query, \
         patch("common.lit") as mock_lit:

        mock_read_query.side_effect = [MagicMock(collect=MagicMock(return_value=contract_sgk)),
                                       MagicMock(collect=MagicMock(return_value=default_sgk))]

        mock_lit.return_value.cast.return_value = "lit_column"

        result = common.add_secure_group_key(
            mock_df, contract_id, schema, None, jdbc_url, dbname, user, pwd
        )

        # Assert contract query used first
        assert result == "final_df"
        mock_df.withColumn.assert_called_once_with("secure_group_key", "lit_column")
        mock_lit.assert_called_once_with(42)
        mock_lit.return_value.cast.assert_called_once_with(LongType())

def test_add_secure_group_key_with_no_contract_match():
    mock_df = MagicMock(spec=DataFrame)
    mock_df.withColumn.return_value = "final_df"

    contract_id = "C999"
    schema = "public"
    jdbc_url = "jdbc_url"
    dbname = "testdb"
    user = "testuser"
    pwd = "testpwd"

    # Contract SGK missing (None), default SGK exists
    contract_sgk = [{"secure_group_key": None}]
    default_sgk = [{"secure_group_key": 99}]

    with patch("common.read_query_from_postgres") as mock_read_query, \
         patch("common.lit") as mock_lit:

        mock_read_query.side_effect = [MagicMock(collect=MagicMock(return_value=contract_sgk)),
                                       MagicMock(collect=MagicMock(return_value=default_sgk))]

        mock_lit.return_value.cast.return_value = "lit_column"

        result = common.add_secure_group_key(
            mock_df, contract_id, schema, None, jdbc_url, dbname, user, pwd
        )

        assert result == "final_df"
        mock_df.withColumn.assert_called_once_with("secure_group_key", "lit_column")
        mock_lit.assert_called_once_with(99)
        mock_lit.return_value.cast.assert_called_once_with(LongType())

def test_add_secure_group_key_empty_contract_result():
    mock_df = MagicMock(spec=DataFrame)
    mock_df.withColumn.return_value = "final_df"

    contract_id = "C999"
    schema = "public"
    jdbc_url = "jdbc_url"
    dbname = "testdb"
    user = "testuser"
    pwd = "testpwd"

    # Contract SGK empty list, default SGK exists
    contract_sgk = []
    default_sgk = [{"secure_group_key": 88}]

    with patch("common.read_query_from_postgres") as mock_read_query, \
         patch("common.lit") as mock_lit:

        mock_read_query.side_effect = [MagicMock(collect=MagicMock(return_value=contract_sgk)),
                                       MagicMock(collect=MagicMock(return_value=default_sgk))]

        mock_lit.return_value.cast.return_value = "lit_column"

        result = common.add_secure_group_key(
            mock_df, contract_id, schema, None, jdbc_url, dbname, user, pwd
        )

        assert result == "final_df"
        mock_df.withColumn.assert_called_once_with("secure_group_key", "lit_column")
        mock_lit.assert_called_once_with(88)
        mock_lit.return_value.cast.assert_called_once_with(LongType())
