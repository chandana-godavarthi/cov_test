import pytest
from unittest.mock import patch, MagicMock
from common import load_cntrt_dlmtr_lkp

def test_load_cntrt_dlmtr_lkp_calls_read_query_with_expected_query():
    mock_spark = MagicMock()
    cntrt_id = 200
    dmnsn_name = "Country"
    postgres_schema = "public"
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pwd"
    expected_df = MagicMock()

    expected_query = f"""SELECT dt.dlmtr_val FROM {postgres_schema}.mm_cntrt_file_lkp ct join {postgres_schema}.mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id WHERE cntrt_id={cntrt_id} AND dmnsn_name='{dmnsn_name}' """

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )
        assert result_df == expected_df


def test_load_cntrt_dlmtr_lkp_with_different_values():
    mock_spark = MagicMock()
    cntrt_id = 888
    dmnsn_name = "Product"
    postgres_schema = "contracts"
    refDBjdbcURL = "jdbc:postgresql://anotherhost:5432/anotherdb"
    refDBname = "anotherdb"
    refDBuser = "anotheruser"
    refDBpwd = "anotherpwd"
    expected_df = MagicMock()

    expected_query = f"""SELECT dt.dlmtr_val FROM {postgres_schema}.mm_cntrt_file_lkp ct join {postgres_schema}.mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id WHERE cntrt_id={cntrt_id} AND dmnsn_name='{dmnsn_name}' """

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )
        assert result_df == expected_df


def test_load_cntrt_dlmtr_lkp_with_empty_schema():
    mock_spark = MagicMock()
    cntrt_id = 999
    dmnsn_name = "Location"
    postgres_schema = ""
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pwd"
    expected_df = MagicMock()

    expected_query = f"""SELECT dt.dlmtr_val FROM .mm_cntrt_file_lkp ct join .mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id WHERE cntrt_id={cntrt_id} AND dmnsn_name='{dmnsn_name}' """

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )
        assert result_df == expected_df


def test_load_cntrt_dlmtr_lkp_with_empty_dmnsn_name():
    mock_spark = MagicMock()
    cntrt_id = 777
    dmnsn_name = ""
    postgres_schema = "test_schema"
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pwd"
    expected_df = MagicMock()

    expected_query = f"""SELECT dt.dlmtr_val FROM {postgres_schema}.mm_cntrt_file_lkp ct join {postgres_schema}.mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id WHERE cntrt_id={cntrt_id} AND dmnsn_name='{dmnsn_name}' """

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )
        assert result_df == expected_df


def test_load_cntrt_dlmtr_lkp_raises_if_read_query_fails():
    mock_spark = MagicMock()
    cntrt_id = 404
    dmnsn_name = "ErrorTest"
    postgres_schema = "test_schema"
    refDBjdbcURL = "url"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pwd"

    with patch("common.read_query_from_postgres", side_effect=Exception("DB error")):
        with pytest.raises(Exception, match="DB error"):
            load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)
