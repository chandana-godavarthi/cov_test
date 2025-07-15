import pytest
from unittest.mock import patch, MagicMock
from common import load_cntrt_lkp

def test_load_cntrt_lkp_calls_read_query_with_expected_query():
    mock_spark = MagicMock()
    cntrt_id = 2001
    postgres_schema = "public"
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "testdb"
    refDBuser = "user"
    refDBpwd = "password"
    expected_df = MagicMock()

    expected_query = f'''SELECT * FROM {postgres_schema}.mm_cntrt_lkp WHERE cntrt_id= {cntrt_id}'''

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_lkp(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )

        assert result_df == expected_df


def test_load_cntrt_lkp_with_different_cntrt_id_and_schema():
    mock_spark = MagicMock()
    cntrt_id = 3333
    postgres_schema = "schema_xyz"
    refDBjdbcURL = "jdbc:postgresql://anotherhost:5432/anotherdb"
    refDBname = "anotherdb"
    refDBuser = "anotheruser"
    refDBpwd = "anotherpassword"
    expected_df = MagicMock()

    expected_query = f'''SELECT * FROM {postgres_schema}.mm_cntrt_lkp WHERE cntrt_id= {cntrt_id}'''

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_lkp(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )

        assert result_df == expected_df


def test_load_cntrt_lkp_with_empty_schema():
    mock_spark = MagicMock()
    cntrt_id = 9999
    postgres_schema = ""
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pass"
    expected_df = MagicMock()

    expected_query = f'''SELECT * FROM .mm_cntrt_lkp WHERE cntrt_id= {cntrt_id}'''

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_lkp(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )

        assert result_df == expected_df


def test_load_cntrt_lkp_return_type():
    mock_spark = MagicMock()
    cntrt_id = 1234
    postgres_schema = "my_schema"
    refDBjdbcURL = "url"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pass"
    expected_df = MagicMock()

    with patch("common.read_query_from_postgres", return_value=expected_df):
        result_df = load_cntrt_lkp(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)
        assert result_df == expected_df


def test_load_cntrt_lkp_raises_if_read_query_fails():
    mock_spark = MagicMock()
    cntrt_id = 7777
    postgres_schema = "test_schema"
    refDBjdbcURL = "url"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pwd"

    with patch("common.read_query_from_postgres", side_effect=Exception("DB failure")):
        with pytest.raises(Exception, match="DB failure"):
            load_cntrt_lkp(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)
