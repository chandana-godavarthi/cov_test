import pytest
from unittest.mock import patch, MagicMock
from common import load_cntrt_col_assign

def test_load_cntrt_col_assign_calls_read_query_with_expected_query():
    mock_spark = MagicMock()
    cntrt_id = 1001
    postgres_schema = "public"
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "testdb"
    refDBuser = "user"
    refDBpwd = "password"
    expected_df = MagicMock()

    expected_query = f'''select * from {postgres_schema}.mm_col_asign_lkp where cntrt_id = {cntrt_id}'''

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_col_assign(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        # Assert read_query_from_postgres called correctly
        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )

        # Assert result matches mocked return value
        assert result_df == expected_df

def test_load_cntrt_col_assign_with_different_cntrt_id_and_schema():
    mock_spark = MagicMock()
    cntrt_id = 5555
    postgres_schema = "schema_xyz"
    refDBjdbcURL = "jdbc:postgresql://anotherhost:5432/anotherdb"
    refDBname = "anotherdb"
    refDBuser = "anotheruser"
    refDBpwd = "anotherpassword"
    expected_df = MagicMock()

    expected_query = f'''select * from {postgres_schema}.mm_col_asign_lkp where cntrt_id = {cntrt_id}'''

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_col_assign(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )

        assert result_df == expected_df

def test_load_cntrt_col_assign_with_empty_schema():
    mock_spark = MagicMock()
    cntrt_id = 7777
    postgres_schema = ""  # empty schema
    refDBjdbcURL = "jdbc:postgresql://host:5432/db"
    refDBname = "db"
    refDBuser = "user"
    refDBpwd = "pass"
    expected_df = MagicMock()

    expected_query = f'''select * from .mm_col_asign_lkp where cntrt_id = {cntrt_id}'''

    with patch("common.read_query_from_postgres", return_value=expected_df) as mock_read_query:
        result_df = load_cntrt_col_assign(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_read_query.assert_called_once_with(
            expected_query,
            mock_spark,
            refDBjdbcURL,
            refDBname,
            refDBuser,
            refDBpwd
        )

        assert result_df == expected_df

def test_load_cntrt_col_assign_return_type():
    """Test that it returns whatever read_query_from_postgres returns."""
    mock_spark = MagicMock()
    cntrt_id = 8888
    postgres_schema = "test_schema"
    refDBjdbcURL = "url"
    refDBname = "dbname"
    refDBuser = "user"
    refDBpwd = "pwd"
    expected_df = MagicMock()

    with patch("common.read_query_from_postgres", return_value=expected_df):
        result_df = load_cntrt_col_assign(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)
        assert result_df == expected_df
def test_load_cntrt_col_assign_raises_if_read_query_fails():
    mock_spark = MagicMock()
    cntrt_id = 1001
    postgres_schema = "public"
    refDBjdbcURL = "url"
    refDBname = "dbname"
    refDBuser = "user"
    refDBpwd = "pwd"

    with patch("common.read_query_from_postgres", side_effect=Exception("DB error")):
        with pytest.raises(Exception, match="DB error"):
            load_cntrt_col_assign(cntrt_id, postgres_schema, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd)
