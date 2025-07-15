import pytest
from unittest.mock import patch, MagicMock
import common  # adjust to your module name


def test_update_to_postgres_happy_path():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("common.psycopg2.connect", return_value=mock_conn) as mock_connect:
        query = "UPDATE table SET col = 'val'"
        refDBjdbcURL = "dummy_url"
        refDBname = "testdb"
        refDBuser = "testuser"
        refDBpwd = "testpwd"

        common.update_to_postgres(query, None, refDBjdbcURL, refDBname, refDBuser, refDBpwd)

        mock_connect.assert_called_once_with(
            dbname=refDBname,
            user=refDBuser,
            password=refDBpwd,
            host="psql-pg-flex-tpconsole-dev-1.postgres.database.azure.com",
            sslmode='require'
        )
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query)
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


def test_update_to_postgres_connection_failure():
    with patch("common.psycopg2.connect", side_effect=Exception("Connection failed")) as mock_connect:
        query = "UPDATE table SET col = 'val'"

        with pytest.raises(Exception, match="Connection failed"):
            common.update_to_postgres(query, None, "url", "db", "user", "pwd")

        mock_connect.assert_called_once()


def test_update_to_postgres_execute_failure():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Execute failed")

    with patch("common.psycopg2.connect", return_value=mock_conn):
        query = "UPDATE table SET col = 'val'"

        with pytest.raises(Exception, match="Execute failed"):
            common.update_to_postgres(query, None, "url", "db", "user", "pwd")

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query)
        # No further calls expected after exception — so don't assert close or commit



def test_update_to_postgres_commit_failure():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.side_effect = Exception("Commit failed")

    with patch("common.psycopg2.connect", return_value=mock_conn):
        query = "UPDATE table SET col = 'val'"

        with pytest.raises(Exception, match="Commit failed"):
            common.update_to_postgres(query, None, "url", "db", "user", "pwd")

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query)
        mock_conn.commit.assert_called_once()
        # No further calls expected after exception — so don't assert cursor.close() or conn.close()

def test_update_to_postgres_cursor_close_failure():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.close.side_effect = Exception("Cursor close failed")

    with patch("common.psycopg2.connect", return_value=mock_conn):
        query = "UPDATE table SET col = 'val'"

        # Since common.update_to_postgres does not handle close() exceptions,
        # it will propagate. So we test for it
        with pytest.raises(Exception, match="Cursor close failed"):
            common.update_to_postgres(query, None, "url", "db", "user", "pwd")

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with(query)
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_not_called()  # since it won’t be reached after cursor.close() exception


