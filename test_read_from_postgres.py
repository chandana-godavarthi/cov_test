from unittest.mock import MagicMock, call
import common  # adjust this if your module is named differently (e.g. from yourpackage import common)

def test_read_from_postgres_calls_spark_read_correctly():
    # Mock SparkSession and its methods
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_read = MagicMock()

    mock_spark.read = mock_read
    mock_format = mock_read.format.return_value
    # Ensure each .option() call returns the same mock (fluent chaining)
    mock_format.option.return_value = mock_format
    mock_format.load.return_value = mock_df

    # Inputs for the function
    object_name = "test_table"
    refDBjdbcURL = "jdbc:postgresql://localhost:5432"
    refDBname = "testdb"
    refDBuser = "testuser"
    refDBpwd = "testpwd"

    # Call the function under test
    result_df = common.read_from_postgres(
        object_name, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd
    )

    # Assert .format("jdbc") was called once
    mock_read.format.assert_called_once_with("jdbc")

    # Define expected chained .option() and .load() calls
    expected_calls = [
        call.option("driver", "org.postgresql.Driver"),
        call.option("url", f"{refDBjdbcURL}/{refDBname}"),
        call.option("dbtable", f"{object_name}"),
        call.option("user", refDBuser),
        call.option("password", refDBpwd),
        call.option("ssl", True),
        call.option("sslmode", "require"),
        call.option("sslfactory", "org.postgresql.ssl.NonValidatingFactory"),
        call.load()
    ]

    # Assert the chain of option calls in exact order followed by load()
    mock_format.assert_has_calls(expected_calls)

    # Assert that the returned DataFrame is what .load() returned
    assert result_df == mock_df
