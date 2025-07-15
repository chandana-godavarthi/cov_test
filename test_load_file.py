import pytest
from unittest.mock import MagicMock, patch
import common  # your module under test

@pytest.fixture
def mock_env():
    mock_spark = MagicMock()
    mock_dbutils = MagicMock()
    mock_read_parquet = MagicMock()
    mock_df = MagicMock()

    mock_spark.read.parquet.return_value = mock_read_parquet
    mock_spark.read.format().option().option().load.return_value = mock_df

    return mock_spark, mock_dbutils, mock_read_parquet, mock_df


# Common patches applied to all relevant tests
common_patches = [
    patch("common.col", lambda x: MagicMock()),
    patch("common.F.col", lambda x: MagicMock()),
    patch("common.F.when", lambda cond, val: MagicMock()),
    patch("common.F.concat_ws", lambda sep, *cols: MagicMock()),
    patch("common.F.regexp_replace", lambda col, pattern, repl: MagicMock()),
]


@pytest.mark.parametrize("file_type", ["prod", "fact", "unknown"])
def test_load_file(file_type, mock_env):
    with patch("common.read_query_from_postgres") as mock_postgres, \
         common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4]:

        mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

        mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
        mock_dbutils.fs.ls.return_value = []

        result = common.load_file(
            file_type=file_type,
            RUN_ID="123",
            CNTRT_ID="CONTRACT",
            STEP_FILE_PATTERN="file_%.csv",
            vendor_pattern="vendor",
            notebook_name="test_notebook",
            delimiter=",",
            dbutils=mock_dbutils,
            postgres_schema="schema",
            spark=mock_spark,
            refDBjdbcURL="url",
            refDBname="dbname",
            refDBuser="user",
            refDBpwd="pwd"
        )

        assert result is not None


def test_load_file_zip_file(mock_env):
    with patch("common.read_query_from_postgres") as mock_postgres, \
         common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4]:

        mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

        mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []

        # simulate zip file found
        mock_dbutils.fs.ls.return_value = [
            MagicMock(name="file1.zip", path="/mnt/tp-source-data/WORK/file1.zip")
        ]

        result = common.load_file(
            file_type="prod",
            RUN_ID="file1",
            CNTRT_ID="CONTRACT",
            STEP_FILE_PATTERN="file_%.csv",
            vendor_pattern="vendor",
            notebook_name="test_notebook",
            delimiter=",",
            dbutils=mock_dbutils,
            postgres_schema="schema",
            spark=mock_spark,
            refDBjdbcURL="url",
            refDBname="dbname",
            refDBuser="user",
            refDBpwd="pwd"
        )

        assert result is not None
def test_load_file_special_columns(mock_env):
    with patch("common.read_query_from_postgres") as mock_postgres, \
         common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4]:

        mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

        mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = [
            {"measr_phys_name": "col#1"},
            {"measr_phys_name": "col#2"}
        ]

        mock_df.columns = ["col#1", "col#2"]
        mock_dbutils.fs.ls.return_value = []

        result = common.load_file(
            file_type="prod",
            RUN_ID="123",
            CNTRT_ID="CONTRACT",
            STEP_FILE_PATTERN="file_%.csv",
            vendor_pattern="vendor",
            notebook_name="test_notebook",
            delimiter=",",
            dbutils=mock_dbutils,
            postgres_schema="schema",
            spark=mock_spark,
            refDBjdbcURL="url",
            refDBname="dbname",
            refDBuser="user",
            refDBpwd="pwd"
        )

        assert result == "Success"
def test_load_file_multiple_mappings(mock_env):
    with patch("common.read_query_from_postgres") as mock_postgres, \
         common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4]:

        mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

        mock_df.columns = ["colA"]
        mock_dbutils.fs.ls.return_value = []

        mock_df.withColumnRenamed.return_value = mock_df
        mock_df.drop.return_value = mock_df

        result = common.load_file(
            file_type="prod",
            RUN_ID="123",
            CNTRT_ID="CONTRACT",
            STEP_FILE_PATTERN="file_%.csv",
            vendor_pattern="vendor",
            notebook_name="test_notebook",
            delimiter=",",
            dbutils=mock_dbutils,
            postgres_schema="schema",
            spark=mock_spark,
            refDBjdbcURL="url",
            refDBname="dbname",
            refDBuser="user",
            refDBpwd="pwd"
        )

        assert result == "Success"
from contextlib import ExitStack

def test_load_file_gz_file(mock_env):
    with ExitStack() as stack:
        mock_postgres = stack.enter_context(patch("common.read_query_from_postgres"))
        for p in common_patches:
            stack.enter_context(p)

        mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env
        mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []

        mock_dbutils.fs.ls.return_value = [
            MagicMock(name="file1.gz", path="/mnt/tp-source-data/WORK/file1.gz")
        ]

        result = common.load_file(
            file_type="prod",
            RUN_ID="file1",
            CNTRT_ID="CONTRACT",
            STEP_FILE_PATTERN="file_%.csv",
            vendor_pattern="vendor",
            notebook_name="test_notebook",
            delimiter=",",
            dbutils=mock_dbutils,
            postgres_schema="schema",
            spark=mock_spark,
            refDBjdbcURL="url",
            refDBname="dbname",
            refDBuser="user",
            refDBpwd="pwd"
        )

        assert result == "Success"
def test_load_file_csv_file(mock_env):
    with patch("common.read_query_from_postgres") as mock_postgres, \
         *common_patches:

        mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env
        mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []

        mock_dbutils.fs.ls.return_value = [
            MagicMock(name="file1.csv", path="/mnt/tp-source-data/WORK/file1.csv")
        ]

        result = common.load_file(
            file_type="prod",
            RUN_ID="file1",
            CNTRT_ID="CONTRACT",
            STEP_FILE_PATTERN="file_%.csv",
            vendor_pattern="vendor",
            notebook_name="test_notebook",
            delimiter=",",
            dbutils=mock_dbutils,
            postgres_schema="schema",
            spark=mock_spark,
            refDBjdbcURL="url",
            refDBname="dbname",
            refDBuser="user",
            refDBpwd="pwd"
        )

        assert result == "Success"
