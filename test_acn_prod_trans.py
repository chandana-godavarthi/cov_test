import pytest
from unittest.mock import MagicMock
import common  

@pytest.fixture(autouse=True)
def mock_pyspark_functions(monkeypatch):
    monkeypatch.setattr(common, 'col', MagicMock(name="col"))
    monkeypatch.setattr(common, 'trim', MagicMock(name="trim"))
    monkeypatch.setattr(common, 'when', MagicMock(name="when"))
    monkeypatch.setattr(common, 'date_format', MagicMock(name="date_format"))
    monkeypatch.setattr(common, 'expr', MagicMock(name="expr")) 

def test_acn_prod_trans_calls_sql_and_parquet_correctly():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_dataframe = MagicMock()

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.read.parquet.assert_called()
    mock_spark.sql.assert_called()
    assert result_df is not None


def test_acn_prod_trans_parquet_empty():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_empty_df = MagicMock()

    mock_empty_df.count.return_value = 0
    mock_empty_df.createOrReplaceTempView.return_value = None
    mock_empty_df.withColumn.return_value = mock_empty_df
    mock_empty_df.unionByName.return_value = mock_empty_df
    mock_empty_df.groupBy.return_value.agg.return_value = mock_empty_df
    mock_empty_df.drop.return_value = mock_empty_df

    mock_spark.sql.return_value = mock_empty_df
    mock_spark.read.parquet.return_value = mock_empty_df

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.read.parquet.assert_called()
    assert result_df is not None


def test_acn_prod_trans_dataframe_methods_called():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_dataframe = MagicMock()

    mock_dataframe.count.return_value = 5
    mock_dataframe.createOrReplaceTempView.return_value = None
    mock_dataframe.withColumn.return_value = mock_dataframe
    mock_dataframe.unionByName.return_value = mock_dataframe
    mock_dataframe.groupBy.return_value.agg.return_value = mock_dataframe
    mock_dataframe.drop.return_value = mock_dataframe

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.read.parquet.assert_called()
    mock_spark.sql.assert_called()
    assert result_df is not None


def test_acn_prod_trans_final_join_query():
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark = MagicMock()
    mock_dataframe = MagicMock()

    mock_dataframe.count.return_value = 5
    mock_dataframe.createOrReplaceTempView.return_value = None
    mock_dataframe.withColumn.return_value = mock_dataframe
    mock_dataframe.unionByName.return_value = mock_dataframe
    mock_dataframe.groupBy.return_value.agg.return_value = mock_dataframe
    mock_dataframe.drop.return_value = mock_dataframe

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = common.acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.sql.assert_called()
    assert result_df is not None
