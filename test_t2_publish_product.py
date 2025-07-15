import pytest
from unittest.mock import MagicMock, call
import common  # your module containing t2_publish_product


def test_t2_publish_product_calls_sql_and_union(monkeypatch):
    # Arrange test values
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    prod_dim = "test_prod_dim"

    # Mock DataFrame and SparkSession
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_select_df = MagicMock()

    # Set up mocks
    mock_spark.sql.side_effect = [mock_select_df, None]  # first call returns a df, second call for merge returns None
    mock_df.unionByName.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    # Act
    common.t2_publish_product(mock_df, catalog_name, schema_name, prod_dim, mock_spark)

    # Assert: spark.sql called twice — first for select, then for merge
    assert mock_spark.sql.call_count == 2

    # Check first sql() call content
    expected_select_query = f"SELECT * FROM {catalog_name}.{schema_name}.{prod_dim} limit 0"
    mock_spark.sql.assert_has_calls([call(expected_select_query)], any_order=False)

    # Check unionByName was called with correct params
    mock_df.unionByName.assert_called_once_with(mock_select_df, allowMissingColumns=True)

    # Check createOrReplaceTempView was called with correct view name
    mock_df.createOrReplaceTempView.assert_called_once_with("df_mm_prod_sdim_promo_vw")

    # Check second sql call content (the MERGE statement)
    merge_sql_call = mock_spark.sql.call_args_list[1][0][0]
    assert "MERGE INTO" in merge_sql_call
    assert f"{catalog_name}.{schema_name}.{prod_dim}" in merge_sql_call
    assert "WHEN MATCHED THEN" in merge_sql_call
    assert "WHEN NOT MATCHED THEN" in merge_sql_call

    # Done — no exceptions means success


def test_t2_publish_product_empty_dataframe(monkeypatch):
    """
    Confirm that function behaves gracefully even if the select returns an empty DataFrame.
    """

    catalog_name = "test_catalog"
    schema_name = "test_schema"
    prod_dim = "test_prod_dim"

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_empty_df = MagicMock()

    mock_spark.sql.side_effect = [mock_empty_df, None]
    mock_df.unionByName.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    common.t2_publish_product(mock_df, catalog_name, schema_name, prod_dim, mock_spark)

    mock_df.unionByName.assert_called_once_with(mock_empty_df, allowMissingColumns=True)
    mock_df.createOrReplaceTempView.assert_called_once_with("df_mm_prod_sdim_promo_vw")
    assert mock_spark.sql.call_count == 2


def test_t2_publish_product_merge_sql_query_correct(monkeypatch):
    """
    Check that the generated merge SQL string matches expected structure.
    """
    catalog_name = "catalog"
    schema_name = "schema"
    prod_dim = "prod_dim"

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_target_df = MagicMock()

    mock_spark.sql.side_effect = [mock_target_df, None]
    mock_df.unionByName.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None

    common.t2_publish_product(mock_df, catalog_name, schema_name, prod_dim, mock_spark)

    merge_sql = mock_spark.sql.call_args_list[1][0][0]

    assert f"MERGE INTO {catalog_name}.{schema_name}.{prod_dim}" in merge_sql
    assert "USING df_mm_prod_sdim_promo_vw src" in merge_sql
    assert "ON src.srce_sys_id = tgt.srce_sys_id AND src.prod_skid = tgt.prod_skid" in merge_sql
    assert "WHEN MATCHED THEN" in merge_sql
    assert "UPDATE SET *" in merge_sql
    assert "WHEN NOT MATCHED THEN" in merge_sql
    assert "INSERT *" in merge_sql
