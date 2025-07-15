import pytest
from unittest.mock import MagicMock, patch
import common


def test_assign_skid_type_not_in_list():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    result = common.assign_skid(mock_df, 1, 'invalid_type', 'catalog_name', mock_spark)
    assert result is None


def test_assign_skid_runid_exists():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']

    # Mock the spark.sql().count() to return >0 (existing run_id)
    mock_spark.sql.return_value.count.return_value = 1

    result_df = MagicMock()
    mock_spark.sql.return_value = result_df

    with patch.object(result_df, 'createOrReplaceTempView') as mock_tempview, \
         patch.object(result_df, 'select', return_value=result_df):

        result = common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark)

        assert result == result_df
        mock_df.createOrReplaceTempView.assert_called()
        mock_spark.sql.assert_called()
        result_df.createOrReplaceTempView.assert_called()
        result_df.select.assert_called()


def test_assign_skid_runid_not_exists():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']

    # Simulate spark.sql().count() = 0 (run_id not exists)
    count_mock = MagicMock(return_value=0)

    # Mock for df_sel (first spark.sql result — the one with .write)
    df_sel_mock = MagicMock()
    df_sel_mock.write.mode.return_value.saveAsTable = MagicMock()

    # Result DF for other spark.sql calls
    result_df = MagicMock()
    result_df.select.return_value = result_df

    # Provide side effects for 4 spark.sql calls
    mock_spark.sql.side_effect = [
        df_sel_mock,                         # 1. SELECT CAST(run_id...) query result
        MagicMock(count=count_mock),         # 2. check run_id exists
        result_df,                           # 3. SELECT skid mapping
        result_df                            # 4. final join query
    ]

    with patch.object(mock_df, 'createOrReplaceTempView'), \
         patch.object(result_df, 'createOrReplaceTempView'):

        result = common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark)

        #  Assert saveAsTable called on df_sel_mock
        df_sel_mock.write.mode.return_value.saveAsTable.assert_called_once_with('catalog_name.internal_tp.tp_prod_skid_seq')

        # Validate result
        assert result == result_df


def test_assign_skid_exception():
    mock_spark = MagicMock()
    mock_df = MagicMock()

    # Force spark.sql to raise an exception
    mock_spark.sql.side_effect = Exception("mock error")

    with pytest.raises(Exception) as exc_info:
        common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark)

    assert "mock error" in str(exc_info.value)
