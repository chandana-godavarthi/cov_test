import pytest
from unittest.mock import MagicMock
from common import acn_prod_trans_materialize, LIGHT_REFINED_PATH

def test_acn_prod_trans_materialize(monkeypatch):
    # Create a mock DataFrame
    mock_df = MagicMock()
    mock_coalesce = MagicMock()
    mock_write = MagicMock()
    mock_format = MagicMock()
    mock_options = MagicMock()
    mock_mode = MagicMock()

    # Chain mocks
    mock_df.coalesce.return_value = mock_coalesce
    mock_coalesce.write = mock_write
    mock_write.format.return_value = mock_format
    mock_format.options.return_value = mock_options
    mock_options.mode.return_value = mock_mode

    run_id = "test_run"

    # Run the function
    acn_prod_trans_materialize(mock_df, run_id)

    expected_path = f'/mnt/{LIGHT_REFINED_PATH}ACN_Prod_Load/{run_id}/ACN_Prod_Load_chain/tp_mm_ACN_Prod_Load_chain.parquet'

    # Assertions on the method call chain
    mock_df.coalesce.assert_called_once_with(1)
    mock_write.format.assert_called_once_with("parquet")
    mock_format.options.assert_called_once_with(header=True)
    mock_options.mode.assert_called_once_with('overwrite')
    mock_mode.save.assert_called_once_with(expected_path)
