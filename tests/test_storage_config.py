import pandas as pd
import yaml
from backtest_data_module.data_storage import HybridStorageManager


def test_manager_reads_config(tmp_path):
    cfg = {
        "tier_order": ["warm", "hot", "cold"],
        "hot_capacity": 2,
        "warm_capacity": 3,
    }
    cfg_path = tmp_path / "storage.yaml"
    cfg_path.write_text(yaml.dump(cfg), encoding="utf-8")

    manager = HybridStorageManager(config_path=str(cfg_path))
    assert manager.tier_order == ["warm", "hot", "cold"]
    assert manager.hot_capacity == 2
    assert manager.warm_capacity == 3

    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl", tier="warm")
    # default tiers should follow config order
    result = manager.read("tbl")
    pd.testing.assert_frame_equal(result, df)
