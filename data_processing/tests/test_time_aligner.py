import pandas as pd
import pytest
from data_processing.time_aligner import TimeAligner

def test_time_aligner():
    # Create a sample DataFrame
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(['2023-01-01 00:00:00', '2023-01-01 00:00:30', '2023-01-01 00:01:00']),
        'value': [1, 2, 3]
    })

    # Create a TimeAligner
    time_aligner = TimeAligner(resample_rule='1T')

    # Process the DataFrame
    processed_df = time_aligner.process(df)

    # Check the output
    assert len(processed_df) == 2
    assert processed_df['value'].iloc[0] == 1.5
    assert processed_df['value'].iloc[1] == 3.0
