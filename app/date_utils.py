import pandas as pd

def iter_complete_months(start: str = "2025-01-01") -> list[str]:
    last_complete_month = pd.Timestamp.today().to_period("M").to_timestamp() - pd.DateOffset(months=1)
    return (
        pd.date_range(start=start, end=last_complete_month, freq="MS")
        .strftime("%Y-%m")
        .tolist()
    )