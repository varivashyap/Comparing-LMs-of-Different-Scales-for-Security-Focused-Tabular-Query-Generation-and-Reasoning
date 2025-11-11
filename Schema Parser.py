import pandas as pd
import numpy as np
import json

def detect_sdtype(col_data: pd.Series) -> str:
    sample = col_data.dropna()

    if sample.empty:
        return 'string'

    sample_str = sample.astype(str).str.lower()

    # Boolean detection (even if stored as strings)
    if sample_str.isin(['true', 'false', 'yes', 'no', '0', '1']).mean() > 0.95:
        return 'boolean'

    # Datetime detection
    try:
        pd.to_datetime(sample, errors='raise')
        return 'datetime'
    except Exception:
        pass

    # Numerical detection
    if pd.api.types.is_numeric_dtype(col_data):
        if col_data.nunique() <= min(20, len(col_data) // 10):
            return 'categorical'
        return 'numerical'

    # Object (string) based heuristics
    num_unique = sample.nunique()
    avg_len = sample.astype(str).map(len).mean()

    # Categorical heuristic: few unique values and short strings
    if num_unique <= min(20, len(sample) * 0.1) and avg_len < 30:
        return 'categorical'

    # Dynamic content (e.g. nested JSON or mixed types)
    try:
        sample.apply(json.loads)
        return 'dynamic'
    except:
        pass

    return 'string'

def detect_datetime_format(series: pd.Series):
    try:
        sample = series.dropna().astype(str).iloc[0]
        dt = pd.to_datetime(sample)
        if dt.strftime("%H:%M:%S") == "00:00:00":
            return "%Y-%m-%d"
        return "%Y-%m-%d %H:%M:%S"
    except:
        return None

def infer_metadata_from_df(df: pd.DataFrame, table_name, table_description):
    metadata = {
        "table_name": table_name,
        "table_description": table_description,
        "columns": []
    }

    for col in df.columns:
        col_data = df[col]
        sdtype = detect_sdtype(col_data)

        col_meta = {
            "column_name": col,
            "data_type": sdtype,
            "description": ""
        }

        if sdtype == "datetime":
            dt_format = detect_datetime_format(col_data)
            if dt_format:
                col_meta["datetime_format"] = dt_format

        metadata["columns"].append(col_meta)

    return metadata

def generate_metadata_from_csv(csv_path, output_path=None):
    df = pd.read_csv(csv_path)
    table_name = csv_path.split("\\")[-1].replace(".csv", "")
    table_description = ""
    metadata = infer_metadata_from_df(df, table_name, table_description)

    if output_path is None:
        output_path = f"{table_name}_metadata.json"

    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=4)

    print(f"Metadata saved to: {output_path}")

# Example usage
csv_path = "C:\\Users\\polad\\Desktop\\Logs\\Varivashya_Poladi\\Synthetic Complex Query Generation\\Scenario 3\\SignInLogsBetaSchema.csv"
output_path = "C:\\Users\\polad\\Desktop\\Logs\\Varivashya_Poladi\\Synthetic Complex Query Generation\\Scenario 3\\metadata.json"

generate_metadata_from_csv(csv_path, output_path)