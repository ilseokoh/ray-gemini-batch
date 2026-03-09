import ray
import os
import ray
import os
import pandas as pd
from gemini import call_gemini_with_attachment

ray.init()

# Read target file list from a SQL query of the dataset.
ds = ray.data.read_bigquery(
    project_id="kevin-ai-playground",
    query = "SELECT * FROM `csv_parse_ds.csv_analysis_src_files`",
)

print(f"----- File Count: {ds.count}")

@ray.remote
def processing_analysis(batch: pd.DataFrame) -> pd.DataFrame:
    results = []
    errors = []
    for i in range(len(batch)):
        if pd.isna(batch.loc[i, 'uri']) or pd.isna(batch.loc[i, 'content_type']) or pd.isna(batch.loc[i, 'size']):
            results.append("")
            errors.append("Missing required columns: uri, content_type, or size")
            continue
        try:
            uri = batch.loc[i, 'uri']
            content_type = batch.loc[i, 'content_type']
            result = call_gemini_with_attachment(url=uri, type=content_type)
            results.append(result)
            errors.append("")
        except Exception as e:
            results.append("")
            print(f"------- {e}")
            errors.append(str(e))

    batch["result"] = results
    batch["error"] = errors
    return batch

# Process the dataset in batches using iter_batches
futures = [processing_analysis.remote(batch) for batch in ds.iter_batches(batch_size=5, batch_format="pandas")]

# Get the results
processed_batches = ray.get(futures)

# Create a new dataset from the processed batches
if processed_batches:
    processed_ds = ray.data.from_pandas(processed_batches)
    # Show the result
    processed_ds.show()
else:
    print("No data to process.")

