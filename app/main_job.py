import ray
import os
import pandas as pd
from gemini import call_gemini_with_attachment, call_gemini_with_csv, PIData
from google.cloud import storage
from typing import List
import json

ray.init()

project_id = os.environ.get('PROJECT_ID', '')
bq_table = os.environ.get('BQ_TABLE_NAME')

# Read target file list from a SQL query of the dataset.
ds = ray.data.read_bigquery(
    project_id=project_id,
    query = "SELECT * FROM `<dataset>.<table>`",
)

print(f"--------------- File Count: {ds.count()}")

@ray.remote
def process_csv_chunk(df_chunk: pd.DataFrame) -> List[PIData]:
    """Calls Gemini API for a chunk of a large CSV file."""
    return call_gemini_with_csv(df_chunk)

@ray.remote
def processing_analysis(batch: pd.DataFrame) -> pd.DataFrame:
    results = []
    errors = []
    size_49mib = 49 * 1024 * 1024

    for i in range(len(batch)):
        if pd.isna(batch.loc[i, 'uri']) or pd.isna(batch.loc[i, 'content_type']) or pd.isna(batch.loc[i, 'size']):
            results.append("")
            errors.append("Missing required columns: uri, content_type, or size")
            continue
        
        try:
            uri = batch.loc[i, 'uri']
            content_type = batch.loc[i, 'content_type']
            file_size = batch.loc[i, 'size']

            # File Size 가 5MiB 이하일 때는 멀티모달 파일 첨부로 Gemini 호출
            if file_size <= size_49mib:
                result = call_gemini_with_attachment(url=uri, type=content_type)
                results.append(result)
                errors.append("")
            else: # File size is > 5MiB CSV 를 읽어서 Chucking 후 Gemini 호출
                if content_type.startswith('text/csv'):
                    
                    # Use pandas to read the CSV from GCS in chunks
                    df_iterator = pd.read_csv(uri, chunksize=2000)
                    
                    # Create tasks for each chunk
                    chunk_tasks = [process_csv_chunk.remote(chunk) for chunk in df_iterator]
                    
                    # Get results and aggregate
                    chunk_results = ray.get(chunk_tasks)
                    aggregated_result = []
                    for res_list in chunk_results:
                        aggregated_result.extend(res_list)
                    
                    results.append(aggregated_result)
                    errors.append("")

                else:
                    results.append("")
                    errors.append(f"File size ({file_size} bytes) exceeds 5MiB limit and is not a CSV file.")

        except Exception as e:
            results.append("")
            print(f"------- {e}")
            errors.append(str(e))

    batch["result"] = results
    batch["error"] = errors
    return batch

# Process the dataset in batches using iter_batches
futures = [processing_analysis.remote(batch) for batch in ds.iter_batches(batch_size=3, batch_format="pandas")]

# Get the results
processed_batches = []
while futures:
    ready, not_ready = ray.wait(futures)
    ready_batches = ray.get(ready)
    processed_batches.extend(ready_batches)
    # progressive save the data to BQ table
    if ready_batches:
        ready_df = pd.concat(ready_batches)
        ready_df['result'] = ready_df['result'].apply(
            lambda x: json.dumps([i.model_dump() for i in x], ensure_ascii=False) if isinstance(x, list) else x
        )
        pandas_gbq.to_gbq(ready_df, bq_table, project_id=project_id, if_exists='append')
        
        #ready_df.to_gbq(destination_table=bq_table, project_id=project_id, if_exists='append') # deprecated
    futures = not_ready

# Create a new dataset from the processed batches
if processed_batches:
    for batch in processed_batches:
        batch['result'] = batch['result'].apply(
            lambda x: json.dumps([i.model_dump() for i in x], ensure_ascii=False) if isinstance(x, list) else x
        )
    processed_ds = ray.data.from_pandas(processed_batches)

    print(f"--------------- Result Count: {processed_ds.count()}")
    # Show the result
    print(f"{processed_ds.show(3)}")

    # Save the result to BigQuery
    if bq_table:
        processed_ds.write_bigquery(
            project_id=project_id,
            dataset=f"{bq_table}_batch" ,
            overwrite_table=True,
        )
        print(f"Successfully saved data to BigQuery table: {bq_table}")
    else:
        print("BQ_TABLE_NAME environment variable not set. Skipping saving to BigQuery.")
    
else:
    print("No data to process.")