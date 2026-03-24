import ray
import os
import pandas as pd
from gemini import call_gemini_with_attachment, call_gemini_with_csv, PIData
from google.cloud import storage
from typing import List

import json
import time
import tempfile
import pandas as pd
from pandas import DataFrame
import pandas_gbq
from gemini import call_gemini_with_attachment, call_gemini_with_csv, PIData, call_gemini_with_txt, detect_gcs_encoding, convert_gcs_encoding_to_utf8_cwd
from google.cloud import storage
from google.cloud.exceptions import NotFound
from typing import List
from typing import Optional
from pandas.errors import ParserError


max_retries = 5
base_delay = 3  # 처음 대기할 시간 (초)

ray.init()

project_id = os.environ.get('PROJECT_ID', '')
bq_table = os.environ.get('BQ_TABLE_NAME')
bq_backup_table = os.environ.get('BQ_BACKUP_TABLE')

# Read target file list from a SQL query of the dataset.
ds = ray.data.read_bigquery(
    project_id=project_id,
    query = f"""
    SELECT
        *
    FROM
        `csv_parse_ds`.`error_retry_work_csv_src_files` AS src
    WHERE
        NOT EXISTS(
        SELECT
            1
        FROM
            `{bq_table}` AS res
        WHERE
            src.uri = res.uri);
    """
)

@ray.remote
def process_csv_chunk(df_chunk: pd.DataFrame) -> List[PIData]:
    """Calls Gemini API for a chunk of a large CSV file."""
    return call_gemini_with_csv(df_chunk)

@ray.remote
def process_txt_chunk(txt: str) -> List[PIData]:
    # Call gemini with plain text
    return call_gemini_with_txt(txt)

@ray.remote
def create_batch_job(df: pd.DataFrame): 
    batch_ds = ray.data.from_pandas(df)
    # Process the dataset in batches using iter_batches
    futures = [processing_analysis.remote(batch) for batch in batch_ds.iter_batches(batch_size=3, batch_format="pandas")]

    # Get the results
    processed_batches = []
    while futures:
        ready, not_ready = ray.wait(futures)
        ready_batches = ray.get(ready)

        processed_batches.extend(ready_batches)
        futures = not_ready

    # Create a new dataset from the processed batches
    if processed_batches:
        # Save the result to BigQuery
        if bq_table:
            print("모든 배치를 취합하여 BigQuery에 한 번에 저장합니다...: ")
            final_df = pd.concat(processed_batches)
            print(f"{len(final_df)} 개 -------------------------")

            first_10_rows = final_df[:10]
            print(first_10_rows)
            
            max_retries = 3
            base_delay = 5
            for attempt in range(max_retries):
                try:
                    pandas_gbq.to_gbq(final_df, bq_table, project_id=project_id, if_exists='append')
                    print("BigQuery 데이터 삽입 성공!")

                    ## 백업 
                    backup_query = f"""
                        INSERT INTO `{bq_backup_table}` (uri,
                            content_type,
                            size,
                            result,
                            error)
                        SELECT
                        t1.uri,
                        t1.content_type,
                        t1.size,
                        t1.result,
                        t1.error
                        FROM
                        `{bq_table}` AS t1
                        WHERE
                        NOT EXISTS(
                        SELECT
                            1
                        FROM
                            `{bq_backup_table}` AS t2
                        WHERE
                            t1.uri = t2.uri );
                    """
                    pandas_gbq.read_gbq(backup_query, project_id=project_id)
                    print("백업 성공!")

                    break  # Exit loop on success
                except Exception as e:
                    if attempt < max_retries - 1:
                        sleep_time = base_delay * (2 ** attempt)
                        print(f"에러 발생: {e}")
                        print(f"{sleep_time}초 후 재시도 합니다... (시도 횟수: {attempt + 1}/{max_retries})")
                        time.sleep(sleep_time)
                    else:
                        print("최대 재시도 횟수를 초과했습니다. 데이터 삽입에 실패했습니다.")

            print(f"Successfully saved data to BigQuery table: {bq_table}")
        else:
            print("BQ_TABLE_NAME environment variable not set. Skipping saving to BigQuery.")
        
    else:
        print("No data to process.")

def is_csv_readable(uri: str):
    """
    주어진 경로(uri)의 CSV 파일을 읽어 DataFrame으로 반환합니다.
    여러 인코딩(utf-8, cp949)을 순차적으로 시도합니다.
    파일을 읽을 수 없거나 오류 발생 시 None을 반환합니다.

    Args:
        uri (str): 파일 경로 또는 URL.

    Returns:
        Optional[DataFrame]: 성공 시 DataFrame, 실패 시 None.
    """
    # 시도할 인코딩 리스트
    encodings_to_try = ['utf-8', 'cp949', 'euc-kr']

    for encoding in encodings_to_try:
        try:
            # 여기에서 'file_path' 대신 파라미터 'uri'를 사용해야 합니다.
            df = pd.read_csv(uri, encoding=encoding, chunksize=800)

            for chunk in df:
                df.head(10)

            print(f"파일 읽기 성공! (인코딩: {encoding})")
            return df
        except UnicodeDecodeError:
            # 현재 인코딩으로 실패하면 다음 인코딩을 시도합니다.
            print(f"'{encoding}' 인코딩으로 파일 읽기 실패. 다음 인코딩을 시도합니다...")
            continue
        except FileNotFoundError:
            print(f"오류: '{uri}' 파일을 찾을 수 없습니다.")
            return None
        except pd.errors.ParserError:
            print(f"오류: CSV 파일의 형식이 잘못되었습니다.")
            return None
        except Exception as e:
            # 예상치 못한 기타 오류 처리
            print(f"파일을 읽는 중 예상치 못한 오류가 발생했습니다: {e}")
            return None

    # 모든 인코딩 시도에 실패한 경우
    print("지원되는 인코딩으로 파일을 읽을 수 없습니다.")
    return None

def read_chunks_from_gcs(gcs_uri: str, chunk_size: int = 100000):
  """
  GCS URI로부터 파일을 다운로드하여 텍스트 조각을 생성하는 이터레이터를 반환합니다.

  Args:
    gcs_uri (str): 처리할 파일의 GCS URI (예: "gs://bucket-name/path/to/file.txt").
    chunk_size (int, optional): 각 조각의 최대 글자 수. 기본값은 100000.

  Returns:
    iterator: 파일의 텍스트 조각을 하나씩 반환하는 이터레이터.
  """
  # 1. GCS URI 파싱
  if not gcs_uri.startswith("gs://"):
    raise ValueError("잘못된 GCS URI 형식입니다. 'gs://bucket-name/blob-path' 형식이어야 합니다.")
  
  bucket_name, blob_name = gcs_uri.replace("gs://", "").split("/", 1)

  # 2. GCS 클라이언트 초기화 및 블롭 가져오기
  try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        print(f"오류: GCS에서 '{gcs_uri}' 파일을 찾을 수 없습니다.")
        return
  
  except Exception as e:
    print(f"GCS 클라이언트 초기화 또는 블롭 확인 중 오류 발생: {e}")
    return

  # detected_encoding = detect_gcs_encoding(gcs_uri=gcs_uri)

  # 3. 임시 파일에 다운로드 후 처리
  # NamedTemporaryFile을 with문과 함께 사용하면 블록이 끝날 때 파일이 자동 삭제됩니다.
  with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=True) as temp_f:
    try:
        print(f"'{gcs_uri}' 다운로드 시작...")
        blob.download_to_filename(temp_f.name)
        print("다운로드 완료. 파일 처리 시작...")
        
        # 파일 포인터를 시작으로 되돌림
        temp_f.seek(0)
        
        while True:
          chunk = temp_f.read(chunk_size)
          if not chunk:
            break
          yield chunk

    except NotFound:
        print(f"오류: GCS에서 '{gcs_uri}' 파일을 다운로드하는 데 실패했습니다.")
    except Exception as e:
        print(f"파일 다운로드 또는 처리 중 오류 발생: {e}")


@ray.remote
def processing_analysis(batch: pd.DataFrame) -> pd.DataFrame:
    results = "[]"
    errors = ""
    size_49mib = 1048570
    #7340032 # 49 * 1024 * 1024 # 1048576

    for i in range(len(batch)):
        if pd.isna(batch.loc[i, 'uri']) or pd.isna(batch.loc[i, 'content_type']) or pd.isna(batch.loc[i, 'size']):
            results = "[]"
            errors = "Missing required columns: uri, content_type, or size"
            continue
        
        try:
            uri = batch.loc[i, 'uri']
            content_type = batch.loc[i, 'content_type']
            file_size = batch.loc[i, 'size']

            # File Size 가 5MiB 이하일 때는 멀티모달 파일 첨부로 Gemini 호출
            if file_size <= 0: 
                results = "[]"
                errors = ""
            elif file_size <= size_49mib:
                print("Processing as a attachment file.")
                results = call_gemini_with_attachment(url=uri, type=content_type)
                errors = ""
            else: # File size is > 5MiB CSV 를 읽어서 Chucking 후 Gemini 호출
                if content_type.startswith('text/csv'):

                    # Safe read the csv file
                    df_iterator = is_csv_readable(uri)
                    chunk_tasks = []
                    if df_iterator: 
                        print("Processing as a CSV file.")
                        chunk_tasks = [process_csv_chunk.remote(chunk) for chunk in df_iterator]
                    else: 
                        # GCS uri 에서 파일을 읽어서 기본 chunk 단위인 500000 캐릭터 단위로 잘라주는 iterator를 반환하는 함수를 호출
                        print("CSV Read ERROR: processing as a text file.")
                        chunk_iterator = read_chunks_from_gcs(uri, chunk_size=200000)
                        chunk_tasks = [process_txt_chunk.remote(chunk) for chunk in chunk_iterator]
                    
                    # Get results and aggregate
                    aggregated_result = []
                    while len(chunk_tasks) > 0:
                        done_ids, chunk_tasks = ray.wait(chunk_tasks)
                        chunk_results = ray.get(done_ids)
                        for res_list in chunk_results:
                            aggregated_result.extend(res_list)

                    if aggregated_result:
                        results = f"[{','.join(item.model_dump_json() for item in aggregated_result)}]"
                    else:
                        results = "[]"
                    errors = ""

                else:
                    errors = f"File size ({file_size} bytes) exceeds 5MiB limit and is not a CSV file."

        except Exception as e:
            print(f"================{str(e)}")
            results = ""
            errors = str(e)

    batch["result"] = results
    batch["error"] = errors
    return batch

batches = [create_batch_job.remote(batch) for batch in ds.iter_batches(batch_size=1000, batch_format="pandas")]

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


print("All batch jobs completed.")

# Shutdown Ray
ray.shutdown()