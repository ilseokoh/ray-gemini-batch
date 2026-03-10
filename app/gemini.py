import os
import random
import time
import json
import chardet
from google.cloud import storage
from google import genai
from pydantic import BaseModel, Field
from typing import Optional, List
from google.genai import types
import pandas as pd

project_id = os.environ.get('PROJECT_ID', 'kevin-ai-playground')
gemini_location = os.environ.get('GEMINI_LOCATION', 'global')
gemini_model = os.environ.get('GEMINI_MODEL','gemini-2.5-flash-lite')
bq_location = os.environ.get('BQ_LOCATION','asia-northeast3')
bq_table = os.environ.get('BQ_TABLE_NAME','csv_parse_ds.csv_a_result')
max_retry_cnt = int(os.environ.get('MAX_RETRY_CNT',3))

# --- Initialize Vertex AI ---
client = genai.Client(vertexai=True, project=project_id, location=gemini_location)

class PIData(BaseModel):
    """Extracted Personal Information Data Model"""
    name: Optional[str] = Field(default=None, description="personal name")
    gender: Optional[str] = Field(default=None, description="gender(Man or Woman)")
    birthday: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    credit_card_no: Optional[str] = Field(default=None, description="credit card number")
    phone_number: Optional[str] = Field(default=None, description="personal phone number")
    address: Optional[str] = Field(default=None, description="full address")
    passport_number: Optional[str] = Field(default=None, description="passport number")
    social_security_number: Optional[str] = Field(default=None, description="social security number")
    drivers_licence_number: Optional[str] = Field(default=None, description="drivers licence number")
    is_sensitive_document: Optional[bool] = Field(
        default=None,
        description="if given document is `medical records` or `family relationship certificates` or `documents containing salary information` or identification information like `passport`, `driving license`, etc. then `true` else `false`"
    )
    email: Optional[str] = Field(default=None, description="email address")
    others: Optional[str] = Field(default=None, description="other information")


# instruction, sturctured ouput schema and prompt
system_instruction = """
You need to extract personal information from a given document or image. Given documents or images include passports from various countries, driver's licence and social security card etc.

Personal information includes person's name, gender, birthday, phone_number, address, passport_number, social_security_number, drivers_licence_number, email etc.
If the name is separately written in the document or image as given/first name and sur/family name, combine those two into a full name.
If the personal information is given in a table format, considering the structure of the table, extract and match the information correctly.
And determine if the given document is `medical records (의료 기록)` or `family relationship certificates (가족관계증명서)` or `identification information (passport, driving license, 여권 등)` or `salary information (급여정보)`.
Check the file name.

The results are output in the following json format, which is a **list of objects (dictionaries)**, allowing for the extraction of information for **multiple individuals**.
If there is no personal information in the given document or image, output should be an **empty list** (`[]`)

 - name: personal name
 - gender: gender(Man or Woman)
 - birthday: YYYY-MM-DD
 - credit_card_no: credit card number
 - phone_number: personal phone number
 - address: full address
 - passport_number: passport number
 - social_security_number: social security number
 - drivers_licence_number: drivers licence number
 - is_sensitive_document: if given document is `medical records` or `family relationship certificates` or `documents containing salary information` or identification information like `passport`, `driving license`, etc. then `true` else `false`
 - email: email address
 - others: other information
"""

prompt = "Please extract the personal information."

response_schema = {
    "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "name": {
        "type": "string"
      },
      "gender": {
        "type": "string"
      },
      "birthday": {
        "type": "string"
      },
      "credit_card_no": {
        "type": "string"
      },
      "phone_number": {
        "type": "string"
      },
      "address": {
        "type": "string"
      },
      "passport_number": {
        "type": "string"
      },
      "social_security_number": {
        "type": "string"
      },
      "drivers_licence_number": {
        "type": "string"
      },
      "is_sensitive_document": {
        "type": "boolean"
      },
      "email": {
        "type": "string"
      },
      "others": {
        "type": "string"
      }
    },
  }
}


def convert_gcs_encoding_to_utf8_cwd(gcs_uri: str, src_encoding: str):
    """
    GCS 파일을 현재 디렉토리로 다운로드하여 지정된 인코딩(src_encoding)을
    UTF-8로 변환한 뒤, GCS에 덮어쓰고 로컬 파일은 삭제합니다.

    Args:
        gcs_uri (str): gs://bucket-name/path/to/file 형식의 URI
        src_encoding (str): 원본 파일의 인코딩 (예: 'euc-kr', 'cp949', 'utf-16')
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError("URI must start with 'gs://'")

    # 1. URI 파싱 및 클라이언트 설정
    parts = gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]
    
    # 파일명 추출 및 로컬 경로 설정
    filename = os.path.basename(blob_name)
    local_input_path = filename
    local_output_path = f"utf8_{filename}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        # 2. 현재 디렉토리로 다운로드
        blob.download_to_filename(local_input_path)

        # 3. 인코딩 변환 (src_encoding -> utf-8)
        with open(local_input_path, "r", encoding=src_encoding) as f_in:
            with open(local_output_path, "w", encoding="utf-8") as f_out:
                for line in f_in:
                    f_out.write(line)

        # 4. GCS에 업로드 (덮어쓰기)
        blob.upload_from_filename(
            local_output_path,
            content_type=blob.content_type  # 기존 Content-Type 유지
        )

    except UnicodeDecodeError:
        print(f"Error: '{src_encoding}' 인코딩으로 파일을 읽을 수 없습니다. 인코딩을 확인해주세요.")
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # 5. 로컬 파일 정리
        if os.path.exists(local_input_path):
            os.remove(local_input_path)
            
        if os.path.exists(local_output_path):
            os.remove(local_output_path)

def detect_gcs_encoding(gcs_uri: str, chunk_size: int = 1024) -> str:
    """
    GCS 파일을 스트리밍으로 열어 초반 1024바이트(chunk_size)만 읽은 뒤
    파일의 인코딩 방식을 감지하여 반환합니다.

    Args:
        gcs_uri (str): gs://bucket-name/path/to/file 형식의 URI
        chunk_size (int): 읽어올 바이트 수 (기본 1024)

    Returns:
        str: 감지된 인코딩 이름 (예: 'utf-8', 'EUC-KR', 'ascii'). 
             감지 실패 시 None 반환.
    """
    if not gcs_uri.startswith("gs://"):
        return "URI must start with 'gs://'", 200

    # 1. URI 파싱
    parts = gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]

    try:
        # 2. GCS 클라이언트 및 Blob 초기화
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # 3. 스트리밍으로 열어서 지정된 바이트(1024)만큼만 읽기
        # 'rb' 모드로 열어야 바이너리 데이터를 읽을 수 있습니다.
        with blob.open("rb") as f:
            raw_data = f.read(chunk_size)

        # 4. 인코딩 감지
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        
        return encoding

    except Exception as e:
        print(f"Error detecting encoding: {e}")
        return None

def check_content_type_for_gemini(content_type: str) -> str | None:
    """Checks if the content type is supported by Gemini."""
    supported_types = ["application/pdf", "text/plain", "text/csv"]
    for t in supported_types:
        if content_type.startswith(t):
            return t
    return None

def call_gemini_with_attachment(url: str, type: str) -> List[PIData]:
    """
    Calls the Gemini model with a given file URI and includes retry logic
    with exponential backoff.

    Args:
        url: The Google Cloud Storage URI of the file (e.g., "gs://bucket/file.csv").
        type: The MIME type of the file.

    Returns:
        The response text from the Gemini model as a JSON string.

    Raises:
        ValueError: If the URL does not start with "gs://".
        Exception: If the API call fails after multiple retries.
    """
    if not url.startswith("gs://"):
        raise ValueError("URL must start with 'gs://'")

    checked_type = check_content_type_for_gemini(type)
    if checked_type is None:
        raise ValueError(f"Unsupported content type: {type}")

    encoding = detect_gcs_encoding(url)

    if encoding:
        if encoding.lower() == 'utf-8':
            pass
        elif encoding.lower() == 'utf-16' or encoding.lower() == 'euc-kr':
            convert_gcs_encoding_to_utf8_cwd(url, encoding.lower())
        else:
            convert_gcs_encoding_to_utf8_cwd(url, encoding.lower())

    print(f"-------Processing request for URL: {url}, type: {type}, encoding: {encoding}")

    csv_file = types.Part.from_uri(file_uri=url, mime_type=checked_type)
    
    max_retries = max_retry_cnt
    for attempt in range(max_retries):
        try:
            # Generate content
            response = client.models.generate_content(
                model=gemini_model,
                contents=[csv_file, prompt],
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=response_schema,
                ),
            )
            result = response.text
            if not result or not result.strip():
                result = "[]"

            # Parse the JSON string and create a list of PIData objects
            result_json = json.loads(result)
            return [PIData(**item) for item in result_json]
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")

            error_str = str(e)
            if "400" in error_str and "INVALID_ARGUMENT" in error_str:
                attempt = max_retries

            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt + random seconds
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Failed to call Gemini API.")
                raise Exception("max_retry_cnt")

def call_gemini_with_csv(df: pd.DataFrame) -> List[PIData]:
    """
    Calls the Gemini model with a given pandas DataFrame.

    Args:
        df: The DataFrame to be analyzed.

    Returns:
        A list of PIData objects if successful, otherwise an empty list.
    """
    content = df.to_string()
    
    max_retries = max_retry_cnt
    for attempt in range(max_retries):
        try:
            # Generate content
            response = client.models.generate_content(
                model=gemini_model,
                contents=[f"{prompt}\n\n{content}"], # Combine prompt and content
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=response_schema,
                ),
            )

            result_text = response.text
            print(result_text)

            if not result_text or not result_text.strip():
                return []

            # Parse the JSON string and create a list of PIData objects
            result_json = json.loads(result_text)
            return [PIData(**item) for item in result_json]

        except Exception as e:
            print(f"Error on attempt {attempt + 1} in call_gemini_with_csv: {e}")
            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt + random seconds
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Failed to call Gemini API with string content.")
                return [] # Return empty list after all retries fail