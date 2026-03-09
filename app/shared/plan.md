# Implementation plan 

## Basic info 
- GCP Project ID: kevin-ai-playground
- 대상 파일 리스트가 들어있는 BigQuery Table `kevin-ai-playground.csv_parse_ds.csv_analysis_src_files`
- Result Dataset BigQuery Table `kevin-ai-playground.csv_parse_ds.csv_result`
- Gemini Model: gemini-2.5-flash 
- Gemini location: global 
- Gemini 호출 시 Retry Policy: exponential retry. Max retry Count: 5

## Processing 

1. BigQuery 테이블(csv_analysis_src_files)을 이용해서 GCS 의 파일을 가져와 Dataset 생성 -> 파일 리스트
1. Ray Task 로 File Processing Task 생성하여 전체 파일 리스트를 분산처리
1. Task Function 에서 파일을 읽어서 CSV 파일인지 확인하고 인코딩이 utf-8 로 되어 있는지 확인 한 후 utf-8 이 아니면 인코딩을 변경하여 GCS 에 덮어 쓰기.
1. Task Function 에서 만약 CSV 파일이고 용량이 5MiB 이하 일 때는 multimodal 로 파일 첨부하여 Gemini global region endpoint 호출해서 결과 추출 - Retry Logic
1. Task Function 에서 만약 CSV 파일이고 용량이 5MiB 초과할 때 새로운 Ray Task 를 생성
    1. CSV 파일을 gs:// url 로 read_csv 로 읽어서 
    1. iter_batches() 로 적당한 크기로 잘라서 분산 처리
    1. batch 데이터셋을 string 으로 만들어 Gemini 의 Prompt 에 넣고 System Instruction과 함께 Gemini 호출 - Retry 로직 
    1. ray.get() 해서 결과를 모아서 Dataset의 result 컬럼에 추가 
1. 결과가 담긴 Dataset 을 새로운 BigQuery 테이블에 저장 

## Gemini call 
- Structured Output 
- Model: gemini-2.5-flash 
- CSV의 content-type: text/csv

## Implementation sample code 

@./shared/main.py