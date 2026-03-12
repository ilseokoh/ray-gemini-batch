# Batch Gemini call with Ray 

## Prepare Data 

GCS Bucket에 들어 있는 모든 CSV 파일을 대상으로 한다. 모든 CSV 파일을 수집하는 가장 간편한 방법은 BigQuery Object Table 생성. 

```sql 
CREATE OR REPLACE EXTERNAL TABLE `pjt-lges-midata`.`csv_parse_ds`.`csv_files_object_table`
WITH CONNECTION `asia-northeast3.llm_connection`  
  OPTIONS ( object_metadata = 'SIMPLE',
    uris = ['gs://bucket-lges-midata/csvdata/*.csv']); 
```