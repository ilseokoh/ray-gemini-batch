#!/bin/bash

# 이 스크립트는 지정된 ray job submit 명령어를 100번 실행합니다.

for i in {1..40}
do
  echo "Executing job $i of 40"
  uv run ray job submit --address http://localhost:8265 --runtime-env=env.yaml -- python main_job.py

  echo "Job $i finished."
  echo "---"
done

echo "All 40 jobs have been submitted."
