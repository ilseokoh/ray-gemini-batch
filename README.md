# ray-gemini-batch

수십만개의 Gemini 호출을 수행해야 하는 과제에서는 분산/병렬처리가 필요하다. 병렬처리를 위해서 Ray 를 선택하고 GKE(Google Kubernetes Engine)에서 Ray를 운영하면서 대량의 Gemini 호출 테스크를 수행하는 인프라와 Ray용 Application에 대한 예제. 

## GKE 인프라 생성 

### gcloud 로 --enable-ray-operator 옵션과 함께 cluster를 생성

default VPC가 없다면 Cluster 생성시 --network 를 지정해야 한다. 

```bash
gcloud container clusters create-auto ray-enabled-gke \
    --enable-ray-operator \
    --enable-ray-cluster-monitoring \
    --enable-ray-cluster-logging \
    --location=asia-northeast3 \
    --network=<your vpc name> \
    --subnetwork=<your subent name>
gcloud container clusters get-credentials ray-enabled-gke --location=asia-northeast3
```

### Terraform으로 나머지 설정 

GKE AI Labs 사이트의 [Ray on GKE 문서](https://gke-ai-labs.dev/docs/tutorials/workflow-orchestration/ray-on-gke/)에 설명되어 있는 [GitHub](https://github.com/ai-on-gke/quick-start-guides)의 Terraform 의 코드를 가져왔다. 

- Google API 호출을 위한 Workload Identity Federation 설정
- Monitoring / Logging 설정 

```
git clone https://github.com/ilseokoh/ray-gemini-batch.git
cd ray-gemini-batch/terraform
```

workloads.tfvars 확인 
```yaml
project_id = "<your project id>"
cluster_name     = "ray-enabled-gke"
cluster_location = "asia-northeast3"
create_cluster    = false
# Workload identity SA 를 사용
create_service_account            = true
workload_identity_service_account = "ray-sa"
```

Terraform 실행 
```
terraform init
terraform apply --var-file=workloads.tfvars
```

GKE 상태 확인 
```
kubectl get nodes

```


```bash
gcloud container clusters get-credentials my-ray-enabled-cluster --location=us-central1
kubectl ray session my-ray-cluster
```

```
source .venv/bin/activate
ray job submit --address http://localhost:8265 --runtime-env=env.yaml -- python gemini-job.py
```


# ray-batch-gemini-inference
Gemini Batch/Parallel Processing with Ray

## Process 

1. Get file list from BigQuery Table 을 가져와서 Ray.Data 준비 - [ray.data.read_bigquery](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_bigquery.html), [Reading SQL database](https://docs.ray.io/en/latest/data/loading-data.html#reading-sql-databases)
```
import ray

# Users will need to authenticate beforehand (e.g., using gcloud auth login)

ds = ray.data.read_bigquery(
    project_id="my_project_id",
    # Specify the dataset and table, or a query
    query="SELECT * FROM `bigquery-public-data.samples.gsod` LIMIT 1000",
)

print(ds.schema())
ds.show()
```
1. [Transforming data](https://docs.ray.io/en/latest/data/quickstart.html#transforming-data) 로 데이터셋 변형 가능 
1. [Consumeing Data](https://docs.ray.io/en/latest/data/quickstart.html#consuming-data) take_batch(batch_size=3) /  iter_batches() 이렇게 해서 Ray Task 또는 Actors 로 전달 가능. [Iterating over batches](https://docs.ray.io/en/latest/data/iterating-over-data.html#iterating-over-batches)
1. Actor 를 사용해서 결과 Json 배열을 계속 누적해서 가지고 있다가 Dataset 에 넣고 
1. [Saving Data](https://docs.ray.io/en/latest/data/quickstart.html#saving-data) 
BigQuery 로 저장 
```
# Assuming 'ds' is your Ray Dataset
ds.write_bigquery(
    project_id="my_project_id",
    dataset="my_dataset_id.my_table_id",
    # Use overwrite_table=False to append to an existing table
    overwrite_table=True
)
```
1. 각각의 파일과 Prompt로 Gemini 호출. 그런데 여기서 대용량의 CSV 파일이 있는 상황. 
1. CSV 파일이 특정 크기 이상일 때만, 이하라면 multimodal 호출
1. retry 로직 필요
1. 큰 CSV 파일은 ray.put 으로 저장하고 활용
1. 3만개 파일의 시작은 Stateless 인데 (Task.function) 각 csv 파일을 나눠서 처리하고 결과를 모으려면 ??? Stateful 하다 (Actors/class)
1. Task 의 결과를 ray.get 으로 가져와서 Dataset 업데이트
1. csv 파일 첨부한 멀티모달 호출이 아니라 메모리에 있는 Dataset 를 가지고 호출하는 방법이 필요. CSV string으로 뽑아서 

1. 결과는 빈 Array 만들고 append 한다음에 ray.get (anti-pattern: Calling ray.get in a loop 참조)

1. GCS에서 파일 읽기 
```
pip install gcsfs
----
import ray

filesystem = gcsfs.GCSFileSystem(project="my-google-project")
ds = ray.data.read_parquet(
    "gs://...",
    filesystem=filesystem
)

print(ds.schema())
```
gcsfs 의 auth : https://gcsfs.readthedocs.io/en/latest/#credentials

참고자료
[어쩐지 오늘은 - Python Ray 사용법 - Python 병렬처리, 분산처리](https://zzsza.github.io/mlops/2021/01/03/python-ray/)
[Ray Design Pattern](https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit?pli=1&tab=t.0)

## Infrastructure - GKE with kubray-operator 

[Deploy GPU-accelerated Ray for AI workloads on GKE](https://docs.cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/quickstarts/ray-gpu-cluster)

 - Create a GKE cluster (Autopilot) with ray-operator 
 ```
 gcloud container clusters create-auto my-ray-enabled-cluster \
    --enable-ray-operator \
    --enable-ray-cluster-monitoring \
    --enable-ray-cluster-logging \
    --location=us-central1
 ```
 - kubectl ray plugin
 - Create custom Compute class 
 - Submit job : [ray job submit](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/cli.html#ray-job-submit-doc)

## 구현 

1. BQ Obejct 테이블을 이용해서 GCS 의 CSV 파일을 가져와 테이블을 생성 -> CSV 파일 리스트 
1. BQML 을 통해서 Gemini 호출해서 결과 저장 -> 서울리전의 gemini-2.5-flash 의 제약 때문에 30% 정도만 성공할 것으로 예상 됨 
1. Ray App 
   1. BQ Object 테이블 쿼리해서 CSV 파일 가져와 (실패한 리스트만 대상)
   2. Ray Task 로 Supervisor Task 생성하여 전체 리스트를 분산처리 
      * Worker Type 1: CSV 파일 용량이 xxx 이하 일 때는 multimodal 로 csv 파일 첨부하여 Gemini global region endpoint 호출해서 결과 추출 (retry 로직)
      * Worker Type 2: 용량이 xxx 이상일 때 1) CSV 파일을 gs:// 에서 read_csv 로 읽어들인다. 2) iter_batches() 로 적당한 크기로 잘라서 분산 후 ray.get 하고 BQ에 insert 
   3. Worker Type 2
      1. pandas 데이터셋을 str 로 만들어 Prompt 에 넣고 gemini 호출 (retry 로직) df = ds.to_pandas() print(pd.to_csv()) 또는 write_csv 다음에 멀티모달


## Monitoring 

Ray Dashboard 를 통해서 Task 1 의 Status를 보면 진행율을 알 수 있을 것으로 예상 

## GKE Workload Identity Federation 

BQ, Gemini 리소스 엑세스를 위한 설정: [GKE 워크로드에서 Google Cloud API에 인증](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity?hl=ko)

[Configuring KubeRay to use Google Cloud Storage Buckets in GKE](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/gke-gcs-bucket.html)

1. Kubernetes ServiceAccount 생성 
```
kubectl create serviceaccount ray-sa 
```
1. Kubernetes ServiceAccount를 참조하는 IAM 허용정책 
```
gcloud projects add-iam-policy-binding projects/kevin-ai-playground \
    --role=roles/bigquery.user \
    --member=principal://iam.googleapis.com/projects/834471899683/locations/global/workloadIdentityPools/kevin-ai-playground.svc.id.goog/subject/ns/default/sa/ray-sa \
    --condition=None
```


### Setup

#### 1. Create GKE Cluster 
```bash
gcloud container clusters create-auto ray-enabled-gke \
    --enable-ray-operator \
    --enable-ray-cluster-monitoring \
    --enable-ray-cluster-logging \
    --location=asia-northeast3 
```

#### 2. Configure kubectl to communicate with your cluster
```
gcloud container clusters get-credentials ray-enabled-cluster --location=asia-northeast3 
```

#### 3. Create Kubernetes ServiceAccount 
```
gcloud container clusters update ray-enabled-cluster --region=asia-northeast3 --workload-pool=kevin-ai-playground.svc.id.goog

kubectl create serviceaccount ray-user --namespace default

gcloud iam service-accounts create ray-user --display-name "Ray User"
gcloud projects add-iam-policy-binding kevin-ai-playground --member "serviceAccount:ray-user@kevin-ai-playground.iam.gserviceaccount.com" --role "roles/storage.objectUser"

gcloud projects add-iam-policy-binding kevin-ai-playground --member "serviceAccount:ray-user@kevin-ai-playground.iam.gserviceaccount.com" --role "roles/bigquery.user"

gcloud iam service-accounts add-iam-policy-binding ray-user@kevin-ai-playground.iam.gserviceaccount.com --member "serviceAccount:kevin-ai-playground.svc.id.goog[default/ray-user]" --role "roles/iam.workloadIdentityUser"

kubectl annotate serviceaccount ray-user --namespace=default iam.gke.io/gcp-service-account=ray-user@kevin-ai-playgound.iam.gserviceaccount.com

```

#### . 
```bash
gcloud workstations start-tcp-tunnel \
    --project=kevin-ai-playground \
    --region=asia-northeast3 \
    --cluster=kevin-ws-cluster \
    --config=kevin-ws-config  \
    --local-host-port=localhost:1025  \
    kevin-workstation 22
```

#### Create the custom compute class in your cluster

```
kubectl apply -f compute-class.yaml
```

#### Create Ray Cluster
```
kubectl ray create cluster ray-cluster \
      --worker-replicas=1 \
      --worker-cpu=128 \
      --worker-memory=512Gi \
      --worker-node-selectors="cloud.google.com/compute-class=n2-128-class"

kubectl ray get cluster
```
```
kubectl apply -f raycluster-config.yaml
```

```
kubectl ray delete ray-cluster
```

```
kubectl ray session ray-cluster
```


```
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install raycluster kuberay/ray-cluster --version 1.5.1
```



#### 1. Create GKE Cluster 
```bash
gcloud container clusters create-auto ray-enabled-gke \
    --enable-ray-operator \
    --enable-ray-cluster-monitoring \
    --enable-ray-cluster-logging \
    --location=asia-northeast3 
gcloud container clusters get-credentials ray-enabled-gke --location=asia-northeast3
kubectl ray session ray-cluster-kuberay
```

#### 2. Run terraform 

```bash
gcloud auth application-default login
cd terraform
terraform init
terraform apply --var-file=workloads.tfvars
```
```
kubectl get raycluster
kubectl get pods
```

#### 3. Test job running properly

```
kubectl ray session ray-cluster-kuberay
cd app
uv sync
uv run ray job submit --address http://localhost:8265 --runtime-env=env.yaml -- python bucket-test.py
cd ..
```

#### 4. 

#### Stop and Delete job 

```
uv run ray job delete --address http://localhost:8265 0f000000
```

```
uv run ray job stop --address http://localhost:8265 0f000000
```