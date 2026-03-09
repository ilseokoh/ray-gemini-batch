# Lay Data 


## Loading Data from BigQuery

### BigQuery

To read from BigQuery, install the [Python Client for Google BigQuery](https://cloud.google.com/python/docs/reference/bigquery/latest) and the [Python Client for Google BigQueryStorage](https://cloud.google.com/python/docs/reference/bigquerystorage/latest).

pip install google-cloud-bigquery
pip install google-cloud-bigquery-storage

To read data from BigQuery, call [`read_bigquery()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_bigquery.html#ray.data.read_bigquery "ray.data.read_bigquery") and specify the project id, dataset, and query (if applicable).

import ray

\# Read the entire dataset. Do not specify query.
ds \= ray.data.read\_bigquery(
    project\_id\="my\_gcloud\_project\_id",
    dataset\="bigquery-public-data.ml\_datasets.iris",
)

\# Read from a SQL query of the dataset. Do not specify dataset.
ds \= ray.data.read\_bigquery(
    project\_id\="my\_gcloud\_project\_id",
    query \= "SELECT \* FROM \`bigquery-public-data.ml\_datasets.iris\` LIMIT 50",
)

\# Write back to BigQuery
ds.write\_bigquery(
    project\_id\="my\_gcloud\_project\_id",
    dataset\="destination\_dataset.destination\_table",
    overwrite\_table\=True,
)

## Iterating over batches[#](https://docs.ray.io/en/latest/data/iterating-over-data.html#iterating-over-batches "Link to this heading")

A batch contains data from multiple rows. Iterate over batches of dataset in different formats by calling one of the following methods:

-   `Dataset.iter_batches() <ray.data.Dataset.iter_batches>`
    
-   `Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
    
-   `Dataset.to_tf() <ray.data.Dataset.to_tf>`
    

### NumPy

import ray

ds \= ray.data.read\_images("s3://anonymous@ray-example-data/image-datasets/simple")

for batch in ds.iter\_batches(batch\_size\=2, batch\_format\="numpy"):
    print(batch)

{'image': array(\[\[\[\[...\]\]\]\], dtype=uint8)}
...
{'image': array(\[\[\[\[...\]\]\]\], dtype=uint8)}

### pandas

import ray

ds \= ray.data.read\_csv("s3://anonymous@air-example-data/iris.csv")

for batch in ds.iter\_batches(batch\_size\=2, batch\_format\="pandas"):
    print(batch)

   sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
0                5.1               3.5                1.4               0.2       0
1                4.9               3.0                1.4               0.2       0
...
   sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
0                6.2               3.4                5.4               2.3       2
1                5.9               3.0                5.1               1.8       2

## Reading files from cloud storage[#](https://docs.ray.io/en/latest/data/loading-data.html#reading-files-from-cloud-storage "Link to this heading")

To read files in cloud storage, authenticate all nodes with your cloud service provider. Then, call a method like [`read_parquet()`](https://docs.ray.io/en/latest/data/api/doc/ray.data.read_parquet.html#ray.data.read_parquet "ray.data.read_parquet") and specify URIs with the appropriate schema. URIs can point to buckets, folders, or objects.

To read formats other than Parquet, see the [Loading Data API](https://docs.ray.io/en/latest/data/api/loading_data.html#loading-data-api).

### S3

To read files from Amazon S3, specify URIs with the `s3://` scheme.

import ray

ds \= ray.data.read\_parquet("s3://anonymous@ray-example-data/iris.parquet")

print(ds.schema())

Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string

Ray Data relies on PyArrow for authentication with Amazon S3. For more on how to configure your credentials to be compatible with PyArrow, see their [S3 Filesystem docs](https://arrow.apache.org/docs/python/filesystems.html#s3).

### GCS

To read files from Google Cloud Storage, install the [Filesystem interface to Google Cloud Storage](https://gcsfs.readthedocs.io/en/latest/)

pip install gcsfs

Then, create a `GCSFileSystem` and specify URIs with the `gs://` scheme.

import ray

filesystem \= gcsfs.GCSFileSystem(project\="my-google-project")
ds \= ray.data.read\_parquet(
    "gs://...",
    filesystem\=filesystem
)

print(ds.schema())

Column        Type
------        ----
sepal.length  double
sepal.width   double
petal.length  double
petal.width   double
variety       string

Ray Data relies on PyArrow for authentication with Google Cloud Storage. For more on how to configure your credentials to be compatible with PyArrow, see their [GCS Filesystem docs](https://arrow.apache.org/docs/python/filesystems.html#google-cloud-storage-file-system).