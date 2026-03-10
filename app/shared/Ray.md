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

## ray.wait — Ray 2.54.0

ray.wait(*ray\_waitables: [List](https://docs.python.org/3/library/typing.html#typing.List "(in Python v3.14)")\[ray.\_raylet.ObjectRef | ray.\_raylet.ObjectRefGenerator\]*, *\**, *num\_returns: [int](https://docs.python.org/3/library/functions.html#int "(in Python v3.14)") \= 1*, *timeout: [float](https://docs.python.org/3/library/functions.html#float "(in Python v3.14)") | [None](https://docs.python.org/3/library/constants.html#None "(in Python v3.14)") \= None*, *fetch\_local: [bool](https://docs.python.org/3/library/functions.html#bool "(in Python v3.14)") \= True*) → [Tuple](https://docs.python.org/3/library/typing.html#typing.Tuple "(in Python v3.14)")\[[List](https://docs.python.org/3/library/typing.html#typing.List "(in Python v3.14)")\[ray.\_raylet.ObjectRef | ray.\_raylet.ObjectRefGenerator\], [List](https://docs.python.org/3/library/typing.html#typing.List "(in Python v3.14)")\[ray.\_raylet.ObjectRef | ray.\_raylet.ObjectRefGenerator\]\][\[source\]](https://docs.ray.io/en/latest/_modules/ray/_private/worker.html#wait)[#](https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html#ray.wait "Link to this definition")

Return a list of IDs that are ready and a list of IDs that are not.

If timeout is set, the function returns either when the requested number of IDs are ready or when the timeout is reached, whichever occurs first. If it is not set, the function simply waits until that number of objects is ready and returns that exact number of object refs.

`ray_waitables` is a list of `ObjectRef` and `ObjectRefGenerator`.

The method returns two lists, ready and unready `ray_waitables`.

ObjectRef:

object refs that correspond to objects that are available in the object store are in the first list. The rest of the object refs are in the second list.

ObjectRefGenerator:

Generators whose next reference (that will be obtained via `next(generator)`) has a corresponding object available in the object store are in the first list. All other generators are placed in the second list.

Ordering of the input list of ray\_waitables is preserved. That is, if A precedes B in the input list, and both are in the ready list, then A will precede B in the ready list. This also holds true if A and B are both in the remaining list.

This method will issue a warning if it’s running inside an async context. Instead of `ray.wait(ray_waitables)`, you can use `await asyncio.wait(ray_waitables)`.

Related patterns and anti-patterns:

-   [Pattern: Using ray.wait to limit the number of pending tasks](https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html)
    
-   [Anti-pattern: Processing results in submission order using ray.get increases runtime](https://docs.ray.io/en/latest/ray-core/patterns/ray-get-submission-order.html)
    

Parameters:

-   **ray\_waitables** – List of `ObjectRef` or `ObjectRefGenerator` for objects that may or may not be ready. Note that these must be unique.
    
-   **num\_returns** – The number of ray\_waitables that should be returned.
    
-   **timeout** – The maximum amount of time in seconds to wait before returning.
    
-   **fetch\_local** – If True, wait for the object to be downloaded onto the local node before returning it as ready. If the `ray_waitable` is a generator, it will wait until the next object in the generator is downloaed. If False, ray.wait() will not trigger fetching of objects to the local node and will return immediately once the object is available anywhere in the cluster.
    

Returns:

A list of object refs that are ready and a list of the remaining object IDs.

## # ray.data.Dataset.write_bigquery — Ray 2.54.0

Dataset.write\_bigquery(*project\_id: [str](https://docs.python.org/3/library/stdtypes.html#str "(in Python v3.14)")*, *dataset: [str](https://docs.python.org/3/library/stdtypes.html#str "(in Python v3.14)")*, *max\_retry\_cnt: [int](https://docs.python.org/3/library/functions.html#int "(in Python v3.14)") \= 10*, *overwrite\_table: [bool](https://docs.python.org/3/library/functions.html#bool "(in Python v3.14)") | [None](https://docs.python.org/3/library/constants.html#None "(in Python v3.14)") \= True*, *ray\_remote\_args: [Dict](https://docs.python.org/3/library/typing.html#typing.Dict "(in Python v3.14)")\[[str](https://docs.python.org/3/library/stdtypes.html#str "(in Python v3.14)"), [Any](https://docs.python.org/3/library/typing.html#typing.Any "(in Python v3.14)")\] | [None](https://docs.python.org/3/library/constants.html#None "(in Python v3.14)") \= None*, *concurrency: [int](https://docs.python.org/3/library/functions.html#int "(in Python v3.14)") | [None](https://docs.python.org/3/library/constants.html#None "(in Python v3.14)") \= None*) → [None](https://docs.python.org/3/library/constants.html#None "(in Python v3.14)")[\[source\]](https://docs.ray.io/en/latest/_modules/ray/data/dataset.html#Dataset.write_bigquery)[#](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.write_bigquery.html#ray.data.Dataset.write_bigquery "Link to this definition")

Write the dataset to a BigQuery dataset table.

To control the number of parallel write tasks, use `.repartition()` before calling this method.

Note

This operation will trigger execution of the lazy transformations performed on this dataset.

Examples

import ray
import pandas as pd

docs \= \[{"title": "BigQuery Datasource test"} for key in range(4)\]
ds \= ray.data.from\_pandas(pd.DataFrame(docs))
ds.write\_bigquery(
    project\_id\="my\_project\_id",
    dataset\="my\_dataset\_table",
    overwrite\_table\=True
)

Parameters:

-   **project\_id** – The name of the associated Google Cloud Project that hosts the dataset to read. For more information, see details in [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects).
    
-   **dataset** – The name of the dataset in the format of `dataset_id.table_id`. The dataset is created if it doesn’t already exist.
    
-   **max\_retry\_cnt** – The maximum number of retries that an individual block write is retried due to BigQuery rate limiting errors. This isn’t related to Ray fault tolerance retries. The default number of retries is 10.
    
-   **overwrite\_table** – Whether the write will overwrite the table if it already exists. The default behavior is to overwrite the table. `overwrite_table=False` will append to the table if it exists.
    
-   **ray\_remote\_args** – Kwargs passed to [`ray.remote()`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote.html#ray.remote "ray.remote") in the write tasks.
    
-   **concurrency** – The maximum number of Ray tasks to run concurrently. Set this to control number of tasks to run concurrently. This doesn’t change the total number of tasks run. By default, concurrency is dynamically decided based on the available resources.

## ray.wait example 

```
from datetime import datetime
import time
import random
import ray

ray.init()

@ray.remote
def do_some_work(x):
    time.sleep(random.uniform(0, 4)) # 0 ~ 4초 사이 램덤으로 지연
    return x

def process_results(sum, result):
    time.sleep(1)
    sum += result
    return sum

start = datetime.now()
datas = [do_some_work.remote(x) for x in range(10)]
sum = 0
while len(datas):
    done, datas = ray.wait(datas) # 다된 작업은 done으로 넘긴다.
    sum = process_results(sum, ray.get(done[0]))
print("duration = ", datetime.now() - start)
print("results = ", sum)
```