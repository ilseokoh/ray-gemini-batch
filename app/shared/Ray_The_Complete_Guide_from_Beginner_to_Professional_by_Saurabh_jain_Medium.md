# Ray: The Complete Guide from Beginner to Professional | by Saurabh jain | Medium

## Introduction

In today’s data-driven world, the ability to process large datasets and perform complex computations efficiently is crucial. Traditional single-machine processing often hits limits when dealing with big data, machine learning models, or computationally intensive tasks. This is where **Ray** comes in — a powerful, open-source framework that makes distributed computing accessible to everyone.

Whether you’re a data scientist looking to speed up your model training, a backend engineer building scalable applications, or a researcher running complex simulations, Ray provides the tools you need to harness the power of distributed computing without the complexity typically associated with it.

## What is Ray?

Ray is a unified framework for scaling AI and Python applications. It provides:

-   **Simple APIs** for turning Python functions and classes into distributed tasks

-   **High performance** with minimal overhead
-   **Fault tolerance** and automatic recovery

-   **Flexible scheduling** that adapts to your workload
-   **Rich ecosystem** of libraries for ML, data processing, and more

## Key Benefits

1.  **Ease of Use**: Convert existing Python code to distributed with minimal changes
2.  **Performance**: Near-linear scaling across multiple machines
3.  **Flexibility**: Works with any Python library (NumPy, Pandas, PyTorch, etc.)
4.  **Production Ready**: Used by companies like Uber, Netflix, and Pinterest

## Ray vs. Other Frameworks

| Feature | Ray | Spark | Dask | Celery |

| — — — — -| — — -| — — — -| — — — | — — — — |

| Learning Curve | Low | Medium | Low | Medium |

| Performance | Excellent | Good | Good | Fair |

| ML Support | Native | Limited | Good | Limited |

| Fault Tolerance | Built-in | Built-in | Limited | Manual |

## Get Saurabh jain’s stories in your inbox

Join Medium for free to get updates from this writer.

| Streaming | Yes | Yes | No | No |

## Installation and Setup

## Basic Installation

  
pip install ray  
  
pip install "ray\[default\]"  
  
pip install "ray\[all\]"

## Cloud Installation

  
pip install "ray\[default\]" boto3  
  
pip install "ray\[default\]" google-cloud-storage  
  
pip install "ray\[default\]" azure-storage-blob

## Docker Setup

FROM python:3.9\-slim  
RUN pip install "ray\[default\]"  
WORKDIR /app  
COPY . .  
CMD \["python", "your\_ray\_app.py"\]

## Verification

import ray  
print(f"Ray version: {ray.\_\_version\_\_}")  
  
ray.init()  
print(f"Ray is running on: {ray.get\_runtime\_context().node\_id}")  
ray.shutdown()

## Core Concepts

## 1\. Tasks vs. Actors

**Tasks** are stateless functions that run once and return a result:

@ray.remote  
def compute\_pi(n):  
    return 4 \* sum(1/((2\*i+1) \* (-1)\*\*i) for i in range(n))

**Actors** are stateful classes that persist across multiple method calls:

@ray.remote  
class Counter:  
    def \_\_init\_\_(self):  
        self.value = 0  
    def increment(self):  
        self.value += 1  
        return self.value

## 2\. Object Store

Ray’s distributed object store allows efficient sharing of large objects between tasks:

import numpy as np  
  
large\_array = np.random.rand(1000000)  
ref = ray.put(large\_array)  
@ray.remote  
def process\_array(array\_ref):  
      
    array = ray.get(array\_ref)  
    return np.sum(array)

## 3\. Scheduling

Ray’s scheduler automatically:

-   Distributes tasks across available resources

-   Handles load balancing
-   Manages dependencies between tasks

-   Provides fault tolerance

## Getting Started — Your First Ray Program

Let’s start with a simple example that demonstrates the power of Ray:

## Sequential Version (Slow)

import time  
import requests  
def fetch\_url(url):  
    """Fetch a URL - this is slow due to network I/O"""  
    response = requests.get(url)  
    return len(response.content)  
  
urls = \[  
    "https://httpbin.org/delay/1",  
    "https://httpbin.org/delay/1",   
    "https://httpbin.org/delay/1",  
    "https://httpbin.org/delay/1"  
\]  
start\_time = time.time()  
results = \[fetch\_url(url) for url in urls\]  
print(f"Sequential time: {time.time() - start\_time:.2f}s")  
print(f"Results: {results}")

## Ray Version (Fast)

import ray  
import time  
import requests  
  
ray.init()  
@ray.remote  
def fetch\_url\_remote(url):  
    """Same function, now distributed"""  
    response = requests.get(url)  
    return len(response.content)  
  
urls = \[  
    "https://httpbin.org/delay/1",  
    "https://httpbin.org/delay/1",   
    "https://httpbin.org/delay/1",  
    "https://httpbin.org/delay/1"  
\]  
start\_time = time.time()  
  
futures = \[fetch\_url\_remote.remote(url) for url in urls\]  
  
results = ray.get(futures)  
print(f"Ray time: {time.time() - start\_time:.2f}s")  
print(f"Results: {results}")  
ray.shutdown()

**Key Observations:**

-   Sequential: ~4 seconds (1 second per URL)

-   Ray: ~1 second (all URLs fetched in parallel)
-   Only added `@ray.remote` decorator and changed function calls!

## Remote Functions (@ray.remote)

Remote functions are the building blocks of Ray applications. They allow you to execute Python functions asynchronously across your cluster.

## Basic Syntax

import ray  
ray.init()  
@ray.remote  
def regular\_function(x, y):  
    return x + y  
  
future = regular\_function.remote(2, 3)  
result = ray.get(future)  

## Resource Specification

You can specify resource requirements for functions:

@ray.remote(num\_cpus=2, num\_gpus=1, memory=1000)  
def gpu\_intensive\_task(data):  
    import torch  
    # GPU computation here  
    return processed\_data  
@ray.remote(num\_cpus=0.5)  # Use half a CPU  
def lightweight\_task(x):  
    return x \* 2

## Return Multiple Values

@ray.remote(num\_returns=3)  
def process\_data():  
    result1 = "processed\_data\_1"  
    result2 = "processed\_data\_2"   
    result3 = "metadata"  
    return result1, result2, result3  
\# Get all returns  
r1, r2, r3 = ray.get(process\_data.remote())

## Advanced Example: Parallel Data Processing

import pandas as pd  
import numpy as np  
@ray.remote  
def process\_chunk(chunk\_data, operations):  
    """Process a chunk of data with specified operations"""  
    df = pd.DataFrame(chunk\_data)  
    for operation in operations:  
        if operation == 'normalize':  
            df = (df - df.mean()) / df.std()  
        elif operation == 'square':  
            df = df \*\* 2  
        elif operation == 'log':  
            df = np.log1p(df.abs())  
    return df.to\_dict('records')  
  
large\_dataset = np.random.rand(10000, 10)  
chunk\_size = 1000  
chunks = \[large\_dataset\[i:i+chunk\_size\]   
          for i in range(0, len(large\_dataset), chunk\_size)\]  
operations = \['normalize', 'square', 'log'\]  
  
futures = \[process\_chunk.remote(chunk, operations)   
           for chunk in chunks\]  
  
processed\_chunks = ray.get(futures)  
  
final\_result = \[\]  
for chunk in processed\_chunks:  
    final\_result.extend(chunk)  
print(f"Processed {len(final\_result)} records")

## Remote Classes (Actors)

Actors are stateful workers that can maintain state between method calls. They’re perfect for scenarios where you need to:

-   Maintain state across multiple operations

-   Implement services or long-running processes
-   Control access to shared resources

## Basic Actor

@ray.remote  
class SimpleCounter:  
    def \_\_init\_\_(self, initial\_value=0):  
        self.value = initial\_value  
    def increment(self, delta=1):  
            self.value += delta  
            return self.value  
    def get\_value(self):  
        return self.value  
    def reset(self):  
        self.value = 0  
  
counter1 = SimpleCounter.remote(initial\_value=10)  
counter2 = SimpleCounter.remote()  
  
future1 = counter1.increment.remote(5)  
future2 = counter2.increment.remote(3)  
print(ray.get(future1))    
print(ray.get(future2))  

## Advanced Actor: In-Memory Database

@ray.remote  
class InMemoryDB:  
    def \_\_init\_\_(self):  
        self.data = {}  
        self.indexes = {}  
    def put(self, key, value):  
            """Store a key-value pair"""  
            self.data\[key\] = value  
              
            value\_type = type(value).\_\_name\_\_  
            if value\_type not in self.indexes:  
                self.indexes\[value\_type\] = \[\]  
            self.indexes\[value\_type\].append(key)  
            return True  
    def get(self, key):  
        """Retrieve value by key"""  
        return self.data.get(key)  
    def query\_by\_type(self, value\_type):  
        """Query all keys of a specific type"""  
        keys = self.indexes.get(value\_type, \[\])  
        return {key: self.data\[key\] for key in keys}  
    def size(self):  
        """Get database size"""  
        return len(self.data)  
    def backup(self):  
        """Return a copy of all data"""  
        return dict(self.data)  
  
db = InMemoryDB.remote()  
  
ray.get(db.put.remote("user\_1", {"name": "Alice", "age": 30}))  
ray.get(db.put.remote("user\_2", {"name": "Bob", "age": 25}))  
ray.get(db.put.remote("count", 42))  
ray.get(db.put.remote("pi", 3.14159))  
  
user\_data = ray.get(db.query\_by\_type.remote("dict"))  
print(f"Users: {user\_data}")  
size = ray.get(db.size.remote())  
print(f"Database size: {size}")

## Actor Patterns: Worker Pool

@ray.remote  
class Worker:  
    def \_\_init\_\_(self, worker\_id):  
        self.worker\_id = worker\_id  
        self.processed\_count = 0  
    def process\_task(self, task\_data):  
        """Simulate task processing"""  
        import time  
        import random  
          
        time.sleep(random.uniform(0.1, 0.5))  
        self.processed\_count += 1  
        result = {  
            'worker\_id': self.worker\_id,  
            'task\_data': task\_data,  
            'processed\_count': self.processed\_count,  
            'result': task\_data \*\* 2  
        }  
        return result  
    def get\_stats(self):  
        return {  
            'worker\_id': self.worker\_id,  
            'processed\_count': self.processed\_count  
        }  
  
num\_workers = 4  
workers = \[Worker.remote(i) for i in range(num\_workers)\]  
  
tasks = list(range(20))  
futures = \[\]  
for i, task in enumerate(tasks):  
    worker = workers\[i % num\_workers\]    
    future = worker.process\_task.remote(task)  
    futures.append(future)  
  
results = ray.get(futures)  
  
stats\_futures = \[worker.get\_stats.remote() for worker in workers\]  
stats = ray.get(stats\_futures)  
print("Results:")  
for result in results\[:5\]:    
    print(f"Worker {result\['worker\_id'\]}: {result\['task\_data'\]} -> {result\['result'\]}")  
print("\\nWorker Statistics:")  
for stat in stats:  
    print(f"Worker {stat\['worker\_id'\]}: {stat\['processed\_count'\]} tasks processed")

## Advanced Patterns

## 1\. Pipeline Processing

Create complex data pipelines where output of one stage feeds into the next:

@ray.remote  
def extract\_data(source):  
    """Extract data from source"""  
    import time  
    time.sleep(0.5)    
    return f"raw\_data\_from\_{source}"  
@ray.remote  
def transform\_data(raw\_data, transformation\_type):  
    """Transform raw data"""  
    import time  
    time.sleep(0.3)    
    return f"transformed\_{raw\_data}\_{transformation\_type}"  
@ray.remote  
def load\_data(transformed\_data, destination):  
    """Load data to destination"""  
    import time  
    time.sleep(0.2)    
    return f"loaded\_{transformed\_data}\_to\_{destination}"  
  
sources = \["database\_1", "database\_2", "api\_endpoint", "file\_system"\]  
  
extraction\_futures = \[extract\_data.remote(source) for source in sources\]  
  
transformation\_futures = \[\]  
for future in extraction\_futures:  
    transformed = transform\_data.remote(future, "normalize")  
    transformation\_futures.append(transformed)  
  
loading\_futures = \[\]  
for future in transformation\_futures:  
    loaded = load\_data.remote(future, "data\_warehouse")  
    loading\_futures.append(loaded)  
  
results = ray.get(loading\_futures)  
print("Pipeline Results:")  
for result in results:  
    print(f"  {result}")

## 2\. Map-Reduce Pattern

Implement distributed map-reduce operations:

@ray.remote  
def map\_function(data\_chunk):  
    """Map phase: process chunk and emit key-value pairs"""  
    word\_counts = {}  
    for line in data\_chunk:  
        words = line.strip().split()  
        for word in words:  
            word\_counts\[word\] = word\_counts.get(word, 0) + 1  
    return word\_counts  
@ray.remote  
def reduce\_function(word\_count\_dicts):  
    """Reduce phase: combine all word counts"""  
    final\_counts = {}  
    for word\_dict in word\_count\_dicts:  
        for word, count in word\_dict.items():  
            final\_counts\[word\] = final\_counts.get(word, 0) + count  
    return final\_counts  
\# Sample data  
text\_data = \[  
    \["hello world", "hello ray", "world of distributed computing"\],  
    \["ray is awesome", "distributed computing with ray"\],  
    \["hello distributed world", "ray makes it easy"\],  
    \["world class performance", "hello performance"\]  
\]  
\# Map phase  
map\_futures = \[map\_function.remote(chunk) for chunk in text\_data\]  
map\_results = ray.get(map\_futures)  
\# Reduce phase    
final\_result = ray.get(reduce\_function.remote(map\_results))  
print("Word Counts:", final\_result)

## 3\. Dynamic Task Generation

Generate tasks dynamically based on intermediate results:

@ray.remote  
def process\_number(n, depth=0, max\_depth=3):  
    """Process number and potentially generate more tasks"""  
    import random  
    if depth >= max\_depth:  
        return n \* 2  
      
    num\_subtasks = random.randint(1, 3)  
    if num\_subtasks == 1:  
          
        return n \* 2  
    else:  
          
        subtask\_futures = \[\]  
        for i in range(num\_subtasks):  
            new\_n = n + random.randint(1, 10)  
            future = process\_number.remote(new\_n, depth + 1, max\_depth)  
            subtask\_futures.append(future)  
          
        subtask\_results = ray.get(subtask\_futures)  
        return sum(subtask\_results)  
  
initial\_numbers = \[1, 2, 3, 4, 5\]  
initial\_futures = \[process\_number.remote(n) for n in initial\_numbers\]  
  
results = ray.get(initial\_futures)  
print("Dynamic task results:", results)

## Ray Libraries Ecosystem

Ray provides specialized libraries for different use cases:

## 1\. Ray Data — Distributed Data Processing

import ray  
  
ds = ray.data.read\_parquet("s3://my-bucket/data/")  
  
ds = ray.data.from\_items(\[{"id": i, "value": i\*\*2} for i in range(1000)\])  
  
ds = ds.map(lambda row: {"id": row\["id"\], "value\_doubled": row\["value"\] \* 2})  
  
ds = ds.filter(lambda row: row\["value\_doubled"\] > 100)  
  
total = ds.map(lambda row: row\["value\_doubled"\]).sum()  
print(f"Total: {total}")  
  
ds.write\_parquet("s3://my-bucket/processed/")

## 2\. Ray Train — Distributed Training

import ray  
from ray import train  
from ray.train import ScalingConfig  
import torch  
import torch.nn as nn  
def train\_func():  
    """Training function that runs on each worker"""  
    model = nn.Linear(10, 1)  
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)  
    for epoch in range(100):  
          
        optimizer.zero\_grad()  
          
          
        train.report({"epoch": epoch, "loss": 0.1})  
  
trainer = train.torch.TorchTrainer(  
    train\_func,  
    scaling\_config=ScalingConfig(  
        num\_workers=4,  
        use\_gpu=True  
    )  
)  
  
result = trainer.fit()

## 3\. Ray Tune — Hyperparameter Optimization

from ray import tune  
from ray.tune.schedulers import ASHAScheduler  
def objective(config):  
    """Objective function to optimize"""  
    import random  
    import time  
      
    accuracy = 1 - (config\["lr"\] - 0.01)\*\*2 - random.uniform(0, 0.1)  
    for step in range(100):  
          
        tune.report(accuracy=accuracy + step \* 0.001)  
        time.sleep(0.01)  
  
search\_space = {  
    "lr": tune.uniform(0.001, 0.1),  
    "batch\_size": tune.choice(\[16, 32, 64, 128\])  
}  
  
tuner = tune.Tuner(  
    objective,  
    param\_space=search\_space,  
    tune\_config=tune.TuneConfig(  
        scheduler=ASHAScheduler(metric="accuracy", mode="max"),  
        num\_samples=20  
    )  
)  
results = tuner.fit()  
best\_result = results.get\_best\_result(metric="accuracy", mode="max")  
print(f"Best config: {best\_result.config}")

## 4\. Ray Serve — Model Serving

from ray import serve  
import pandas as pd  
@serve.deployment(num\_replicas=3)  
class MLModel:  
    def \_\_init\_\_(self):  
          
        self.model = self.\_load\_model()  
    def \_load\_model(self):  
          
        return lambda x: x \* 2    
    def predict(self, data):  
          
        processed\_data = self.\_preprocess(data)  
          
        prediction = self.model(processed\_data)  
        return {"prediction": prediction}  
    def \_preprocess(self, data):  
          
        return float(data.get("value", 0))  
  
serve.start()  
MLModel.deploy()  
  
import requests  
response = requests.post(  
    "http://localhost:8000/MLModel/predict",  
    json={"value": 10}  
)  
print(response.json())

## Production Deployments

## 1\. Local Cluster Setup

  
ray.init(  
    num\_cpus=8,  
    num\_gpus=2,  
    memory=16\_000\_000\_000,    
    object\_store\_memory=8\_000\_000\_000    
)

## 2\. Multi-Node Cluster

**Head Node:**

\# Start head node  
ray start 

**Worker Nodes:**

\# Connect worker nodes  
ray start 

**In Python:**

  
ray.init(address="ray://head\_node\_ip:10001")

## 3\. Kubernetes Deployment

  
apiVersion: ray.io/v1alpha1  
kind: RayCluster  
metadata:  
  name: ray-cluster  
spec:  
  rayVersion: '2.0.0'  
  headGroupSpec:  
    replicas: 1  
    rayStartParams:  
      dashboard-host: '0.0.0.0'  
    template:  
      spec:  
        containers:  
        \- name: ray-head  
          image: rayproject/ray:2.0.0  
          resources:  
            limits:  
              cpu: 2  
              memory: 4Gi  
            requests:  
              cpu: 2  
              memory: 4Gi  
  workerGroupSpecs:  
  \- replicas: 3  
    minReplicas: 1  
    maxReplicas: 10  
    groupName: worker-group  
    rayStartParams: {}  
    template:  
      spec:  
        containers:  
        \- name: ray-worker  
          image: rayproject/ray:2.0.0  
          resources:  
            limits:  
              cpu: 2  
              memory: 4Gi  
            requests:  
              cpu: 2  
              memory: 4Gi

## 4\. AWS/Cloud Deployment

  
from ray.autoscaler.aws.config import bootstrap\_aws  
config = {  
    "cluster\_name": "my-ray-cluster",  
    "provider": {  
        "type": "aws",  
        "region": "us-west-2",  
    },  
    "auth": {  
        "ssh\_user": "ubuntu"  
    },  
    "head\_node": {  
        "InstanceType": "m5.large",  
        "ImageId": "ami-0a2363a9cff180a64"    
    },  
    "worker\_nodes": {  
        "InstanceType": "m5.large",  
        "ImageId": "ami-0a2363a9cff180a64",  
        "min\_workers": 0,  
        "max\_workers": 10  
    }  
}  
  

## Performance Optimization

## 1\. Object Store Optimization

import numpy as np  
  
@ray.remote  
def process\_large\_array():  
    large\_array = np.random.rand(1000000)  
      
    ref = ray.put(large\_array)  
    return ref  
  
array\_ref = ray.get(process\_large\_array.remote())  
@ray.remote  
def compute\_sum(array\_ref):  
    array = ray.get(array\_ref)  
    return np.sum(array)  
@ray.remote    
def compute\_mean(array\_ref):  
    array = ray.get(array\_ref)  
    return np.mean(array)  
  
sum\_future = compute\_sum.remote(array\_ref)  
mean\_future = compute\_mean.remote(array\_ref)  
results = ray.get(\[sum\_future, mean\_future\])

## 2\. Batching for Efficiency

  
@ray.remote  
def process\_single\_item(item):  
    return item \*\* 2  
  
@ray.remote  
def process\_batch(items):  
    return \[item \*\* 2 for item in items\]  
  
items = list(range(10000))  
  
  
  
batch\_size = 1000  
batches = \[items\[i:i+batch\_size\] for i in range(0, len(items), batch\_size)\]  
futures = \[process\_batch.remote(batch) for batch in batches\]  
results = \[\]  
for batch\_result in ray.get(futures):  
    results.extend(batch\_result)

## 3\. Resource Management

  
@ray.remote(num\_cpus=4, num\_gpus=1, memory=2000)  
def memory\_intensive\_task():  
    import numpy as np  
      
    large\_matrix = np.random.rand(10000, 10000)  
    return np.linalg.det(large\_matrix)  
  
@ray.remote(resources={"custom\_resource": 1})  
def special\_hardware\_task():  
      
    pass

## 4\. Async Patterns

import asyncio  
@ray.remote  
class AsyncActor:  
    async def async\_method(self, x):  
        await asyncio.sleep(1)    
        return x \* 2  
    def sync\_method(self, x):  
        return x + 1  
  
actor = AsyncActor.remote()  
  
futures = \[actor.async\_method.remote(i) for i in range(10)\]  
results = ray.get(futures)

## Best Practices

## 1\. Error Handling and Fault Tolerance

@ray.remote(max\_retries=3, retry\_exceptions=True)  
def unreliable\_task(data):  
    import random  
    if random.random() < 0.3:    
        raise Exception("Random failure")  
    return data \* 2  
  
def safe\_process(items):  
    futures = \[unreliable\_task.remote(item) for item in items\]  
    results = \[\]  
    for future in futures:  
        try:  
            result = ray.get(future)  
            results.append(result)  
        except Exception as e:  
            print(f"Task failed: {e}")  
            results.append(None)    
    return results

## 2\. Memory Management

  
large\_data = np.random.rand(1000000)  
data\_ref = ray.put(large\_data)  
@ray.remote  
def process\_with\_shared\_data(data\_ref, params):  
    data = ray.get(data\_ref)    
      
    return result  
  
del data\_ref

## 3\. Monitoring and Debugging

  
import logging  
logging.basicConfig(level=logging.INFO)  
@ray.remote  
def monitored\_task(x):  
    import time  
    start\_time = time.time()  
      
    result = x \*\* 2  
    time.sleep(0.1)    
    end\_time = time.time()  
    print(f"Task completed in {end\_time - start\_time:.2f}s")  
    return result  
  

## 4\. Testing Ray Applications

import pytest  
@pytest.fixture  
def ray\_setup():  
    ray.init(local\_mode=True)    
    yield  
    ray.shutdown()  
def test\_ray\_function(ray\_setup):  
    @ray.remote  
    def add\_numbers(a, b):  
        return a + b  
    result = ray.get(add\_numbers.remote(2, 3))  
    assert result == 5  
def test\_ray\_actor(ray\_setup):  
    @ray.remote  
    class Counter:  
        def \_\_init\_\_(self):  
            self.value = 0  
        def increment(self):  
            self.value += 1  
            return self.value  
    counter = Counter.remote()  
    result = ray.get(counter.increment.remote())  
    assert result == 1

## Real-World Examples

## 1\. Distributed Web Scraping

import requests  
from bs4 import BeautifulSoup  
import ray  
from urllib.parse import urljoin, urlparse  
import time  
@ray.remote  
class URLFetcher:  
    def \_\_init\_\_(self):  
        self.session = requests.Session()  
        self.session.headers.update({  
            'User-Agent': 'Mozilla/5.0 (compatible; RayBot/1.0)'  
        })  
    def fetch\_url(self, url):  
        try:  
            response = self.session.get(url, timeout=10)  
            response.raise\_for\_status()  
            return {  
                'url': url,  
                'status\_code': response.status\_code,  
                'content\_length': len(response.content),  
                'title': self.\_extract\_title(response.content),  
                'links': self.\_extract\_links(response.content, url)  
            }  
        except Exception as e:  
            return {  
                'url': url,  
                'error': str(e),  
                'status\_code': None  
            }  
    def \_extract\_title(self, content):  
        try:  
            soup = BeautifulSoup(content, 'html.parser')  
            title\_tag = soup.find('title')  
            return title\_tag.text.strip() if title\_tag else 'No title'  
        except:  
            return 'Parse error'  
    def \_extract\_links(self, content, base\_url):  
        try:  
            soup = BeautifulSoup(content, 'html.parser')  
            links = \[\]  
            for link in soup.find\_all('a', href=True):  
                href = link\['href'\]  
                full\_url = urljoin(base\_url, href)  
                if urlparse(full\_url).netloc:    
                    links.append(full\_url)  
            return links\[:10\]    
        except:  
            return \[\]  
  
@ray.remote  
def scrape\_websites(urls, num\_workers=5):  
      
    fetchers = \[URLFetcher.remote() for \_ in range(num\_workers)\]  
      
    futures = \[\]  
    for i, url in enumerate(urls):  
        fetcher = fetchers\[i % num\_workers\]  
        future = fetcher.fetch\_url.remote(url)  
        futures.append(future)  
      
    results = ray.get(futures)  
      
    successful = \[r for r in results if 'error' not in r\]  
    failed = \[r for r in results if 'error' in r\]  
    return {  
        'successful\_count': len(successful),  
        'failed\_count': len(failed),  
        'results': successful,  
        'errors': failed  
    }  
  
if \_\_name\_\_ == "\_\_main\_\_":  
    ray.init()  
    urls\_to\_scrape = \[  
        'https://www.python.org',  
        'https://www.github.com',  
        'https://stackoverflow.com',  
        'https://www.reddit.com',  
        'https://news.ycombinator.com'  
    \]  
    start\_time = time.time()  
    results = ray.get(scrape\_websites.remote(urls\_to\_scrape))  
    end\_time = time.time()  
    print(f"Scraping completed in {end\_time - start\_time:.2f} seconds")  
    print(f"Successful: {results\['successful\_count'\]}")  
    print(f"Failed: {results\['failed\_count'\]}")  
    for result in results\['results'\]:  
        print(f"  {result\['url'\]}: {result\['title'\]}")

## 2\. Distributed Machine Learning Pipeline

import numpy as np  
import pandas as pd  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.model\_selection import train\_test\_split  
from sklearn.metrics import accuracy\_score  
import ray  
@ray.remote  
class DataPreprocessor:  
    def \_\_init\_\_(self):  
        self.scalers = {}  
    def fit\_transform(self, data, target\_column):  
        """Fit preprocessor and transform data"""  
        df = pd.DataFrame(data)  
          
        X = df.drop(columns=\[target\_column\])  
        y = df\[target\_column\]  
          
        X\_encoded = pd.get\_dummies(X)  
          
        numerical\_cols = X\_encoded.select\_dtypes(include=\[np.number\]).columns  
        for col in numerical\_cols:  
            mean\_val = X\_encoded\[col\].mean()  
            std\_val = X\_encoded\[col\].std()  
            self.scalers\[col\] = {'mean': mean\_val, 'std': std\_val}  
            X\_encoded\[col\] = (X\_encoded\[col\] - mean\_val) / (std\_val + 1e-8)  
        return X\_encoded.values, y.values  
    def transform(self, data):  
        """Transform new data using fitted preprocessor"""  
        df = pd.DataFrame(data)  
        X\_encoded = pd.get\_dummies(df)  
        for col, scaler in self.scalers.items():  
            if col in X\_encoded.columns:  
                X\_encoded\[col\] = (X\_encoded\[col\] - scaler\['mean'\]) / (scaler\['std'\] + 1e-8)  
        return X\_encoded.values  
@ray.remote  
class ModelTrainer:  
    def \_\_init\_\_(self, model\_params=None):  
        self.model\_params = model\_params or {  
            'n\_estimators': 100,  
            'random\_state': 42  
        }  
        self.model = None  
    def train(self, X\_train, y\_train):  
        """Train the model"""  
        self.model = RandomForestClassifier(\*\*self.model\_params)  
        self.model.fit(X\_train, y\_train)  
          
        train\_pred = self.model.predict(X\_train)  
        train\_accuracy = accuracy\_score(y\_train, train\_pred)  
        return {  
            'train\_accuracy': train\_accuracy,  
            'feature\_count': X\_train.shape\[1\],  
            'sample\_count': X\_train.shape\[0\]  
        }  
    def predict(self, X):  
        """Make predictions"""  
        if self.model is None:  
            raise ValueError("Model not trained yet")  
        return self.model.predict(X)  
    def evaluate(self, X\_test, y\_test):  
        """Evaluate model performance"""  
        predictions = self.predict(X\_test)  
        accuracy = accuracy\_score(y\_test, predictions)  
        return {  
            'test\_accuracy': accuracy,  
            'predictions': predictions.tolist()  
        }  
@ray.remote  
def create\_synthetic\_data(n\_samples=1000, n\_features=20, random\_seed=None):  
    """Create synthetic dataset for demonstration"""  
    if random\_seed:  
        np.random.seed(random\_seed)  
      
    X = np.random.randn(n\_samples, n\_features)  
      
    target = (X\[:, 0\] + X\[:, 1\] - X\[:, 2\] + np.random.normal(0, 0.1, n\_samples)) > 0  
      
    feature\_names = \[f'feature\_{i}' for i in range(n\_features)\]  
    data = pd.DataFrame(X, columns=feature\_names)  
    data\['target'\] = target.astype(int)  
    return data.to\_dict('records')  
@ray.remote  
def ml\_pipeline(n\_samples=1000, test\_size=0.2, random\_seed=42):  
    """Complete ML pipeline"""  
    print("Starting ML pipeline...")  
      
    print("Generating synthetic data...")  
    data\_future = create\_synthetic\_data.remote(  
        n\_samples=n\_samples,   
        random\_seed=random\_seed  
    )  
    data = ray.get(data\_future)  
      
    print("Preprocessing data...")  
    preprocessor = DataPreprocessor.remote()  
    X, y = ray.get(preprocessor.fit\_transform.remote(data, 'target'))  
      
    print("Splitting data...")  
    X\_train, X\_test, y\_train, y\_test = train\_test\_split(  
        X, y, test\_size=test\_size, random\_state=random\_seed  
    )  
      
    print("Training models...")  
    model\_configs = \[  
        {'n\_estimators': 50, 'max\_depth': 5, 'random\_state': random\_seed},  
        {'n\_estimators': 100, 'max\_depth': 10, 'random\_state': random\_seed},  
        {'n\_estimators': 200, 'max\_depth': 15, 'random\_state': random\_seed}  
    \]  
    trainers = \[ModelTrainer.remote(config) for config in model\_configs\]  
    train\_futures = \[trainer.train.remote(X\_train, y\_train) for trainer in trainers\]  
    train\_results = ray.get(train\_futures)  
      
    print("Evaluating models...")  
    eval\_futures = \[trainer.evaluate.remote(X\_test, y\_test) for trainer in trainers\]  
    eval\_results = ray.get(eval\_futures)  
      
    best\_idx = np.argmax(\[result\['test\_accuracy'\] for result in eval\_results\])  
    best\_trainer = trainers\[best\_idx\]  
    print("Pipeline completed!")  
    return {  
        'best\_model\_idx': best\_idx,  
        'best\_config': model\_configs\[best\_idx\],  
        'train\_results': train\_results,  
        'eval\_results': eval\_results,  
        'best\_trainer': best\_trainer  
    }  
  
if \_\_name\_\_ == "\_\_main\_\_":  
    ray.init()  
      
    start\_time = time.time()  
    pipeline\_result = ray.get(ml\_pipeline.remote(n\_samples=5000))  
    end\_time = time.time()  
    print(f"\\nPipeline completed in {end\_time - start\_time:.2f} seconds")  
    print(f"Best model configuration: {pipeline\_result\['best\_config'\]}")  
    print("\\nModel comparison:")  
    for i, (train\_res, eval\_res) in enumerate(zip(  
        pipeline\_result\['train\_results'\],   
        pipeline\_result\['eval\_results'\]  
    )):  
        print(f"  Model {i+1}: Train Acc = {train\_res\['train\_accuracy'\]:.3f}, "  
              f"Test Acc = {eval\_res\['test\_accuracy'\]:.3f}")

## 3\. Real-Time Data Processing System

import ray  
import time  
import random  
from collections import defaultdict, deque  
import json  
from datetime import datetime, timedelta  
@ray.remote  
class DataGenerator:  
    """Simulates real-time data stream"""  
    def \_\_init\_\_(self, source\_id):  
        self.source\_id = source\_id  
        self.running = False  
    def start\_generating(self, duration\_seconds=60):  
        """Generate data for specified duration"""  
        self.running = True  
        start\_time = time.time()  
        while self.running and (time.time() - start\_time) < duration\_seconds:  
              
            event\_type = random.choice(\['user\_action', 'system\_metric', 'error'\])  
            data = {  
                'timestamp': datetime.now().isoformat(),  
                'source\_id': self.source\_id,  
                'event\_type': event\_type,  
                'value': random.uniform(0, 100),  
                'metadata': {  
                    'user\_id': random.randint(1, 1000),  
                    'session\_id': f"session\_{random.randint(1, 100)}"  
                }  
            }  
            yield data  
            time.sleep(random.uniform(0.1, 0.5))    
    def stop(self):  
        self.running = False  
@ray.remote  
class StreamProcessor:  
    """Processes streaming data in real-time"""  
    def \_\_init\_\_(self, processor\_id):  
        self.processor\_id = processor\_id  
        self.processed\_count = 0  
        self.error\_count = 0  
        self.recent\_data = deque(maxlen=100)    
    def process\_batch(self, data\_batch):  
        """Process a batch of streaming data"""  
        results = \[\]  
        for data in data\_batch:  
            try:  
                processed = self.\_process\_single\_item(data)  
                results.append(processed)  
                self.recent\_data.append(processed)  
                self.processed\_count += 1  
            except Exception as e:  
                self.error\_count += 1  
                results.append({  
                    'error': str(e),  
                    'original\_data': data,  
                    'timestamp': datetime.now().isoformat()  
                })  
        return {  
            'processor\_id': self.processor\_id,  
            'processed\_items': len(results),  
            'results': results,  
            'stats': self.get\_stats()  
        }  
    def \_process\_single\_item(self, data):  
        """Process individual data item"""  
          
        event\_type = data\['event\_type'\]  
        value = data\['value'\]  
        if event\_type == 'user\_action':  
              
            engagement\_score = min(value \* 1.2, 100)  
            return {  
                'type': 'engagement',  
                'score': engagement\_score,  
                'user\_id': data\['metadata'\]\['user\_id'\],  
                'timestamp': data\['timestamp'\]  
            }  
        elif event\_type == 'system\_metric':  
              
            is\_anomaly = value > 90 or value < 10  
            return {  
                'type': 'system\_health',  
                'value': value,  
                'is\_anomaly': is\_anomaly,  
                'source': data\['source\_id'\],  
                'timestamp': data\['timestamp'\]  
            }  
        elif event\_type == 'error':  
            return {  
                'type': 'alert',  
                'severity': 'high' if value > 70 else 'medium',  
                'source': data\['source\_id'\],  
                'timestamp': data\['timestamp'\]  
            }  
    def get\_stats(self):  
        return {  
            'processed\_count': self.processed\_count,  
            'error\_count': self.error\_count,  
            'recent\_items\_count': len(self.recent\_data)  
        }  
@ray.remote  
class StreamAggregator:  
    """Aggregates processed streaming data"""  
    def \_\_init\_\_(self):  
        self.aggregated\_data = defaultdict(list)  
        self.alerts = \[\]  
        self.metrics = defaultdict(float)  
    def aggregate\_batch(self, processed\_batch):  
        """Aggregate a batch of processed data"""  
        for item in processed\_batch\['results'\]:  
            if isinstance(item, dict) and 'type' in item:  
                item\_type = item\['type'\]  
                self.aggregated\_data\[item\_type\].append(item)  
                  
                if item\_type == 'system\_health' and item.get('is\_anomaly'):  
                    self.alerts.append({  
                        'alert\_type': 'anomaly\_detected',  
                        'details': item,  
                        'timestamp': datetime.now().isoformat()  
                    })  
                  
                if item\_type == 'alert' and item.get('severity') == 'high':  
                    self.alerts.append({  
                        'alert\_type': 'critical\_error',  
                        'details': item,  
                        'timestamp': datetime.now().isoformat()  
                    })  
          
        self.metrics\['total\_processed'\] += len(processed\_batch\['results'\])  
        self.metrics\['alerts\_generated'\] = len(self.alerts)  
        return {  
            'aggregated\_count': len(self.aggregated\_data),  
            'alert\_count': len(self.alerts),  
            'metrics': dict(self.metrics)  
        }  
    def get\_summary(self):  
        """Get current summary of aggregated data"""  
        summary = {}  
        for data\_type, items in self.aggregated\_data.items():  
            summary\[data\_type\] = {  
                'count': len(items),  
                'latest\_items': items\[-5:\]    
            }  
        return {  
            'summary': summary,  
            'recent\_alerts': self.alerts\[-10:\],    
            'metrics': dict(self.metrics)  
        }  
@ray.remote  
def real\_time\_processing\_pipeline(num\_sources=3, num\_processors=2, duration=30):  
    """Complete real-time data processing pipeline"""  
    print(f"Starting real-time processing with {num\_sources} sources and {num\_processors} processors")  
      
    generators = \[DataGenerator.remote(f"source\_{i}") for i in range(num\_sources)\]  
    processors = \[StreamProcessor.remote(f"processor\_{i}") for i in range(num\_processors)\]  
    aggregator = StreamAggregator.remote()  
      
    print("Starting data generation...")  
    generation\_futures = \[gen.start\_generating.remote(duration) for gen in generators\]  
    start\_time = time.time()  
    batch\_size = 10  
    try:  
        while time.time() - start\_time < duration:  
              
            data\_batch = \[\]  
              
              
            for \_ in range(batch\_size):  
                source\_idx = random.randint(0, num\_sources - 1)  
                  
                data = {  
                    'timestamp': datetime.now().isoformat(),  
                    'source\_id': f'source\_{source\_idx}',  
                    'event\_type': random.choice(\['user\_action', 'system\_metric', 'error'\]),  
                    'value': random.uniform(0, 100),  
                    'metadata': {  
                        'user\_id': random.randint(1, 1000),  
                        'session\_id': f"session\_{random.randint(1, 100)}"  
                    }  
                }  
                data\_batch.append(data)  
            if data\_batch:  
                  
                processor\_idx = random.randint(0, num\_processors - 1)  
                processor = processors\[processor\_idx\]  
                  
                processed\_future = processor.process\_batch.remote(data\_batch)  
                processed\_batch = ray.get(processed\_future)  
                  
                agg\_future = aggregator.aggregate\_batch.remote(processed\_batch)  
                agg\_result = ray.get(agg\_future)  
                print(f"Processed batch: {agg\_result\['aggregated\_count'\]} types, "  
                      f"{agg\_result\['alert\_count'\]} total alerts")  
            time.sleep(1)    
    except KeyboardInterrupt:  
        print("Processing interrupted")  
      
    final\_summary = ray.get(aggregator.get\_summary.remote())  
      
    processor\_stats = ray.get(\[p.get\_stats.remote() for p in processors\])  
    return {  
        'duration': time.time() - start\_time,  
        'final\_summary': final\_summary,  
        'processor\_stats': processor\_stats  
    }  
  
if \_\_name\_\_ == "\_\_main\_\_":  
    ray.init()  
      
    print("Starting real-time data processing pipeline...")  
    start = time.time()  
    result = ray.get(real\_time\_processing\_pipeline.remote(  
        num\_sources=5,  
        num\_processors=3,  
        duration=20    
    ))  
    end = time.time()  
    print(f"\\nPipeline completed in {end - start:.2f} seconds")  
    print(f"Processing duration: {result\['duration'\]:.2f} seconds")  
    print("\\nFinal Summary:")  
    for data\_type, info in result\['final\_summary'\]\['summary'\].items():  
        print(f"  {data\_type}: {info\['count'\]} items processed")  
    print(f"\\nTotal Alerts: {len(result\['final\_summary'\]\['recent\_alerts'\])}")  
    print(f"Metrics: {result\['final\_summary'\]\['metrics'\]}")  
    print("\\nProcessor Statistics:")  
    for i, stats in enumerate(result\['processor\_stats'\]):  
        print(f"  Processor {i}: {stats\['processed\_count'\]} processed, "  
              f"{stats\['error\_count'\]} errors")

## Troubleshooting

## Common Issues and Solutions

### 1\. Out of Memory Errors

\# Problem: Large objects causing memory issues  
\# Solution: Use object store efficiently  
\# Instead of this (creates copies):  
@ray.remote  
def bad\_function(large\_data):  
    return process(large\_data)  
\# Do this (uses references):  
large\_data\_ref = ray.put(large\_data)  
@ray.remote  
def good\_function(data\_ref):  
    data = ray.get(data\_ref)  
    return process(data)

### 2\. Slow Performance

  
  
  
futures = \[small\_task.remote(x) for x in range(10000)\]  
  
batch\_size = 100  
batches = \[list(range(i, min(i + batch\_size, 10000)))   
           for i in range(0, 10000, batch\_size)\]  
futures = \[batch\_task.remote(batch) for batch in batches\]

### 3\. Debugging Strategies

  
ray.init(local\_mode=True)    
  
import logging  
logging.basicConfig(level=logging.DEBUG)  
@ray.remote  
def debuggable\_task(data):  
    try:  
        print(f"Processing data: {type(data)}, size: {len(data)}")  
        result = complex\_operation(data)  
        print(f"Result: {type(result)}")  
        return result  
    except Exception as e:  
        print(f"Error in task: {e}")  
        import traceback  
        traceback.print\_exc()  
        raise  
  
print(ray.cluster\_resources())  
print(ray.available\_resources())

### 4\. Connection Issues

  
  
  
import subprocess  
result = subprocess.run(\['ray', 'status'\], capture\_output=True, text=True)  
print(result.stdout)  
  
ray.init(address='ray://localhost:10001')  
  

## Performance Profiling

  
@ray.remote  
def profiled\_task():  
    import cProfile  
    import pstats  
    pr = cProfile.Profile()  
    pr.enable()  
      
    result = expensive\_computation()  
    pr.disable()  
    stats = pstats.Stats(pr)  
    stats.sort\_stats('cumulative')  
    stats.print\_stats(10)    
    return result

## Conclusion

Ray transforms distributed computing from a complex, expert-only domain into something accessible to any Python developer. Whether you’re scaling a simple script or building a complex distributed system, Ray provides the tools and abstractions you need.

## Key Takeaways

1.  **Start Simple**: Begin with `@ray.remote` decorators on existing functions
2.  **Think in Patterns**: Use tasks for stateless operations, actors for stateful ones
3.  **Optimize Gradually**: Profile first, then optimize bottlenecks
4.  **Embrace the Ecosystem**: Leverage Ray’s specialized libraries (Train, Tune, Serve, Data)
5.  **Plan for Production**: Consider deployment, monitoring, and fault tolerance early

## Next Steps

1.  **Experiment**: Try the examples in this guide
2.  **Build**: Convert an existing project to use Ray
3.  **Learn More**: Explore Ray’s specialized libraries
4.  **Contribute**: Join the Ray community and contribute back

## Resources

-   **Official Documentation**: [docs.ray.io](https://docs.ray.io/)

-   **GitHub Repository**: [github.com/ray-project/ray](https://github.com/ray-project/ray)
-   **Community Forum**: [discuss.ray.io](https://discuss.ray.io/)

-   **Tutorials**: [tutorial.ray.io](https://tutorial.ray.io/)
-   **Examples**: [github.com/ray-project/ray/tree/master/python/ray/examples](https://github.com/ray-project/ray/tree/master/python/ray/examples)

Ray is more than just a distributed computing framework — it’s an enabler of innovation. By removing the complexity traditionally associated with distributed systems, Ray allows developers to focus on solving real problems rather than wrestling with infrastructure.

Whether you’re processing terabytes of data, training machine learning models, or building real-time applications, Ray scales with your ambitions. The future of computing is distributed, and Ray makes that future accessible today.

*Happy distributed computing with Ray! 🚀*

---
> **Note:** This page contains 1 cross-origin iframe(s) that could not be accessed due to browser security policies. Some content may be missing. Links to these iframes have been preserved where possible.


---
Source: [Ray: The Complete Guide from Beginner to Professional](https://medium.com/@sjbpr1/ray-the-complete-guide-from-beginner-to-professional-74160d98749b)