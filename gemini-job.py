import ray
import os

ray.init()

# Read from a SQL query of the dataset. Do not specify dataset.
ds = ray.data.read_bigquery(
    project_id="kevin-ai-playground",
    query = "SELECT * FROM `csv_parse_ds.csv_files_object_table`",
)

print(ds.schema())
ds.show()

@ray.remote
class Counter:
    def __init__(self):
        # Used to verify runtimeEnv
        self.name = os.getenv("counter_name")
        self.counter = 0

    def inc(self):
        self.counter += 1

    def get_counter(self):
        return "{} got {}".format(self.name, self.counter)

counter = Counter.remote()

for _ in range(5):
        ray.get(counter.inc.remote())
        print(ray.get(counter.get_counter.remote()))