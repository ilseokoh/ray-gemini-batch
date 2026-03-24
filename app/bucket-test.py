import ray
import os
from google.cloud import storage


GCP_GCS_BUCKET = ""
GCP_GCS_FILE = "test_file.txt"

ray.init(address="auto")

@ray.remote
def check_gcs_read_write():
    client = storage.Client()
    bucket = client.bucket(GCP_GCS_BUCKET)
    blob = bucket.blob(GCP_GCS_FILE)

    # Write to the bucket
    blob.upload_from_string("Hello, Ray on GKE!")

    # Read from the bucket
    content = blob.download_as_text()

    return content

result = ray.get(check_gcs_read_write.remote())
print(result)