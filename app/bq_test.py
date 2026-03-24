import ray
import os

ray.init()


# Read target file list from a SQL query of the dataset.
ds = ray.data.read_bigquery(
    project_id="pjt-lges-midata",
    query = "SELECT * FROM `csv_parse_ds.csv_analysis_src_files` WHERE size > 10000 LIMIT 5",
)

print(f"--------------- File Count: {ds.count()}")
