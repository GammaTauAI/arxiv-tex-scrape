import datasets
import datetime
import json
from tqdm import tqdm
import os
ds = datasets.load_dataset("CCRss/arXiv_dataset", split="train")

# only keep "id"


# filter category down to cs: "cs." and no papers older than 2021
print("pre-filter")
print(ds)
# would be nice to filter for citations; lots of papers are garbage
# NOTE: dates are in datetime
ds = ds.filter(lambda x: "cs." in x["categories"] and x["update_date"] > datetime.datetime(2021, 1, 1))
print("post-filter")
print(ds)
# care only about "id"
print("pre-map")
ids = []
for x in ds:
    ids.append(x["id"])

print("writing")
with open("arxiv_ids.json", "w") as f:
    json.dump(ids, f)
