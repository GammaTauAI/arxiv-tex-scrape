import datasets
import json
from tqdm import tqdm
import os
ds = datasets.load_dataset("CCRss/arXiv_dataset", split="train")

# only keep "id"


# filter category down to cs: "cs."
print("pre-filter")
print(ds)
# would be nice to filter for citations; lots of papers are garbage
ds = ds.filter(lambda x: "cs." in x["categories"], num_proc=os.cpu_count())
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
