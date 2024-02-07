import datasets
import os
ds = datasets.load_dataset("CCRss/arXiv_dataset", split="train")

# only keep "id"


# filter category down to cs: "cs."
print("pre-filter")
print(ds)
ds = ds.filter(lambda x: "cs." in x["categories"], num_proc=os.cpu_count())
print("post-filter")
print(ds)
