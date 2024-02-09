import datasets
import os
import datetime
from pathlib import Path
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--papers", type=str, required=True)
parser.add_argument("--push", type=str, required=True)
args = parser.parse_args()

ds = datasets.load_dataset("CCRss/arXiv_dataset", split="train")

base_path = Path(args.papers)


ds2 = []
i = 0
for ex in tqdm(ds, total=len(ds)):
    i += 1
    if i > 10_000:
        break
    p_id = ex["id"]
    p_dir = base_path / p_id
    if p_dir.exists():
        # read all files in the directory
        files = {}
        for f in p_dir.iterdir():
            if f.is_file():
                files[f.name] = f.read_text()

        ex2 = {"files": files, **ex}
        ds2.append(ex2)

ds = datasets.Dataset.from_list(ds2)
print(ds)

ds.push_to_hub(args.push)
