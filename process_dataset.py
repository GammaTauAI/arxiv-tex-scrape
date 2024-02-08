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


def process(ex):
    p_id = ex["id"]
    p_dir = base_path / p_id
    if p_dir.exists():
        # read all files in the directory
        files = {}
        for f in p_dir.iterdir():
            if f.is_file():
                files[f.name] = f.read_text()

        return {"files": files, **ex}
    else:
        return None


ds = ds.map(process)
print(ds)

ds.push_to_hub(args.push)
